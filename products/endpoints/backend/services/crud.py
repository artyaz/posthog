"""Create/update/delete orchestration for endpoints.

``EndpointCrudService`` owns the write-path business rules: version creation on
query change, materialization transfer between versions, tag application, and
activity logging. The viewset only parses/validates the request and serializes
the result.
"""

import dataclasses
from typing import Union, cast

import structlog
from loginas.utils import is_impersonated_session
from rest_framework.exceptions import ValidationError
from rest_framework.request import Request

from posthog.schema import EndpointRequest, HogQLQuery

from posthog.api.tagged_item import cleanup_orphan_tags, set_tags_on_object
from posthog.clickhouse.query_tagging import Product
from posthog.event_usage import report_user_action
from posthog.exceptions_capture import capture_exception
from posthog.models import Team, User
from posthog.models.activity_logging.activity_log import Change, Detail, changes_between, log_activity
from posthog.types import InsightQueryNode

from products.data_modeling.backend.services.saved_query_dag_sync import delete_node_from_dag
from products.data_warehouse.backend.data_load.saved_query_service import trigger_saved_query_schedule
from products.endpoints.backend.constants import DEFAULT_DATA_FRESHNESS_SECONDS
from products.endpoints.backend.models import Endpoint, EndpointVersion
from products.endpoints.backend.rate_limit import clear_endpoint_materialization_cache
from products.endpoints.backend.services.activity import EndpointContext
from products.endpoints.backend.services.materialization import EndpointMaterializationService
from products.endpoints.backend.services.validation import validate_bucket_overrides

logger = structlog.get_logger(__name__)


def apply_tags(endpoint: Endpoint, tags: list[str] | None) -> None:
    """Replace the endpoint's tags. No-op when tags is None (field omitted)."""
    if tags is None:
        return
    # `prefetched_tags` is a dynamic attribute populated by the TaggedItem prefetch / set_tags_on_object;
    # it's not declared on the Endpoint model.
    endpoint.prefetched_tags = set_tags_on_object(tags, endpoint)  # type: ignore[attr-defined]
    cleanup_orphan_tags(endpoint.team_id)


@dataclasses.dataclass(frozen=True)
class EndpointUpdateResult:
    """Outcome of an update, with everything the viewset needs to build the response."""

    endpoint: Endpoint
    target_version: EndpointVersion | None
    version_targeted: bool
    materialization_error: str | None = None


class EndpointCrudService:
    """Write-path orchestration for endpoints."""

    def __init__(self, team: Team, request: Request):
        self.team = team
        self.request = request
        self.user = cast(User, request.user)
        self.materialization = EndpointMaterializationService(team, request)

    def _log_activity(self, *, item_id: str, scope: str, activity: str, detail: Detail) -> None:
        log_activity(
            organization_id=self.team.organization_id,
            team_id=self.team.pk,
            user=self.user,
            was_impersonated=is_impersonated_session(self.request),
            item_id=item_id,
            scope=scope,
            activity=activity,
            detail=detail,
        )

    # ------------------------------------------------------------------
    # Create
    # ------------------------------------------------------------------

    def create(self, data: EndpointRequest) -> Endpoint:
        """Create an endpoint with its initial version. Assumes the payload is validated."""
        if Endpoint.objects.filter(team=self.team, name=data.name, deleted=False).exists():
            raise ValidationError({"name": "An endpoint with this name already exists for this team."})

        try:
            query_dict = cast(Union[HogQLQuery, InsightQueryNode], data.query).model_dump()
            endpoint = Endpoint.objects.create(
                team=self.team,
                created_by=self.user,
                name=cast(str, data.name),  # verified in validate_endpoint_request
                is_active=data.is_active if data.is_active is not None else True,
                current_version=1,
                derived_from_insight=data.derived_from_insight,
            )

            try:
                columns: list[dict] | None = EndpointVersion.extract_columns(query_dict, team_id=self.team.pk)
            except Exception:
                columns = None
            EndpointVersion.objects.create(
                endpoint=endpoint,
                team=self.team,
                version=1,
                query=query_dict,
                description=data.description or "",
                data_freshness_seconds=(
                    data.data_freshness_seconds
                    if data.data_freshness_seconds is not None
                    else DEFAULT_DATA_FRESHNESS_SECONDS
                ),
                created_by=self.user,
                columns=columns,
            )

            apply_tags(endpoint, data.tags)

            self._log_activity(
                item_id=str(endpoint.id),
                scope="Endpoint",
                activity="created",
                detail=Detail(name=endpoint.name),
            )

            report_user_action(
                user=self.user,
                event="endpoint created",
                properties={
                    "endpoint_id": str(endpoint.id),
                    "endpoint_name": endpoint.name,
                    "query_kind": query_dict.get("kind") if isinstance(query_dict, dict) else None,
                },
                team=self.team,
                request=self.request,
            )

            return endpoint

        except Exception as e:
            capture_exception(
                e,
                {
                    "product": Product.ENDPOINTS,
                    "team_id": self.team.pk,
                    "endpoint_name": data.name,
                },
            )
            raise ValidationError("Failed to create endpoint.")

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------

    def update(
        self,
        endpoint: Endpoint,
        data: EndpointRequest,
        raw_data: dict,
        version_number: int | None = None,
    ) -> EndpointUpdateResult:
        """Update an endpoint, creating a new version when the query changes.

        When version_number is provided, updates target that specific version
        (and query changes are rejected). ``raw_data`` is the unparsed request
        payload, needed to distinguish omitted fields from explicit nulls.
        """
        endpoint_before_update = Endpoint.objects.get(pk=endpoint.id)

        target_version_override = None
        if version_number is not None:
            try:
                target_version_override = endpoint.get_version(version_number)
            except EndpointVersion.DoesNotExist:
                raise ValidationError({"version": f"Version {version_number} not found for this endpoint."})
            if data.query is not None:
                raise ValidationError(
                    {
                        "query": "Cannot change query when targeting a specific version. Query changes create a new version."
                    }
                )

        try:
            current_version = endpoint.get_version()
            target_version = target_version_override or current_version

            version_before_update = EndpointVersion.objects.get(pk=target_version.pk) if target_version else None
            version_was_created = False
            query_changed = False
            new_query_dict = None
            if data.query is not None:
                new_query_dict = data.query.model_dump()
                query_changed = endpoint.has_query_changed(new_query_dict)

            # Deactivates the whole endpoint - we deactivate a version later if requested
            if data.is_active is not None and target_version_override is None:
                endpoint.is_active = data.is_active
            endpoint.save()

            final_is_active = data.is_active if data.is_active is not None else endpoint.is_active
            was_materialized = current_version.saved_query_id is not None

            # Step 1: Handle deactivation (disables materialization, prevents any materialization operations)
            if not final_is_active and was_materialized:
                self.materialization.disable_materialization(endpoint, current_version)

            # Step 2: Handle query changes and versioning (independent of active/materialization state)
            old_bucket_overrides: dict[str, str] | None = None
            if query_changed and new_query_dict is not None:
                if was_materialized:
                    old_bucket_overrides = current_version.bucket_overrides

                new_version = endpoint.create_new_version(query=new_query_dict, user=self.user)
                version_was_created = True
                current_version = new_version
                target_version = new_version

            # Step 3: Update version-level fields on target version
            if target_version:
                update_fields = []
                if data.description is not None:
                    target_version.description = data.description
                    update_fields.append("description")
                if "data_freshness_seconds" in raw_data:
                    target_version.data_freshness_seconds = (
                        data.data_freshness_seconds
                        if data.data_freshness_seconds is not None
                        else DEFAULT_DATA_FRESHNESS_SECONDS
                    )
                    update_fields.append("data_freshness_seconds")
                # When targeting a specific version, is_active updates the version
                if data.is_active is not None and target_version_override is not None:
                    target_version.is_active = data.is_active
                    update_fields.append("is_active")
                if update_fields:
                    update_fields.append("updated_at")
                    target_version.save(update_fields=update_fields)

            # Step 4: Handle materialization state (only if endpoint should be active)
            stored_bucket_overrides = target_version.bucket_overrides if target_version else None
            materialization_error: str | None = None
            if final_is_active and target_version:
                # When targeting a specific version, check that version's materialization state
                # Otherwise use was_materialized (state before this update) to support materialization transfer
                if target_version_override is not None:
                    check_was_materialized = target_version.saved_query_id is not None
                else:
                    check_was_materialized = was_materialized

                should_enable = data.is_materialized is True or (
                    data.is_materialized is None and check_was_materialized
                )
                should_disable = data.is_materialized is False

                if should_enable:
                    bucket_overrides = raw_data.get("bucket_overrides")
                    if bucket_overrides is None and version_was_created:
                        bucket_overrides = old_bucket_overrides
                    validate_bucket_overrides(bucket_overrides)
                    try:
                        self.materialization.enable_materialization(
                            endpoint,
                            target_version.data_freshness_seconds,
                            target_version,
                            bucket_overrides=bucket_overrides,
                        )
                        # Trigger immediate refresh when bucket_overrides changed
                        if (
                            bucket_overrides is not None
                            and bucket_overrides != stored_bucket_overrides
                            and target_version.saved_query is not None
                        ):
                            try:
                                trigger_saved_query_schedule(target_version.saved_query)
                            except Exception:
                                logger.warning(
                                    "failed_to_trigger_materialization_refresh",
                                    team_id=self.team.pk,
                                    saved_query_id=str(target_version.saved_query_id),
                                )
                    except Exception as e:
                        if version_was_created:
                            # The new version was already committed — don't fail the whole update.
                            # Materialization can be retried via a subsequent update.
                            materialization_error = str(e)
                            logger.exception(
                                "Materialization failed after version creation",
                                endpoint_name=endpoint.name,
                                version=target_version.version,
                            )
                            capture_exception(
                                e,
                                {
                                    "product": Product.ENDPOINTS,
                                    "team_id": self.team.pk,
                                    "endpoint_name": endpoint.name,
                                    "version": target_version.version,
                                },
                            )
                        else:
                            raise
                elif should_disable:
                    self.materialization.disable_materialization(endpoint, target_version)

            apply_tags(endpoint, data.tags)

            endpoint_changes = changes_between("Endpoint", previous=endpoint_before_update, current=endpoint)
            if endpoint_changes:
                # endpoint-level activity
                self._log_activity(
                    item_id=str(endpoint.id),
                    scope="Endpoint",
                    activity="updated",
                    detail=Detail(name=endpoint.name, changes=endpoint_changes),
                )

            # version-level activity
            if version_was_created:
                query_change = Change(
                    type="EndpointVersion",
                    action="changed",
                    field="query",
                    before=version_before_update.query if version_before_update else None,
                    after=target_version.query,
                )
                self._log_activity(
                    item_id=str(endpoint.id),
                    scope="Endpoint",
                    activity="version_created",
                    detail=Detail(
                        name=endpoint.name,
                        changes=[query_change],
                        context=EndpointContext(version=target_version.version),
                    ),
                )
            elif target_version and version_before_update:
                version_changes = changes_between(
                    "EndpointVersion", previous=version_before_update, current=target_version
                )

                if version_changes:
                    self._log_activity(
                        item_id=str(endpoint.id),
                        scope="EndpointVersion",
                        activity="version_updated",
                        detail=Detail(
                            name=endpoint.name,
                            changes=version_changes,
                            context=EndpointContext(version=target_version.version),
                        ),
                    )

            return EndpointUpdateResult(
                endpoint=endpoint,
                target_version=target_version,
                version_targeted=target_version_override is not None,
                materialization_error=materialization_error,
            )

        except ValidationError:
            raise
        except Exception as e:
            current_version = endpoint.get_version()
            capture_exception(
                e,
                {
                    "product": Product.ENDPOINTS,
                    "team_id": self.team.pk,
                    "endpoint_id": endpoint.id,
                    "saved_query_id": current_version.saved_query.id if current_version.saved_query else None,
                },
            )
            raise ValidationError("Failed to update endpoint.")

    # ------------------------------------------------------------------
    # Destroy
    # ------------------------------------------------------------------

    def destroy(self, endpoint: Endpoint) -> None:
        """Soft-delete an endpoint and clean up materialized queries."""
        endpoint_id = str(endpoint.id)
        endpoint_name = endpoint.name

        for version in endpoint.versions.filter(saved_query__isnull=False):
            try:
                if version.saved_query:
                    delete_node_from_dag(version.saved_query)
            except Exception as e:
                logger.exception(
                    "Failed to remove endpoint node from DAG on destroy",
                    endpoint_name=endpoint.name,
                    saved_query_id=version.saved_query.id if version.saved_query else None,
                )
                capture_exception(
                    e,
                    {
                        "product": Product.ENDPOINTS,
                        "team_id": self.team.pk,
                        "endpoint_name": endpoint.name,
                        "saved_query_id": version.saved_query.id if version and version.saved_query else None,
                    },
                )

        endpoint.soft_delete()
        clear_endpoint_materialization_cache(self.team.pk, endpoint.name)
        self._log_activity(
            item_id=endpoint_id,
            scope="Endpoint",
            activity="deleted",
            detail=Detail(name=endpoint_name),
        )
