"""Endpoint run-path orchestration.

``EndpointExecutionService`` owns everything that happens after the viewset has
authenticated the caller and parsed the request: run-request validation, choosing
between materialized / inline / DuckLake execution, query-service invocation,
response post-processing, metrics, and failure signals. Kind-specific behavior
(variables, WHERE building, response re-shaping) is delegated to the
``EndpointQueryStrategy`` classes.
"""

import re
import time
import uuid
from datetime import timedelta
from typing import Union, cast

from django.utils import timezone

import structlog
import posthoganalytics
from asgiref.sync import async_to_sync
from dateutil.parser import isoparse
from pydantic import BaseModel
from rest_framework import status
from rest_framework.exceptions import Throttled, ValidationError
from rest_framework.request import Request
from rest_framework.response import Response

from posthog.schema import (
    EndpointRefreshMode,
    EndpointRunRequest,
    HogQLQuery,
    HogQLQueryModifiers,
    HogQLVariable,
    QueryRequest,
    RefreshType,
)

from posthog.hogql.errors import ExposedHogQLError, ResolutionError

from posthog.api.mixins import PydanticModelMixin
from posthog.api.query import _process_query_request
from posthog.api.services.query import process_query_model
from posthog.clickhouse.client.connection import Workload
from posthog.clickhouse.client.limit import ConcurrencyLimitExceeded
from posthog.clickhouse.query_tagging import Feature, Product, get_query_tag_value, tag_queries
from posthog.ducklake.common import get_duckgres_server_for_organization
from posthog.errors import ExposedCHQueryError
from posthog.event_usage import get_request_analytics_properties, report_user_action
from posthog.exceptions_capture import capture_exception
from posthog.hogql_queries.insights.utils.breakdowns import BREAKDOWN_NULL_STRING_LABEL, BREAKDOWN_OTHER_STRING_LABEL
from posthog.hogql_queries.query_runner import BLOCKING_EXECUTION_MODES
from posthog.models import Team, User

from products.data_warehouse.backend.data_load.saved_query_service import trigger_saved_query_schedule
from products.endpoints.backend.insight_transformers import MaterializedSeriesMismatchError
from products.endpoints.backend.materialization import ENDPOINT_BREAKDOWN_LIMIT
from products.endpoints.backend.metrics import (
    ENDPOINT_CACHE_RESULT_TOTAL,
    ENDPOINT_CONCURRENCY_REJECTED_TOTAL,
    ENDPOINT_DUCKLAKE_FALLBACK_TOTAL,
    ENDPOINT_EXECUTION_DURATION_SECONDS,
    ENDPOINT_EXECUTION_TOTAL,
    ENDPOINT_HOGQL_RESULT_ROWS,
    ENDPOINT_MATERIALIZED_AGE_SECONDS,
    ENDPOINT_VALIDATION_ERROR_TOTAL,
    query_kind_label,
)
from products.endpoints.backend.models import Endpoint, EndpointVersion
from products.endpoints.backend.services.pagination import EndpointPagination
from products.endpoints.backend.services.strategies import EndpointQueryStrategy, strategy_for

from common.hogvm.python.utils import HogVMException

logger = structlog.get_logger(__name__)

LAST_EXECUTED_THROTTLE = timedelta(minutes=30)


def _emit_endpoint_failure_signal(
    team,
    endpoint: Endpoint,
    exc: BaseException,
    *,
    materialized: bool,
    version: int | None = None,
    saved_query_id: Union[str, uuid.UUID, None] = None,
    query_kind: str | None = None,
    executed_sql: str | None = None,
    saved_query_status: str | None = None,
    saved_query_last_run_at: str | None = None,
    saved_query_columns: dict | None = None,
    endpoint_columns: list | None = None,
) -> None:
    """Fire a Signal when an endpoint execution fails, so the AI can reason about it later.

    Fails silently — signal emission must never mask the underlying error.
    """
    from products.signals.backend.facade.api import emit_signal

    try:
        error_class = type(exc).__name__
        error_msg = str(exc)
        version_str = f" v{version}" if version else ""

        if materialized:
            execution_mode = "materialized"
            context = (
                f"The materialized table (saved_query_id={saved_query_id}) may be stale, missing, or have a schema mismatch. "
                f"Check whether the materialization refresh completed successfully and whether the underlying query still produces valid columns."
            )
            if saved_query_status:
                context += f"\nSaved query status: {saved_query_status}"
            if saved_query_last_run_at:
                context += f", last materialized at: {saved_query_last_run_at}"
            if saved_query_columns:
                context += f"\nMaterialized table columns: {saved_query_columns}"
            if endpoint_columns:
                context += f"\nEndpoint version columns: {endpoint_columns}"
        else:
            execution_mode = "inline"
            context = (
                f"The query is executed on-demand against live data. "
                f"Common causes: invalid HogQL syntax, missing or renamed properties, query timeout, or incompatible variable overrides."
            )

        parts = [
            f"Endpoint '{endpoint.name}'{version_str} failed during {execution_mode} execution.",
            f"Error: {error_class}: {error_msg}",
        ]
        if query_kind:
            parts.append(f"Query kind: {query_kind}")
        if executed_sql:
            parts.append(f"Executed HogQL: {executed_sql}")
        parts.append(context)
        parts.append(f"Endpoint path: {endpoint.endpoint_path}")
        description = "\n".join(parts)

        async_to_sync(emit_signal)(
            team=team,
            source_product="endpoints",
            source_type="endpoint_execution_failed",
            source_id=f"{team.id}:{endpoint.name}",
            description=description,
            weight=0.5,
            extra={
                "endpoint_name": endpoint.name,
                "endpoint_version": version,
                "materialized": materialized,
                "saved_query_id": str(saved_query_id) if saved_query_id else None,
                "error_class": error_class,
                "error_message": error_msg,
            },
        )
    except Exception:
        logger.exception("Failed to emit endpoint failure signal", endpoint_name=endpoint.name)


def _endpoint_refresh_mode_to_refresh_type(
    mode: EndpointRefreshMode | None,
) -> RefreshType:
    """
    Map EndpointRefreshMode to RefreshType.

    - cache -> blocking
    - force/direct -> force_blocking (materialization bypass handled in should_use_materialized_table)
    """
    if mode is None or mode == EndpointRefreshMode.CACHE:
        return RefreshType.BLOCKING
    return RefreshType.FORCE_BLOCKING


def _clean_breakdown_sentinels(result: dict) -> None:
    """Replace breakdown sentinel strings in API response results in-place.

    Handles both list-of-lists (materialized/HogQL) and list-of-dicts (inline insight).
    If the "Other" sentinel is found, it means the breakdown_limit was exceeded —
    we clean it to "Other" but also capture_exception for visibility.
    """
    rows = result.get("results")
    if not rows:
        return

    found_other = False
    columns = result.get("columns")
    if columns:
        # HogQL/materialized path: results are list[tuple] or list[list]
        indices = [i for i, col in enumerate(columns) if col == "breakdown_value"]
        if not indices:
            return
        for row_idx, row in enumerate(rows):
            needs_clean = any(isinstance(row[i], (str, list)) or row[i] is None for i in indices)
            if not needs_clean:
                continue
            if not found_other:
                found_other = any(_value_contains_other(row[i], BREAKDOWN_OTHER_STRING_LABEL) for i in indices)
            row_list = list(row)
            for i in indices:
                row_list[i] = _clean_sentinel_value(row_list[i])
            rows[row_idx] = type(row)(row_list)
    elif isinstance(rows[0], dict):
        # Inline insight path: results are list[dict]
        for row in rows:
            if "breakdown_value" in row:
                if not found_other:
                    found_other = _value_contains_other(row["breakdown_value"], BREAKDOWN_OTHER_STRING_LABEL)
                row["breakdown_value"] = _clean_sentinel_value(row["breakdown_value"])
            if "label" in row:
                if not found_other and isinstance(row["label"], str):
                    found_other = BREAKDOWN_OTHER_STRING_LABEL in row["label"]
                row["label"] = _clean_sentinel_label(row["label"])

    if found_other:
        capture_exception(
            Exception(
                f"Endpoint breakdown limit ({ENDPOINT_BREAKDOWN_LIMIT}) exceeded — 'Other' bucket appeared in results"
            )
        )


def _value_contains_other(value: object, other_label: str) -> bool:
    """Check if a breakdown value contains the 'Other' sentinel."""
    if isinstance(value, str):
        return value == other_label
    if isinstance(value, list):
        return any(item == other_label for item in value if isinstance(item, str))
    return False


def _clean_sentinel_value(value: object) -> object:
    """Clean breakdown sentinel strings (null and other) in a value or list of values."""
    if isinstance(value, str):
        if value == BREAKDOWN_NULL_STRING_LABEL or value == "":
            return None
        if value == BREAKDOWN_OTHER_STRING_LABEL:
            return "Other"
    elif isinstance(value, list):
        return [_clean_sentinel_value(item) for item in value]
    return value


def _clean_sentinel_label(label: object) -> object:
    """Clean a label string containing ::-joined breakdown parts."""
    if not isinstance(label, str):
        return label
    if BREAKDOWN_NULL_STRING_LABEL not in label and BREAKDOWN_OTHER_STRING_LABEL not in label:
        return label
    parts = label.split("::")
    cleaned = [
        "null" if p == BREAKDOWN_NULL_STRING_LABEL else "Other" if p == BREAKDOWN_OTHER_STRING_LABEL else p
        for p in parts
    ]
    return "::".join(cleaned)


class EndpointExecutionService(PydanticModelMixin):
    """Executes an endpoint version, choosing the best execution path."""

    def __init__(self, team: Team, request: Request):
        self.team = team
        self.request = request
        self.user = cast(User, request.user)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_run_request(
        self, data: EndpointRunRequest, endpoint: Endpoint, version: EndpointVersion | None = None
    ) -> None:
        version = version or endpoint.get_version()
        strategy = strategy_for(endpoint, version, self.team)

        is_materialized = bool(version.is_materialized and version.saved_query)

        if version and not version.is_active:
            ENDPOINT_VALIDATION_ERROR_TOTAL.labels(reason="inactive_version").inc()
            raise ValidationError(f"Version {version.version} is inactive and cannot be executed.")

        # Reject query_override (always)
        if hasattr(data, "query_override") and data.query_override is not None:
            raise ValidationError({"query_override": "Not allowed. Use variables instead."})

        # Allow filters_override for insight endpoints (deprecated but backwards compatible)
        # Reject for HogQL endpoints
        if data.filters_override is not None:
            if strategy.query_kind == "HogQLQuery":
                raise ValidationError({"filters_override": "Not allowed for HogQL endpoints. Use variables instead."})

        # Validate refresh mode
        if data.refresh == EndpointRefreshMode.DIRECT and not is_materialized:
            ENDPOINT_VALIDATION_ERROR_TOTAL.labels(reason="direct_refresh_not_materialized").inc()
            raise ValidationError(
                {
                    "refresh": "'direct' refresh mode is only valid for materialized endpoints. "
                    "Use 'cache' or 'force' instead, or enable materialization on this endpoint."
                }
            )

        # Validate variables
        if data.variables:
            allowed_vars = strategy.allowed_variables(is_materialized)
            unknown_vars = set(data.variables.keys()) - allowed_vars
            if unknown_vars:
                ENDPOINT_VALIDATION_ERROR_TOTAL.labels(reason="unknown_variable").inc()
                raise ValidationError({"variables": f"Unknown variable(s): {', '.join(sorted(unknown_vars))}"})

        # SECURITY: For materialized endpoints with required variables, ALL must be provided.
        # Without this check, omitting variables would return ALL data instead of filtered data.
        # Exception: filters_override is the deprecated way to provide filters for insight endpoints
        if is_materialized and not (data.filters_override and data.filters_override.properties):
            required_vars = strategy.required_materialized_variables()
            if required_vars:
                provided = set(data.variables.keys()) if data.variables else set()
                missing = sorted(required_vars - provided)
                if missing:
                    ENDPOINT_VALIDATION_ERROR_TOTAL.labels(reason="missing_required_variable").inc()
                    raise ValidationError(
                        {"variables": f"Required variable(s) {', '.join(repr(v) for v in missing)} not provided"}
                    )

    # ------------------------------------------------------------------
    # Execution path selection
    # ------------------------------------------------------------------

    def should_use_materialized_table(
        self, endpoint: Endpoint, data: EndpointRunRequest, version: EndpointVersion | None = None
    ) -> bool:
        """
        Decide whether to use materialized table or inline execution.

        Returns False if:
        - Not materialized
        - Materialization incomplete/failed
        - Materialized data is stale (older than sync frequency)
        - User overrides present (variables, query)
        - 'direct' mode requested (explicitly bypass materialization)
        """
        version = version or endpoint.get_version()
        if not version.is_materialized or not version.saved_query:
            return False

        saved_query = version.saved_query
        if saved_query.status not in ["Completed"]:
            return False

        if not saved_query.table:
            return False

        # Check if materialized data is stale
        if saved_query.last_run_at and saved_query.sync_frequency_interval:
            next_refresh_due = saved_query.last_run_at + saved_query.sync_frequency_interval
            if timezone.now() >= next_refresh_due:
                return False

        # 'direct' mode explicitly bypasses materialization to run the original query
        if data.refresh == EndpointRefreshMode.DIRECT:
            return False

        # Check if variables are valid for materialized execution
        if data.variables:
            strategy = strategy_for(endpoint, version, self.team)
            if not strategy.can_serve_variables_from_materialized(set(data.variables.keys())):
                return False

        return True

    def _should_use_ducklake(self, endpoint: Endpoint, version: EndpointVersion | None) -> bool:
        if version is None:
            return False
        if version.query.get("kind") != "HogQLQuery":
            return False

        user_email = getattr(self.request.user, "email", "") if self.request else ""
        ff_result = posthoganalytics.feature_enabled(
            "endpoints-ducklake-execution",
            user_email,
            person_properties={"email": str(user_email)},
            only_evaluate_locally=True,
            send_feature_flag_events=False,
        )
        logger.info(
            "Ducklake FF evaluation",
            endpoint_name=endpoint.name,
            ff_result=ff_result,
        )
        if not ff_result:
            return False

        server = get_duckgres_server_for_organization(str(self.team.organization_id))
        if server is None:
            logger.info("Ducklake skip: no duckgres server", endpoint_name=endpoint.name, team_id=self.team.pk)
        return server is not None

    # ------------------------------------------------------------------
    # Top-level entry point
    # ------------------------------------------------------------------

    def execute(
        self,
        endpoint: Endpoint,
        data: EndpointRunRequest,
        version_obj: EndpointVersion | None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Response:
        """Run an endpoint version and return the HTTP response.

        Assumes the caller has resolved the version (or established that none was
        requested explicitly) and parsed limit/offset.
        """
        # Track endpoint execution for deprecation monitoring
        report_user_action(
            user=self.user,
            event="endpoint executed",
            properties={
                "endpoint_id": str(endpoint.id),
                "endpoint_name": endpoint.name,
                "has_filters_override": bool(data.filters_override),
                "has_variables": bool(data.variables),
                "has_limit": data.limit is not None,
                "has_offset": data.offset is not None,
                "refresh_mode": data.refresh.value if data.refresh else None,
            },
            team=self.team,
            request=self.request,
        )

        self.validate_run_request(data, endpoint, version_obj)

        if offset is not None and version_obj:
            query_kind = version_obj.query.get("kind")
            if query_kind != "HogQLQuery":
                return Response(
                    {"error": "offset is only supported for HogQL endpoints"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        # Check if we should use materialization for this version
        use_materialized = self.should_use_materialized_table(endpoint, data, version_obj)

        debug = data.debug or False
        execution_type = "materialized" if use_materialized else "inline"
        query_kind_metric = query_kind_label(version_obj.query if version_obj else None)
        execution_status: str | None = None
        _start_time = time.monotonic()

        try:
            if use_materialized:
                result = self._execute_materialized_endpoint(
                    endpoint, data, version=version_obj, debug=debug, limit=limit, offset=offset
                )
            else:
                # Use version's query
                if not version_obj:
                    return Response(
                        {"error": "No version found for this endpoint"},
                        status=status.HTTP_404_NOT_FOUND,
                    )
                query_to_use = version_obj.query.copy()

                use_ducklake = self._should_use_ducklake(endpoint, version_obj)
                if use_ducklake:
                    try:
                        result = self._execute_ducklake_endpoint(endpoint, query_to_use, debug=debug)
                        execution_type = "ducklake"
                    except Exception:
                        logger.warning(
                            "DuckLake execution failed, falling back to inline",
                            endpoint_name=endpoint.name,
                        )
                        ENDPOINT_DUCKLAKE_FALLBACK_TOTAL.inc()
                        execution_type = "ducklake_fallback"
                        result = self._execute_inline_endpoint(
                            endpoint,
                            data,
                            query_to_use,
                            version=version_obj,
                            debug=debug,
                            limit=limit,
                            offset=offset,
                        )
                else:
                    result = self._execute_inline_endpoint(
                        endpoint,
                        data,
                        query_to_use,
                        version=version_obj,
                        debug=debug,
                        limit=limit,
                        offset=offset,
                    )
            execution_status = "success"
        except (ExposedHogQLError, ExposedCHQueryError) as e:
            execution_status = "error"
            logger.exception(
                "Endpoint execution failed",
                endpoint_name=endpoint.name,
                code_name=getattr(e, "code_name", None),
            )
            raise ValidationError("Query execution failed.", getattr(e, "code_name", None))
        except HogVMException:
            execution_status = "error"
            logger.exception(
                "Endpoint execution failed (HogVM)",
                endpoint_name=endpoint.name,
            )
            raise ValidationError("Query execution failed: HogQL virtual machine error")
        except ResolutionError:
            execution_status = "error"
            logger.exception(
                "Endpoint resolution failed",
                endpoint_name=endpoint.name,
            )
            raise ValidationError("Query resolution failed: unable to resolve table or field references.")
        except ConcurrencyLimitExceeded:
            ENDPOINT_CONCURRENCY_REJECTED_TOTAL.inc()
            raise Throttled(detail="Too many concurrent requests. Please try again later.")
        except Exception:
            execution_status = "error"
            raise
        finally:
            if execution_status is not None:
                _duration = time.monotonic() - _start_time
                ENDPOINT_EXECUTION_DURATION_SECONDS.labels(
                    execution_type=execution_type, query_kind=query_kind_metric
                ).observe(_duration)
                ENDPOINT_EXECUTION_TOTAL.labels(
                    execution_type=execution_type, query_kind=query_kind_metric, status=execution_status
                ).inc()

        self._record_result_metrics(result, execution_type, query_kind_metric)
        self._track_last_executed(endpoint, version_obj)

        if isinstance(result.data, dict):
            result.data["name"] = endpoint.name
            if version_obj:
                result.data["endpoint_version"] = version_obj.version
                result.data["endpoint_version_created_at"] = version_obj.created_at.isoformat()

        return result

    def _record_result_metrics(self, result: Response, execution_type: str, query_kind_metric: str) -> None:
        try:
            if isinstance(result.data, dict):
                # DuckLake bypasses the query result cache entirely — don't claim hit/miss for it.
                if execution_type != "ducklake":
                    cache_outcome = "hit" if bool(result.data.get("is_cached")) else "miss"
                    ENDPOINT_CACHE_RESULT_TOTAL.labels(
                        execution_type=execution_type, query_kind=query_kind_metric, outcome=cache_outcome
                    ).inc()

                if query_kind_metric == "hogql":
                    results_value = result.data.get("results")
                    if isinstance(results_value, list):
                        ENDPOINT_HOGQL_RESULT_ROWS.labels(execution_type=execution_type).observe(len(results_value))
        except Exception:
            logger.debug("Failed to record endpoint result metrics", exc_info=True)

    def _track_last_executed(self, endpoint: Endpoint, version_obj: EndpointVersion | None) -> None:
        """Record last execution time (30-minute granularity, personal API key calls only)."""
        if get_query_tag_value("access_method") != "personal_api_key":
            return
        now = timezone.now()
        if endpoint.last_executed_at is None or (now - endpoint.last_executed_at > LAST_EXECUTED_THROTTLE):
            endpoint.last_executed_at = now
            endpoint.save(update_fields=["last_executed_at"])
        if version_obj is not None and (
            version_obj.last_executed_at is None or (now - version_obj.last_executed_at > LAST_EXECUTED_THROTTLE)
        ):
            version_obj.last_executed_at = now
            version_obj.save(update_fields=["last_executed_at"])

    # ------------------------------------------------------------------
    # Materialized execution
    # ------------------------------------------------------------------

    def _execute_materialized_endpoint(
        self,
        endpoint: Endpoint,
        data: EndpointRunRequest,
        version: EndpointVersion | None = None,
        debug: bool = False,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Response:
        """Execute against a materialized table in S3."""
        materialized_hogql_query = None
        query_kind = None
        try:
            version = version or endpoint.get_version()
            if not version.saved_query:
                raise ValidationError("No materialized query found for this endpoint")
            saved_query = version.saved_query

            strategy = strategy_for(endpoint, version, self.team)
            query_kind = strategy.query_kind

            select_query, original_limit = strategy.build_materialized_select(table_name=saved_query.name)

            pagination: EndpointPagination | None = None

            # Only paginate flat-row HogQL results. Insight types get transformed
            # into nested structures where flat-row LIMIT/OFFSET is meaningless.
            if limit is not None and strategy.supports_pagination:
                pagination = EndpointPagination(limit=limit, offset=offset or 0, ceiling=original_limit)
                pagination.apply_to(select_query)

            deprecation_headers = strategy.apply_materialized_filters(select_query, data)

            materialized_hogql_query = HogQLQuery(
                query=select_query.to_hogql(),
                modifiers=HogQLQueryModifiers(useMaterializedViews=True),
            )

            refresh_type = _endpoint_refresh_mode_to_refresh_type(data.refresh)

            query_request_data = {
                "client_query_id": data.client_query_id,
                "name": f"{endpoint.name}_materialized",
                "refresh": refresh_type,
                "query": materialized_hogql_query.model_dump(),
            }

            extra_fields = {
                "endpoint_materialized": True,
                "endpoint_materialized_at": saved_query.last_run_at.isoformat() if saved_query.last_run_at else None,
            }
            tag_queries(
                workload=Workload.ENDPOINTS,
                warehouse_query=True,
                endpoint_version=version.version if version else None,
            )

            # Compute dynamic cache TTL: time remaining until data_freshness window expires
            cache_ttl = None
            if saved_query.last_run_at and version.data_freshness_seconds:
                remaining = (
                    saved_query.last_run_at + timedelta(seconds=version.data_freshness_seconds) - timezone.now()
                ).total_seconds()
                if remaining <= 0:
                    logger.warning(
                        "endpoint_materialization_behind_sla",
                        endpoint_name=endpoint.name,
                        team_id=self.team.pk,
                        data_freshness_seconds=version.data_freshness_seconds,
                        last_run_at=saved_query.last_run_at.isoformat(),
                        remaining_seconds=remaining,
                    )
                    tag_queries(endpoint_materialization_behind=True)
                cache_ttl = max(1, int(remaining))  # at least 1 second to enable caching

            result = self._execute_query_and_respond(
                query_request_data,
                data.client_query_id,
                cache_age_seconds=cache_ttl,
                extra_result_fields=extra_fields,
                debug=debug,
                headers=deprecation_headers,
                pagination=pagination,
            )

            if self._is_cache_stale(result, saved_query):
                query_request_data["refresh"] = RefreshType.FORCE_BLOCKING
                result = self._execute_query_and_respond(
                    query_request_data,
                    data.client_query_id,
                    extra_result_fields=extra_fields,
                    debug=debug,
                    headers=deprecation_headers,
                    pagination=pagination,
                )

            try:
                strategy.transform_materialized_response(result.data, saved_query)
            except MaterializedSeriesMismatchError:
                # Series drift: query was likely edited after materialization. Trigger a refresh
                # so the next request succeeds, but fail this one loudly rather than
                # returning wrong-labeled data.
                logger.warning(
                    "Materialized endpoint series mismatch, triggering re-materialization",
                    endpoint_name=endpoint.name,
                    saved_query_id=saved_query.id,
                )
                trigger_saved_query_schedule(saved_query)
                raise

            if saved_query.last_run_at:
                age_seconds = max((timezone.now() - saved_query.last_run_at).total_seconds(), 0.0)
                ENDPOINT_MATERIALIZED_AGE_SECONDS.observe(age_seconds)

            return result
        except Exception as e:
            logger.exception(
                "Materialized endpoint execution failed",
                endpoint_name=endpoint.name,
                saved_query_id=saved_query.id if saved_query else None,
                saved_query_status=saved_query.status if saved_query else None,
            )
            capture_exception(
                e,
                {
                    "product": Product.ENDPOINTS,
                    "team_id": self.team.pk,
                    "endpoint_name": endpoint.name,
                    "materialized": True,
                    "saved_query_id": saved_query.id if saved_query else None,
                },
            )
            _emit_endpoint_failure_signal(
                self.team,
                endpoint,
                e,
                materialized=True,
                version=version.version if version else None,
                saved_query_id=saved_query.id if saved_query else None,
                query_kind=query_kind,
                executed_sql=materialized_hogql_query.query if materialized_hogql_query else None,
                saved_query_status=saved_query.status if saved_query else None,
                saved_query_last_run_at=(
                    saved_query.last_run_at.isoformat() if saved_query and saved_query.last_run_at else None
                ),
                saved_query_columns=saved_query.columns if saved_query else None,
                endpoint_columns=version.columns if version else None,
            )
            raise

    def _is_cache_stale(self, result: Response, saved_query) -> bool:
        """Check if cached result is older than the materialization."""
        if not isinstance(result.data, dict) or not result.data.get("is_cached"):
            return False

        last_refresh = result.data.get("last_refresh")
        if not last_refresh or not saved_query.last_run_at:
            return False

        if isinstance(last_refresh, str):
            last_refresh = isoparse(last_refresh)

        return last_refresh < saved_query.last_run_at

    # ------------------------------------------------------------------
    # Inline + DuckLake execution
    # ------------------------------------------------------------------

    def _execute_inline_endpoint(
        self,
        endpoint: Endpoint,
        data: EndpointRunRequest,
        query: dict,
        version: EndpointVersion | None = None,
        debug: bool = False,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Response:
        """Execute query directly against ClickHouse."""
        strategy: EndpointQueryStrategy | None = None
        try:
            if version is None:
                version = endpoint.get_version()
            strategy = strategy_for(endpoint, version, self.team)

            query = strategy.prepare_inline_query(query)

            pagination: EndpointPagination | None = None
            if limit is not None:
                query, pagination = strategy.apply_pagination(query, limit, offset or 0)

            refresh_type = _endpoint_refresh_mode_to_refresh_type(data.refresh)

            plan = strategy.build_inline_plan(query, data)

            query_request_data = {
                "client_query_id": data.client_query_id,
                "filters_override": plan.filters_override.model_dump() if plan.filters_override else None,
                "name": endpoint.name,
                "refresh": refresh_type,
                "query": query,
            }

            cache_age = version.data_freshness_seconds if version else None
            tag_queries(endpoint_version=version.version if version else None)

            return self._execute_query_and_respond(
                query_request_data,
                data.client_query_id,
                variables_override=plan.variables_override,
                cache_age_seconds=cache_age,
                debug=debug,
                headers=plan.deprecation_headers,
                pagination=pagination,
            )

        except Exception as e:
            self.handle_column_ch_error(e)
            logger.exception(
                "Inline endpoint execution failed",
                endpoint_name=endpoint.name,
            )
            capture_exception(
                e,
                {
                    "product": Product.ENDPOINTS,
                    "team_id": self.team.pk,
                    "materialized": False,
                    "endpoint_name": endpoint.name,
                },
            )
            query_kind = strategy.query_kind if strategy else query.get("kind")
            _emit_endpoint_failure_signal(
                self.team,
                endpoint,
                e,
                materialized=False,
                version=version.version if version else None,
                query_kind=query_kind,
                executed_sql=query.get("query") if query_kind == "HogQLQuery" else None,
                endpoint_columns=version.columns if version else None,
            )
            raise

    def _execute_ducklake_endpoint(
        self,
        endpoint: Endpoint,
        query: dict,
        debug: bool = False,
    ) -> Response:
        from posthog.ducklake.client import execute_ducklake_query

        try:
            result = execute_ducklake_query(
                self.team.pk,
                query=HogQLQuery(query=query["query"]),
                organization_id=str(self.team.organization_id),
            )
            response_data: dict = {
                "results": result.results,
                "columns": result.columns,
                "types": result.types,
                "hasMore": False,
                "backend": "ducklake",
            }
            if debug:
                response_data["query"] = query.get("query")
                response_data["hogql"] = result.hogql
                response_data["ducklake_sql"] = result.sql
            return Response(response_data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.exception(
                "DuckLake endpoint execution failed",
                endpoint_name=endpoint.name,
            )
            capture_exception(
                e,
                {
                    "product": Product.ENDPOINTS,
                    "team_id": self.team.pk,
                    "ducklake": True,
                    "endpoint_name": endpoint.name,
                },
            )
            raise

    # ------------------------------------------------------------------
    # Query service plumbing
    # ------------------------------------------------------------------

    def _execute_query_and_respond(
        self,
        query_request_data: dict,
        client_query_id: str | None,
        variables_override: list[HogQLVariable] | None = None,
        cache_age_seconds: int | None = None,
        extra_result_fields: dict | None = None,
        debug: bool = False,
        headers: dict[str, str] | None = None,
        pagination: EndpointPagination | None = None,
    ) -> Response:
        """Shared query execution logic."""
        merged_data = self.get_model(query_request_data, QueryRequest)

        logger.debug(merged_data)
        query, client_query_id, execution_mode = _process_query_request(
            merged_data, self.team, client_query_id, self.request.user
        )
        self._tag_client_query_id(client_query_id)
        endpoint_feature = (
            Feature.ENDPOINT_EXECUTION if get_query_tag_value("access_method") else Feature.ENDPOINT_PLAYGROUND
        )
        tag_queries(product=Product.ENDPOINTS, feature=endpoint_feature)

        if execution_mode not in BLOCKING_EXECUTION_MODES:
            raise ValidationError({"refresh": f"Only sync modes are supported, got: {execution_mode}"})

        result = process_query_model(
            self.team,
            query,
            variables_override=variables_override,
            execution_mode=execution_mode,
            query_id=client_query_id,
            user=self.user,
            is_query_service=(get_query_tag_value("access_method") == "personal_api_key"),
            cache_age_seconds=cache_age_seconds,
            analytics_props=get_request_analytics_properties(self.request),
        )

        if isinstance(result, BaseModel):
            result = result.model_dump(by_alias=True)

        if isinstance(result, dict) and extra_result_fields:
            result.update(extra_result_fields)

        if not debug:
            debug_fields_to_remove = [
                "calculation_trigger",
                "cache_key",
                "explain",
                "modifiers",
                "resolved_date_range",
                "timings",
                "hogql",
            ]

            for field in debug_fields_to_remove:
                result.pop(field, None)

        if pagination and "results" in result:
            pagination.process_results(result)
        elif "results" in result:
            result["hasMore"] = False

        _clean_breakdown_sentinels(result)

        if "results" in result:
            results_value = result.pop("results")
            result = {"results": results_value, **result}

        response_status = (
            status.HTTP_202_ACCEPTED
            if result.get("query_status") and result["query_status"].get("complete") is False
            else status.HTTP_200_OK
        )
        return Response(result, status=response_status, headers=headers)

    def handle_column_ch_error(self, error) -> None:
        if getattr(error, "message", None):
            match = re.search(r"There's no column.*in table", error.message)
            if match:
                # TODO: remove once we support all column types
                raise ValidationError(match.group(0) + ". Not all column types are fully supported yet.")
        return

    def _tag_client_query_id(self, query_id: str | None) -> None:
        if query_id is None:
            return

        tag_queries(client_query_id=query_id)
