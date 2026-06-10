"""Materialization lifecycle for endpoint versions.

``EndpointMaterializationService`` owns enabling/disabling materialization
(creating and reverting the backing ``DataWarehouseSavedQuery``), the
materialization preview, and the status payload. The AST-level analysis and
query transforms live in ``products.endpoints.backend.materialization``.
"""

from typing import cast

import structlog
from loginas.utils import is_impersonated_session
from rest_framework.exceptions import ValidationError
from rest_framework.request import Request

from posthog.hogql import ast
from posthog.hogql.context import HogQLContext
from posthog.hogql.parser import parse_select
from posthog.hogql.printer import to_printed_hogql
from posthog.hogql.printer.utils import print_prepared_ast

from posthog.clickhouse.query_tagging import Product
from posthog.exceptions_capture import capture_exception
from posthog.models import Team, User
from posthog.models.activity_logging.activity_log import Detail, log_activity

from products.data_modeling.backend.models.datawarehouse_saved_query import DataWarehouseSavedQuery
from products.data_modeling.backend.services.saved_query_dag_sync import delete_node_from_dag, sync_saved_query_to_dag
from products.endpoints.backend.constants import DATA_FRESHNESS_BUCKETS
from products.endpoints.backend.materialization import (
    _extract_aggregate_name,
    analyze_variables_for_materialization,
    build_endpoint_hogql,
    convert_insight_query_to_hogql,
    get_reaggregation,
    transform_query_for_materialization,
)
from products.endpoints.backend.metrics import ENDPOINT_MATERIALIZATION_EVENT_TOTAL
from products.endpoints.backend.models import Endpoint, EndpointVersion
from products.endpoints.backend.rate_limit import clear_endpoint_materialization_cache
from products.endpoints.backend.services.activity import EndpointContext
from products.endpoints.backend.services.strategies import apply_where_filter, strategy_for
from products.warehouse_sources.backend.models.external_data_schema import sync_frequency_to_sync_frequency_interval

logger = structlog.get_logger(__name__)


class OrphanedEndpointSavedQueryError(Exception):
    pass


def prepare_executable_query(saved_query: DataWarehouseSavedQuery) -> None:
    """Rebuild the saved query's executable HogQL from its endpoint version.

    Called by the data-modeling Temporal workflow before each materialization run,
    so query-printer changes and bucket overrides are always reflected.
    """
    version = saved_query.endpoint_versions.first()
    if version is None:
        raise OrphanedEndpointSavedQueryError(
            f"Saved query {saved_query.id} ({saved_query.name}) has no linked EndpointVersion"
        )

    saved_query.query = build_endpoint_hogql(version.query, saved_query.team, bucket_overrides=version.bucket_overrides)
    saved_query.save(update_fields=["query", "updated_at"])


def build_materialization_info(version: EndpointVersion, endpoint_name: str | None = None) -> dict:
    """Build the materialization status dict for a version."""
    if version.saved_query:
        result = {
            "status": version.saved_query.status or "Unknown",
            "can_materialize": True,
            "last_materialized_at": (
                version.saved_query.last_run_at.isoformat() if version.saved_query.last_run_at else None
            ),
            "error": (version.saved_query.latest_error or "")
            if version.saved_query.status != DataWarehouseSavedQuery.Status.COMPLETED
            else "",
            "saved_query_id": str(version.saved_query.id),
        }
    else:
        can_mat, reason = version.can_materialize()
        result = {
            "can_materialize": can_mat,
            "reason": reason if not can_mat else None,
        }

    if endpoint_name is not None:
        result["name"] = endpoint_name
    return result


def cant_materialize_preview(reason: str) -> dict:
    return {
        "can_materialize": False,
        "reason": reason,
        "transformed_query": None,
        "execution_query": None,
        "range_pairs": [],
        "aggregates": [],
    }


class EndpointMaterializationService:
    """Enable, disable, and preview materialization for endpoint versions."""

    def __init__(self, team: Team, request: Request):
        self.team = team
        self.request = request
        self.user = cast(User, request.user)

    def enable_materialization(
        self,
        endpoint: Endpoint,
        data_freshness_seconds: int,
        version: EndpointVersion | None = None,
        bucket_overrides: dict[str, str] | None = None,
    ) -> None:
        """Enable materialization for an endpoint version.

        If version is not specified, uses the current version.
        Each version gets its own saved_query with naming: {endpoint_name}_v{version}
        """
        try:
            self._enable_materialization_inner(endpoint, data_freshness_seconds, version, bucket_overrides)
            ENDPOINT_MATERIALIZATION_EVENT_TOTAL.labels(action="enable", status="success").inc()
            target_version = version or endpoint.get_version()
            if target_version and target_version.saved_query:
                log_activity(
                    organization_id=self.team.organization_id,
                    team_id=self.team.pk,
                    user=self.user,
                    was_impersonated=is_impersonated_session(self.request),
                    item_id=str(target_version.saved_query.id),
                    scope="DataWarehouseSavedQuery",
                    activity="materialization_enabled",
                    detail=Detail(
                        name=target_version.saved_query.name,
                        context=EndpointContext(version=target_version.version),
                    ),
                )
        except ValidationError:
            ENDPOINT_MATERIALIZATION_EVENT_TOTAL.labels(action="enable", status="validation_error").inc()
            raise
        except Exception:
            ENDPOINT_MATERIALIZATION_EVENT_TOTAL.labels(action="enable", status="error").inc()
            raise ValidationError("Failed to enable materialization.")

    def _enable_materialization_inner(
        self,
        endpoint: Endpoint,
        data_freshness_seconds: int,
        version: EndpointVersion | None = None,
        bucket_overrides: dict[str, str] | None = None,
    ) -> None:
        version = version or endpoint.get_version()

        can_mat, reason = version.can_materialize()
        if not can_mat:
            raise ValidationError(f"Cannot materialize endpoint. Reason: {reason}")

        # Per-version naming allows independent materialization for each version
        saved_query_name = f"{endpoint.name}_v{version.version}"
        saved_query = DataWarehouseSavedQuery.objects.filter(
            name=saved_query_name, team=self.team, deleted=False
        ).first()
        if saved_query is None:
            saved_query = DataWarehouseSavedQuery(
                name=saved_query_name,
                team=self.team,
                origin=DataWarehouseSavedQuery.Origin.ENDPOINT,
            )

        hogql_query = build_endpoint_hogql(version.query, self.team, bucket_overrides=bucket_overrides)

        saved_query.query = hogql_query
        saved_query.external_tables = saved_query.s3_tables
        saved_query.is_materialized = True
        saved_query.sync_frequency_interval = sync_frequency_to_sync_frequency_interval(
            DATA_FRESHNESS_BUCKETS[data_freshness_seconds]
        )

        saved_query.save()

        version.saved_query = saved_query
        version.bucket_overrides = bucket_overrides
        version.save(update_fields=["saved_query", "bucket_overrides", "updated_at"])

        saved_query.schedule_materialization()

        try:
            sync_saved_query_to_dag(saved_query)
        except Exception as e:
            logger.exception(
                "Failed to sync endpoint node to DAG",
                endpoint_name=endpoint.name,
                saved_query_id=version.saved_query.id if version and version.saved_query else None,
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

    def disable_materialization(self, endpoint: Endpoint, version: EndpointVersion | None = None) -> None:
        """Disable materialization for an endpoint version.

        If version is not specified, uses the current version.
        """
        version = version or endpoint.get_version()
        if version:
            if version.saved_query:
                saved_query_id = str(version.saved_query.id)
                saved_query_name = version.saved_query.name
                try:
                    delete_node_from_dag(version.saved_query)
                except Exception as e:
                    logger.exception(
                        "Failed to remove endpoint node from DAG",
                        endpoint_name=endpoint.name,
                        saved_query_id=version.saved_query.id if version and version.saved_query else None,
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
                try:
                    version.disable_materialization()
                except Exception:
                    ENDPOINT_MATERIALIZATION_EVENT_TOTAL.labels(action="disable", status="error").inc()
                    raise
                ENDPOINT_MATERIALIZATION_EVENT_TOTAL.labels(action="disable", status="success").inc()
                log_activity(
                    organization_id=self.team.organization_id,
                    team_id=self.team.pk,
                    user=self.user,
                    was_impersonated=is_impersonated_session(self.request),
                    item_id=saved_query_id,
                    scope="DataWarehouseSavedQuery",
                    activity="materialization_disabled",
                    detail=Detail(
                        name=saved_query_name,
                        context=EndpointContext(version=version.version),
                    ),
                )
        clear_endpoint_materialization_cache(self.team.pk, endpoint.name)

    def preview(
        self,
        endpoint: Endpoint,
        version: EndpointVersion,
        bucket_overrides: dict[str, str] | None = None,
    ) -> dict:
        """Preview the materialization transform without enabling it.

        Returns the response payload: transformed query, range pair info, and
        aggregate re-aggregation info.
        """
        can_mat, reason = version.can_materialize()
        if not can_mat:
            return cant_materialize_preview(reason)

        hogql_query = convert_insight_query_to_hogql(version.query, self.team)

        range_pairs: list[dict] = []
        aggregates: list[dict] = []
        transformed_query_str: str | None = None
        variable_infos: list = []

        if version.query.get("variables"):
            can_materialize_vars, var_reason, variable_infos = analyze_variables_for_materialization(
                version.query, bucket_overrides=bucket_overrides
            )

            if not can_materialize_vars:
                return cant_materialize_preview(var_reason)

            if variable_infos:
                transformed = transform_query_for_materialization(
                    hogql_query, variable_infos, self.team, bucket_overrides=bucket_overrides
                )
                transformed_query_str = transformed.get("query")

                # Extract range pairs grouped by column
                seen_columns: dict[str, dict] = {}
                for v in variable_infos:
                    if v.bucket_fn is not None:
                        col_key = ".".join(v.column_chain) if v.column_chain else v.column_expression
                        if col_key not in seen_columns:
                            seen_columns[col_key] = {
                                "column": col_key,
                                "variables": [],
                                "bucket_fn": v.bucket_fn,
                            }
                        seen_columns[col_key]["variables"].append(v.code_name)
                range_pairs = list(seen_columns.values())

                query_str = hogql_query.get("query", "")
                if query_str:
                    try:
                        parsed = parse_select(query_str)
                    except Exception:
                        logger.warning("materialization_preview: failed to parse HogQL for aggregate extraction")
                    else:
                        if isinstance(parsed, ast.SelectQuery) and parsed.select:
                            for expr in parsed.select:
                                agg_name = _extract_aggregate_name(expr)
                                if agg_name:
                                    reagg_info = get_reaggregation(agg_name)
                                    reagg = reagg_info.reaggregate_fn if reagg_info else None
                                    if isinstance(expr, ast.Alias):
                                        label = expr.alias
                                    else:
                                        label = expr.to_hogql()
                                    aggregates.append(
                                        {
                                            "expression": label,
                                            "reaggregate_fn": reagg,
                                        }
                                    )
        else:
            # No variables — just show the converted query as-is
            transformed_query_str = hogql_query.get("query")

        # Build the execution query preview — what runs at request time against the materialized table
        execution_query_str: str | None = None
        display_execution_query_str: str | None = None
        try:
            saved_query_name = f"{endpoint.name}_v{version.version}"
            strategy = strategy_for(endpoint, version, self.team)

            def _build_exec_preview(table_name: str) -> ast.SelectQuery:
                q, _ = strategy.build_materialized_select(
                    table_name=table_name,
                    variable_infos=variable_infos or None,
                )
                for v in variable_infos:
                    apply_where_filter(
                        q,
                        v.code_name,
                        f"{{variables.{v.code_name}}}",
                        op=v.operator,
                        value_wrapper_fns=v.value_wrapper_fns,
                        bucket_fn=v.bucket_fn,
                    )
                return q

            # Each call builds a fresh SelectQuery, so WHERE mutations don't leak between calls
            execution_query_str = to_printed_hogql(_build_exec_preview(saved_query_name), team=self.team)

            # Display variant uses the friendly endpoint name — printed without type resolution
            # since the friendly name isn't a real table in the database
            display_execution_query_str = print_prepared_ast(
                node=_build_exec_preview(endpoint.name),
                context=HogQLContext(team_id=self.team.pk, enable_select_queries=True),
                dialect="hogql",
                pretty=True,
            )
        except Exception:
            logger.debug("Failed to build execution query preview", exc_info=True)

        return {
            "can_materialize": True,
            "reason": None,
            "transformed_query": transformed_query_str,
            "execution_query": execution_query_str,
            "display_execution_query": display_execution_query_str,
            "range_pairs": range_pairs,
            "aggregates": aggregates,
        }
