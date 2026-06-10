DATA_FRESHNESS_BUCKETS: dict[int, str] = {
    900: "15min",
    1800: "30min",
    3600: "1hour",
    21600: "6hour",
    43200: "12hour",
    86400: "24hour",
    604800: "7day",
}
VALID_DATA_FRESHNESS_SECONDS: frozenset[int] = frozenset(DATA_FRESHNESS_BUCKETS)
DEFAULT_DATA_FRESHNESS_SECONDS = 86400

ENDPOINT_NAME_REGEX = r"^[a-zA-Z][a-zA-Z0-9_-]{0,127}$"
