export const HTTP_SIZE_METRICS_MAX_STATES = 4096

// The OTel SDK reserves one aggregation cardinality slot for overflow.
export const HTTP_SIZE_METRICS_AGGREGATION_CARDINALITY_LIMIT = HTTP_SIZE_METRICS_MAX_STATES + 1
