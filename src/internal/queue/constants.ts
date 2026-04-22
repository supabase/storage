/**
 * Placeholder tenant for events that run across the whole fleet
 * (e.g. pg-boss maintenance) and have no single tenant context.
 */
export const SYSTEM_TENANT = { ref: '', host: '' } as const
