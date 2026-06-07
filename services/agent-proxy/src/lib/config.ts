// Environment variable loading and validation for the agent-proxy service.
//
// Required vars (must be present in production / NODE_ENV=production):
//   REDIS_URL
//   SANDBOX_JWT_PUBLIC_KEY
//   AGENT_PROXY_DJANGO_CALLBACK_URL
//
// Optional with defaults:
//   TASKS_AGENT_PROXY_CORS_ORIGINS  — comma-separated origins; '' disables CORS
//   PORT                            — default 8003
//   HOST                            — default '0.0.0.0'
//   SHUTDOWN_GRACE_MS               — default 300000 (5 min)
//   SHUTDOWN_PRESTOP_DELAY_MS       — default 0

import { getEnv, type KnownEnvKey } from './env.js'
import { logger } from './logging.js'

export interface Config {
    redisUrl: string
    // PEM string with real newlines (backslash-n sequences normalized before storage)
    sandboxJwtPublicKeyPem: string
    // Parsed from comma-separated TASKS_AGENT_PROXY_CORS_ORIGINS; '*' = all origins
    corsOrigins: Set<string>
    // Base URL of the internal Django service (no trailing slash)
    djangoCallbackBaseUrl: string
    // Shared secret sent as X-Agent-Proxy-Secret on the Django callback so Django can prove the call
    // came from this proxy and not directly from a sandbox. Empty disables it (local/dev).
    agentProxyCallbackSecret: string
    port: number
    host: string
    shutdownGraceMs: number
    shutdownPrestopDelayMs: number
}

// Replace every literal two-character sequence `\n` (backslash + n, as stored
// in environment variables) with a real newline (0x0A), so the PEM can be
// parsed by importSPKI. Must run before any call to importSPKI.
export function normalizePemKey(raw: string): string {
    return raw.replace(/\\n/g, '\n')
}

function parseCorsOrigins(raw: string): Set<string> {
    if (!raw.trim()) {
        return new Set()
    }
    return new Set(
        raw
            .split(',')
            .map((s) => s.trim())
            .filter(Boolean)
    )
}

function requireEnv(name: KnownEnvKey, isProd: boolean): string {
    const value = getEnv(name)
    if (!value) {
        if (isProd) {
            logger.error('config:missing_required_env', { name })
            process.exit(1)
        }
        return ''
    }
    return value
}

export function loadConfig(): Config {
    const isProd = getEnv('NODE_ENV') === 'production'

    // nosemgrep: trailofbits.generic.redis-unencrypted-transport.redis-unencrypted-transport
    const redisUrl = getEnv('REDIS_URL') ?? (isProd ? requireEnv('REDIS_URL', true) : 'redis://localhost:6379')

    const rawPublicKey = requireEnv('SANDBOX_JWT_PUBLIC_KEY', isProd)
    const sandboxJwtPublicKeyPem = normalizePemKey(rawPublicKey)

    const djangoCallbackBaseUrl = requireEnv('AGENT_PROXY_DJANGO_CALLBACK_URL', isProd)

    const agentProxyCallbackSecret = getEnv('AGENT_PROXY_CALLBACK_SECRET') ?? ''

    const corsOrigins = parseCorsOrigins(getEnv('TASKS_AGENT_PROXY_CORS_ORIGINS') ?? '')

    const portRaw = getEnv('PORT')
    const port = portRaw !== undefined ? parseInt(portRaw, 10) : 8003
    if (Number.isNaN(port)) {
        logger.error('config:invalid_port', { raw: portRaw })
        process.exit(1)
    }

    const host = getEnv('HOST') ?? '0.0.0.0'

    const graceRaw = getEnv('SHUTDOWN_GRACE_MS')
    const shutdownGraceMs = graceRaw !== undefined ? parseInt(graceRaw, 10) : 300_000
    const prestopRaw = getEnv('SHUTDOWN_PRESTOP_DELAY_MS')
    const shutdownPrestopDelayMs = prestopRaw !== undefined ? parseInt(prestopRaw, 10) : 0

    return {
        redisUrl,
        sandboxJwtPublicKeyPem,
        corsOrigins,
        djangoCallbackBaseUrl,
        agentProxyCallbackSecret,
        port,
        host,
        shutdownGraceMs,
        shutdownPrestopDelayMs,
    }
}
