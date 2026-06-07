// JWT verification for the agent-proxy service.
//
// Verify-only: this service never signs tokens. The private key is never loaded.
// Both legs (stream-read and sandbox event ingest) use the same RS256 public key
// from SANDBOX_JWT_PUBLIC_KEY (normalized in config.ts before reaching here).
//
// Kid-based key rotation is NOT implemented. When jwtVerify receives a concrete
// CryptoKey (rather than a JWKS), jose ignores the kid header entirely — which is
// the intended behavior per the design (single configured key, no kid logic needed).

import { importSPKI, jwtVerify } from 'jose'

import { SANDBOX_EVENT_INGEST_AUDIENCE, STREAM_READ_AUDIENCE } from './constants.js'
import type { SandboxEventIngestTokenPayload, StreamReadTokenPayload } from './types.js'

// ---------------------------------------------------------------------------
// Public key loading
// ---------------------------------------------------------------------------

// Call once at startup with config.sandboxJwtPublicKeyPem (already normalized).
// The returned CryptoKey is cached by the caller for the process lifetime.
export async function loadPublicKey(pemRaw: string): Promise<CryptoKey> {
    return importSPKI(pemRaw, 'RS256')
}

// ---------------------------------------------------------------------------
// Shared claim extraction
// ---------------------------------------------------------------------------

// Extract and validate the three required claims shared by both token types.
// Throws a plain Error (mapped to 401 by the server) on any claim type violation.
//
// Validation mirrors the Python validate_* helpers exactly:
//   - run_id: must be a string
//   - task_id: must be a string
//   - team_id: must be an integer (Number.isInteger rejects floats, booleans,
//     strings, null — booleans fail because typeof true === 'boolean', not
//     'number', so Number.isInteger(true) is false)
function assertStreamClaims(payload: Record<string, unknown>): { runId: string; taskId: string; teamId: number } {
    const runId = payload['run_id']
    const taskId = payload['task_id']
    const teamId = payload['team_id']

    if (typeof runId !== 'string') {
        throw new Error('Token has invalid claim: run_id must be a string')
    }
    if (typeof taskId !== 'string') {
        throw new Error('Token has invalid claim: task_id must be a string')
    }
    if (!Number.isInteger(teamId)) {
        throw new Error('Token has invalid claim: team_id must be an integer')
    }

    return { runId, taskId, teamId: teamId as number }
}

// ---------------------------------------------------------------------------
// Stream-read token  (GET /v1/runs/:run/stream leg)
// ---------------------------------------------------------------------------

// Audience: posthog:stream_read
// Required claims: run_id (string), task_id (string), team_id (integer)
// Algorithm: RS256, no clockTolerance (matches Python leeway=0 default)
//
// Throws jose error subtypes (JWTExpired, JWTInvalid, JWSSignatureVerificationFailed,
// JWTClaimValidationFailed, etc.) on bad signature, wrong audience or expiry; throws
// a plain Error on malformed claim types. The server maps all of these to 401.
export async function validateStreamReadToken(token: string, publicKey: CryptoKey): Promise<StreamReadTokenPayload> {
    const { payload } = await jwtVerify(token, publicKey, {
        algorithms: ['RS256'],
        audience: STREAM_READ_AUDIENCE,
    })

    const claims = assertStreamClaims(payload as Record<string, unknown>)
    return claims
}

// ---------------------------------------------------------------------------
// Sandbox event ingest token  (POST /v1/runs/:run/ingest leg)
// ---------------------------------------------------------------------------

// Audience: posthog:sandbox_event_ingest
// Required claims: run_id (string), task_id (string), team_id (integer)
// Algorithm: RS256, no clockTolerance (matches Python leeway=0 default)
//
// Same error semantics as validateStreamReadToken.
export async function validateSandboxEventIngestToken(
    token: string,
    publicKey: CryptoKey
): Promise<SandboxEventIngestTokenPayload> {
    const { payload } = await jwtVerify(token, publicKey, {
        algorithms: ['RS256'],
        audience: SANDBOX_EVENT_INGEST_AUDIENCE,
    })

    const claims = assertStreamClaims(payload as Record<string, unknown>)
    return claims
}
