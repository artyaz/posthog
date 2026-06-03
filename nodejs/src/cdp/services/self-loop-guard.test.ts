import { Team } from '../../types'
import { extractRequestApiKey, isPostHogIngestUrl, isSelfReferentialIngestFetch } from './self-loop-guard'

// Synthetic, non-production values. OWN_TOKEN is the project the destination runs in;
// OTHER_TOKEN is a different project (legitimate cross-project replication).
const OWN_TOKEN = 'phc_synthetic_own_0000000000000000'
const OWN_SECRET_TOKEN = 'phsx_synthetic_own_secret_00000000'
const OTHER_TOKEN = 'phc_synthetic_other_111111111111111'

const TEAM: Pick<Team, 'api_token' | 'secret_api_token'> = {
    api_token: OWN_TOKEN,
    secret_api_token: OWN_SECRET_TOKEN,
}

const INGEST_URL = 'https://us.i.posthog.com/capture/'
const LOGS_URL = 'https://us.i.posthog.com/i/v1/logs'
const API_URL = 'https://us.posthog.com/api/projects/100/insights/'
const EXTERNAL_URL = 'https://external.example.com/webhook'

const captureBody = (event: string, properties: Record<string, unknown> = {}, apiKey = OWN_TOKEN): string =>
    JSON.stringify({ api_key: apiKey, event, distinct_id: 'synthetic_user_1', properties })

const detect = (overrides: { url?: string; body?: string | null }): boolean =>
    isSelfReferentialIngestFetch({
        url: overrides.url ?? INGEST_URL,
        body: overrides.body === undefined ? captureBody('replicated_event') : overrides.body,
        team: TEAM,
    })

describe('self-loop-guard', () => {
    describe('isPostHogIngestUrl', () => {
        it.each([
            ['https://us.i.posthog.com/capture/', true],
            ['https://us.i.posthog.com/capture', true],
            ['https://eu.i.posthog.com/batch/', true],
            ['https://us.i.posthog.com/e/', true],
            ['https://us.i.posthog.com/track/', true],
            ['https://us.i.posthog.com/i/v0/e/', true],
            ['https://us.i.posthog.com/capture/?api_key=abc', true],
            ['https://posthog.com/capture', true],
            // observability + REST endpoints are NOT ingestion - cannot form a loop
            ['https://us.i.posthog.com/i/v1/logs', false],
            ['https://us.posthog.com/api/projects/100/insights/', false],
            ['https://us.i.posthog.com/decide', false],
            // non-posthog hosts
            ['https://external.example.com/capture', false],
            ['https://posthog.com.evil.com/capture', false],
            ['https://notposthog.com/capture', false],
            ['not a url', false],
        ])('classifies %s as ingest=%s', (url, expected) => {
            expect(isPostHogIngestUrl(url)).toBe(expected)
        })
    })

    describe('extractRequestApiKey', () => {
        it('reads the top-level api_key field', () => {
            expect(extractRequestApiKey(captureBody('e'), INGEST_URL)).toBe(OWN_TOKEN)
        })

        it.each(['token', 'api_token'])('reads the top-level %s field', (field) => {
            expect(extractRequestApiKey(JSON.stringify({ [field]: OWN_TOKEN }), INGEST_URL)).toBe(OWN_TOKEN)
        })

        it('reads the api_key query parameter when body has none', () => {
            expect(extractRequestApiKey('', `${INGEST_URL}?api_key=${OWN_TOKEN}`)).toBe(OWN_TOKEN)
        })

        it('does NOT treat $lib_token in event properties as the request credential', () => {
            // The SDK auto-attaches the team token as $lib_token on event properties. That
            // is metadata, not an intent to ingest as the project - it must not be matched.
            const body = JSON.stringify({ event: 'ticket_updated', properties: { $lib_token: OWN_TOKEN } })
            expect(extractRequestApiKey(body, API_URL)).toBeNull()
        })

        it('returns null for an unparseable body and no query token', () => {
            expect(extractRequestApiKey('not-json{{', INGEST_URL)).toBeNull()
        })
    })

    describe('isSelfReferentialIngestFetch', () => {
        it('detects a capture to an ingest endpoint with the project own token', () => {
            expect(detect({})).toBe(true)
        })

        it('detects when the project own token is on the URL query string', () => {
            expect(detect({ url: `${INGEST_URL}?api_key=${OWN_TOKEN}`, body: '' })).toBe(true)
        })

        it('detects a capture authenticated with the project secret token', () => {
            expect(detect({ body: captureBody('e', {}, OWN_SECRET_TOKEN) })).toBe(true)
        })

        it('does not flag an external (non-PostHog) fetch', () => {
            expect(detect({ url: EXTERNAL_URL })).toBe(false)
        })

        it('does not flag the observability logs endpoint', () => {
            // A destination that uploads to an external API then ships an observability log
            // to PostHog. The log endpoint is not ingestion -> no loop.
            expect(detect({ url: LOGS_URL })).toBe(false)
        })

        it('does not flag a workflow step posting to a PostHog REST API endpoint', () => {
            // A workflow "update ticket" step calls a PostHog API endpoint and the SDK has
            // auto-attached $lib_token into the body. Neither is an ingestion self-capture.
            const body = JSON.stringify({ status: 'open', properties: { $lib_token: OWN_TOKEN } })
            expect(detect({ url: API_URL, body })).toBe(false)
        })

        it('does not flag cross-project replication (different project token)', () => {
            // Replicator forwarding to a *different* project - the request authenticates with
            // another project's token, so it is not a self-loop.
            expect(detect({ body: captureBody('any_event', {}, OTHER_TOKEN) })).toBe(false)
        })

        it('does not flag when the team token only appears as $lib_token, not the api_key', () => {
            const body = JSON.stringify({ event: 'alpha', properties: { $lib_token: OWN_TOKEN } })
            expect(detect({ body })).toBe(false)
        })

        it('does not flag when the team token is a substring of an unrelated field', () => {
            const body = JSON.stringify({ event: 'alpha', properties: { ref: `${OWN_TOKEN}_extra` } })
            expect(detect({ body })).toBe(false)
        })

        it('does not flag an ingest fetch carrying no credential at all', () => {
            expect(detect({ body: JSON.stringify({ event: 'alpha' }) })).toBe(false)
        })
    })
})
