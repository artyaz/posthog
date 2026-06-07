import { expectLogic } from 'kea-test-utils'
import { ReadableStream as NodeReadableStream } from 'stream/web'

import { initKeaTests } from '~/test/init'

import { OriginProduct, Task, TaskRun, TaskRunEnvironment, TaskRunStatus } from '../types'
import { taskDetailSceneLogic } from './taskDetailSceneLogic'
import { tasksLogic } from './tasksLogic'

const createMockTask = (id: string): Task => ({
    id,
    task_number: 1,
    slug: `task-${id}`,
    title: `Task ${id}`,
    description: 'A test task',
    origin_product: OriginProduct.USER_CREATED,
    repository: 'test/repo',
    github_integration: null,
    json_schema: null,
    internal: false,
    latest_run: null,
    created_at: '2024-01-01T00:00:00Z',
    updated_at: '2024-01-01T00:00:00Z',
    created_by: null,
})

const createMockRun = (id: string, status: TaskRunStatus): TaskRun => ({
    id,
    task: 'task-123',
    stage: null,
    branch: null,
    status,
    environment: TaskRunEnvironment.CLOUD,
    log_url: null,
    error_message: null,
    output: null,
    state: {},
    artifacts: [],
    created_at: '2024-01-01T00:00:00Z',
    updated_at: '2024-01-01T00:00:00Z',
    completed_at: null,
})

function buildReadableStream(chunks: string[], keepOpen = false): ReadableStream<Uint8Array> {
    const encoder = new TextEncoder()
    let index = 0
    const StreamConstructor = globalThis.ReadableStream ?? NodeReadableStream

    return new StreamConstructor({
        pull(controller) {
            if (index < chunks.length) {
                controller.enqueue(encoder.encode(chunks[index]))
                index += 1
            } else if (!keepOpen) {
                controller.close()
            }
        },
    })
}

function createSseResponse(chunks: string[], keepOpen = false): Response {
    return {
        ok: true,
        body: buildReadableStream(chunks, keepOpen),
    } as Response
}

function createConsoleSseEvent(id: string, message: string): string {
    return `id: ${id}\nevent: message\ndata: ${JSON.stringify({
        type: 'notification',
        timestamp: '2024-01-01T00:00:00Z',
        notification: {
            jsonrpc: '2.0',
            method: '_posthog/console',
            params: { level: 'info', message },
        },
    })}\n\n`
}

function createToolCallSseEvent(id: string, toolCallId: string, status: string, rawOutput?: unknown): string {
    return `id: ${id}\nevent: message\ndata: ${JSON.stringify({
        type: 'notification',
        timestamp: '2024-01-01T00:00:00Z',
        notification: {
            jsonrpc: '2.0',
            method: 'session/update',
            params: {
                update: {
                    sessionUpdate: status === 'pending' || status === 'in_progress' ? 'tool_call' : 'tool_call_update',
                    toolCallId,
                    title: 'Read file',
                    status,
                    rawInput: { path: 'README.md' },
                    ...(rawOutput !== undefined ? { rawOutput } : {}),
                },
            },
        },
    })}\n\n`
}

function createJsonResponse(payload: unknown): Response {
    return new Response(JSON.stringify(payload), {
        headers: { 'Content-Type': 'application/json' },
    })
}

function createFetchMock({
    runs = {},
    streamResponses = [],
    streamBaseUrl = null,
}: {
    runs?: Record<string, TaskRun>
    streamResponses?: Response[]
    streamBaseUrl?: string | null
} = {}): typeof fetch {
    return jest.fn((input: RequestInfo | URL) => {
        const url = String(input)
        const streamTokenMatch = url.match(/\/tasks\/([^/]+)\/runs\/([^/]+)\/stream_token\/$/)
        const taskRunMatch = url.match(/\/tasks\/([^/]+)\/runs\/([^/]+)\/$/)
        // Matches both the Django read path (.../runs/:run/stream/) and the proxy path (/v1/runs/:run/stream).
        const streamMatch = url.match(/\/runs\/([^/]+)\/stream\/?(\?|$)/)
        const logsMatch = url.match(/\/tasks\/([^/]+)\/runs\/([^/]+)\/logs\/$/)
        const runsListMatch = url.match(/\/tasks\/([^/]+)\/runs\/$/)
        const taskMatch = url.match(/\/tasks\/([^/]+)\/$/)

        if (streamTokenMatch) {
            return Promise.resolve(createJsonResponse({ token: 'proxy-test-token', stream_base_url: streamBaseUrl }))
        }
        if (streamMatch) {
            const nextResponse = streamResponses.shift()
            if (!nextResponse) {
                throw new Error(`Missing stream response for ${url}`)
            }
            return Promise.resolve(nextResponse)
        }
        if (logsMatch) {
            return Promise.resolve(new Response(''))
        }
        if (taskRunMatch) {
            return Promise.resolve(
                createJsonResponse(runs[taskRunMatch[2]] ?? createMockRun(taskRunMatch[2], TaskRunStatus.COMPLETED))
            )
        }
        if (runsListMatch) {
            return Promise.resolve(createJsonResponse({ results: [] }))
        }
        if (taskMatch) {
            return Promise.resolve(createJsonResponse(createMockTask(taskMatch[1])))
        }
        throw new Error(`Unexpected fetch url: ${url}`)
    }) as typeof fetch
}

async function flushStreaming(): Promise<void> {
    // fetch-event-source has a deeper internal async chain (fetch -> onopen -> getBytes
    // -> getLines -> getMessages -> onmessage), so flush several macro+micro cycles.
    for (let i = 0; i < 6; i++) {
        await Promise.resolve()
        await new Promise((resolve) => setTimeout(resolve, 0))
    }
}

describe('taskDetailSceneLogic', () => {
    const originalFetch = global.fetch

    beforeEach(() => {
        initKeaTests()
        global.fetch = createFetchMock()
    })

    afterEach(() => {
        jest.restoreAllMocks()
        global.fetch = originalFetch
    })

    describe('setSelectedRunId cross-talk prevention', () => {
        it('only updates selectedRunId for the matching taskId', async () => {
            const logicA = taskDetailSceneLogic({ taskId: 'task-A' })
            const logicB = taskDetailSceneLogic({ taskId: 'task-B' })
            logicA.mount()
            logicB.mount()
            await expectLogic(logicA).toFinishAllListeners()
            await expectLogic(logicB).toFinishAllListeners()

            expect(logicA.values.selectedRunId).toBe(null)
            expect(logicB.values.selectedRunId).toBe(null)

            logicA.actions.setSelectedRunId('run-A', 'task-A')
            await expectLogic(logicA).toFinishAllListeners()
            await expectLogic(logicB).toFinishAllListeners()

            expect(logicA.values.selectedRunId).toBe('run-A')
            expect(logicB.values.selectedRunId).toBe(null)

            logicA.unmount()
            logicB.unmount()
        })

        it('runTaskSuccess only processes events for its own task', async () => {
            const logicA = taskDetailSceneLogic({ taskId: 'task-A' })
            const logicB = taskDetailSceneLogic({ taskId: 'task-B' })
            logicA.mount()
            logicB.mount()
            await expectLogic(logicA).toFinishAllListeners()
            await expectLogic(logicB).toFinishAllListeners()

            const taskAResult = {
                ...createMockTask('task-A'),
                latest_run: createMockRun('run-A', TaskRunStatus.QUEUED),
            }
            logicA.actions.runTaskSuccess(taskAResult)
            await expectLogic(logicA).toFinishAllListeners()
            await expectLogic(logicB).toFinishAllListeners()

            expect(logicA.values.selectedRunId).toBe('run-A')
            expect(logicB.values.selectedRunId).toBe(null)

            logicA.unmount()
            logicB.unmount()
        })
    })

    describe('updateRun', () => {
        it('updates run status in runs list when polling', async () => {
            const logic = taskDetailSceneLogic({ taskId: 'task-123' })
            logic.mount()

            const initialRun = createMockRun('run-456', TaskRunStatus.QUEUED)
            logic.actions.loadRunsSuccess([initialRun])
            expect(logic.values.runs[0].status).toBe(TaskRunStatus.QUEUED)

            const updatedRun = createMockRun('run-456', TaskRunStatus.IN_PROGRESS)
            logic.actions.updateRun(updatedRun)

            expect(logic.values.runs[0].status).toBe(TaskRunStatus.IN_PROGRESS)
            logic.unmount()
        })

        it('does not affect other runs in the list', async () => {
            const logic = taskDetailSceneLogic({ taskId: 'task-123' })
            logic.mount()

            const run1 = createMockRun('run-1', TaskRunStatus.COMPLETED)
            const run2 = createMockRun('run-2', TaskRunStatus.QUEUED)
            logic.actions.loadRunsSuccess([run1, run2])

            const updatedRun2 = createMockRun('run-2', TaskRunStatus.IN_PROGRESS)
            logic.actions.updateRun(updatedRun2)

            expect(logic.values.runs[0].status).toBe(TaskRunStatus.COMPLETED)
            expect(logic.values.runs[1].status).toBe(TaskRunStatus.IN_PROGRESS)
            logic.unmount()
        })
    })

    describe('loadTaskSuccess updates tasksLogic', () => {
        it('updates sidebar tasks list when task loads', async () => {
            const tasksLogicInstance = tasksLogic()
            tasksLogicInstance.mount()
            const mockTask = createMockTask('task-123')
            tasksLogicInstance.actions.loadTasksSuccess([mockTask])

            const logic = taskDetailSceneLogic({ taskId: 'task-123' })
            logic.mount()
            await expectLogic(logic).toFinishAllListeners()

            const updatedTask = { ...mockTask, title: 'New Title' }
            logic.actions.loadTaskSuccess(updatedTask)
            await expectLogic(logic).toFinishAllListeners()

            expect(tasksLogicInstance.values.tasks.find((t) => t.id === 'task-123')?.title).toBe('New Title')

            logic.unmount()
            tasksLogicInstance.unmount()
        })
    })

    describe('streaming', () => {
        const getHeader = (init: RequestInit | undefined, name: string): string | undefined => {
            const headers = (init?.headers ?? {}) as Record<string, string>
            const key = Object.keys(headers).find((candidate) => candidate.toLowerCase() === name.toLowerCase())
            return key !== undefined ? headers[key] : undefined
        }

        const streamFetchCalls = (): [RequestInfo | URL, RequestInit | undefined][] =>
            (global.fetch as jest.Mock).mock.calls.filter(([url]) => /\/runs\/[^/]+\/stream\/?(\?|$)/.test(String(url)))

        beforeEach(() => {
            window.sessionStorage.clear()
        })

        afterEach(() => {
            jest.useRealTimers()
        })

        it('streams events, dedupes by id, and resumes from the last event id on restart', async () => {
            const run = createMockRun('run-1', TaskRunStatus.IN_PROGRESS)
            global.fetch = createFetchMock({
                runs: { [run.id]: run },
                streamResponses: [
                    createSseResponse(
                        [
                            'id: 1-0\nevent: message\ndata: {"type":"user","content":"hello"}\n\n',
                            'id: 2-0\nevent: message\ndata: {"type":"assistant","content":"world"}\n\n',
                        ],
                        true
                    ),
                    createSseResponse(
                        [
                            'id: 1-0\nevent: message\ndata: {"type":"user","content":"hello"}\n\n',
                            'event: keepalive\ndata: {"status":"ok"}\n\n',
                            'id: 3-0\nevent: message\ndata: {"type":"assistant","content":"again"}\n\n',
                        ],
                        true
                    ),
                ],
            })

            const logic = taskDetailSceneLogic({ taskId: 'task-123' })
            logic.mount()
            await expectLogic(logic).toFinishAllListeners()

            logic.actions.setSelectedRunId(run.id, 'task-123')
            await expectLogic(logic).toFinishAllListeners()
            await flushStreaming()

            expect(logic.values.lastStreamEventId).toBe('2-0')
            expect(logic.values.streamEntries.map((entry) => entry.message)).toEqual(['hello', 'world'])

            logic.actions.stopStreaming()
            await expectLogic(logic).toFinishAllListeners()

            logic.actions.startStreaming()
            await expectLogic(logic).toFinishAllListeners()
            await flushStreaming()

            const calls = streamFetchCalls()
            expect(calls).toHaveLength(2)
            expect(getHeader(calls[1][1], 'Last-Event-ID')).toBe('2-0')
            expect(logic.values.lastStreamEventId).toBe('3-0')
            expect(logic.values.streamEntries.map((entry) => entry.message)).toEqual(['hello', 'worldagain'])

            logic.unmount()
        })

        it('resumes from the sessionStorage event id after a page refresh', async () => {
            const run = createMockRun('run-1', TaskRunStatus.IN_PROGRESS)
            window.sessionStorage.setItem('tasks:stream-resume:run-1', '5-0')
            global.fetch = createFetchMock({
                runs: { [run.id]: run },
                streamResponses: [
                    createSseResponse(
                        ['id: 6-0\nevent: message\ndata: {"type":"assistant","content":"resumed"}\n\n'],
                        true
                    ),
                ],
            })

            const logic = taskDetailSceneLogic({ taskId: 'task-123' })
            logic.mount()
            await expectLogic(logic).toFinishAllListeners()

            logic.actions.setSelectedRunId(run.id, 'task-123')
            await expectLogic(logic).toFinishAllListeners()
            await flushStreaming()

            const calls = streamFetchCalls()
            expect(calls).toHaveLength(1)
            expect(getHeader(calls[0][1], 'Last-Event-ID')).toBe('5-0')
            expect(logic.values.streamEntries.map((entry) => entry.message)).toEqual(['resumed'])

            logic.unmount()
        })

        it('stops on a stream-end event for a terminal run without reconnecting or downgrading to polling', async () => {
            const run = createMockRun('run-1', TaskRunStatus.COMPLETED)
            global.fetch = createFetchMock({
                runs: { [run.id]: run },
                streamResponses: [
                    createSseResponse([
                        createConsoleSseEvent('1-0', 'hello'),
                        'event: stream-end\ndata: {"status":"complete"}\n\n',
                    ]),
                ],
            })

            const logic = taskDetailSceneLogic({ taskId: 'task-123' })
            logic.mount()
            await expectLogic(logic).toFinishAllListeners()

            logic.actions.setSelectedRunId(run.id, 'task-123')
            await expectLogic(logic).toFinishAllListeners()
            await flushStreaming()

            expect(logic.values.streamComplete).toBe(true)
            expect(logic.values.streamingFailed).toBe(false)
            expect(logic.values.isStreaming).toBe(false)
            expect(logic.values.streamEntries.map((entry) => entry.message)).toEqual(['hello'])
            expect(streamFetchCalls()).toHaveLength(1)
            expect(window.sessionStorage.getItem('tasks:stream-resume:run-1')).toBeNull()

            logic.unmount()
        })

        it('polls after a stream-end event when the refreshed run is still in progress', async () => {
            const setIntervalSpy = jest.spyOn(window, 'setInterval')
            const run = createMockRun('run-1', TaskRunStatus.IN_PROGRESS)
            global.fetch = createFetchMock({
                runs: { [run.id]: run },
                streamResponses: [
                    createSseResponse([
                        createConsoleSseEvent('1-0', 'hello'),
                        'event: stream-end\ndata: {"status":"complete"}\n\n',
                    ]),
                ],
            })

            const logic = taskDetailSceneLogic({ taskId: 'task-123' })
            logic.mount()
            await expectLogic(logic).toFinishAllListeners()

            logic.actions.setSelectedRunId(run.id, 'task-123')
            await expectLogic(logic).toFinishAllListeners()
            await flushStreaming()

            expect(logic.values.streamComplete).toBe(true)
            expect(logic.values.streamingFailed).toBe(false)
            expect(logic.values.isStreaming).toBe(false)
            expect(streamFetchCalls()).toHaveLength(1)
            expect(setIntervalSpy).toHaveBeenCalledWith(expect.any(Function), 1000)

            logic.unmount()
        })

        it('routes the stream through the proxy when the server resolves a stream base url', async () => {
            const run = createMockRun('run-1', TaskRunStatus.IN_PROGRESS)
            global.fetch = createFetchMock({
                runs: { [run.id]: run },
                streamBaseUrl: 'https://proxy.example/',
                streamResponses: [
                    createSseResponse(['id: 1-0\nevent: message\ndata: {"type":"assistant","content":"hi"}\n\n'], true),
                ],
            })

            const logic = taskDetailSceneLogic({ taskId: 'task-123' })
            logic.mount()
            await expectLogic(logic).toFinishAllListeners()

            logic.actions.setSelectedRunId(run.id, 'task-123')
            await expectLogic(logic).toFinishAllListeners()
            await flushStreaming()

            const calls = streamFetchCalls()
            expect(calls).toHaveLength(1)
            expect(String(calls[0][0])).toBe('https://proxy.example/v1/runs/run-1/stream')
            expect(getHeader(calls[0][1], 'Authorization')).toBe('Bearer proxy-test-token')
            expect(logic.values.streamEntries.map((entry) => entry.message)).toEqual(['hi'])

            logic.unmount()
        })

        it('reconnects after a dropped connection without latching to polling', async () => {
            jest.useFakeTimers()
            const run = createMockRun('run-1', TaskRunStatus.IN_PROGRESS)
            global.fetch = createFetchMock({
                runs: { [run.id]: run },
                streamResponses: [
                    // First connection emits one event then the body ends (a drop)
                    createSseResponse([createConsoleSseEvent('1-0', 'first message')]),
                    // Reconnect picks up the next event and stays open
                    createSseResponse([createConsoleSseEvent('2-0', 'second message')], true),
                ],
            })

            const logic = taskDetailSceneLogic({ taskId: 'task-123' })
            logic.mount()
            await jest.advanceTimersByTimeAsync(0)

            logic.actions.setSelectedRunId(run.id, 'task-123')
            await jest.advanceTimersByTimeAsync(0)

            // A drop must not immediately downgrade to polling
            expect(logic.values.streamingFailed).toBe(false)

            // Advance past the first reconnect backoff (1000ms base)
            await jest.advanceTimersByTimeAsync(1000)
            await jest.advanceTimersByTimeAsync(0)

            const calls = streamFetchCalls()
            expect(calls).toHaveLength(2)
            expect(logic.values.streamingFailed).toBe(false)
            expect(logic.values.streamEntries.map((entry) => entry.message)).toEqual([
                'first message',
                'second message',
            ])

            logic.unmount()
        })

        it('does not restart an active stream when selected run reloads', async () => {
            const run = createMockRun('run-1', TaskRunStatus.IN_PROGRESS)
            global.fetch = createFetchMock({
                runs: { [run.id]: run },
                streamResponses: [
                    createSseResponse(
                        ['id: 1-0\nevent: message\ndata: {"type":"assistant","content":"hello"}\n\n'],
                        true
                    ),
                ],
            })

            const logic = taskDetailSceneLogic({ taskId: 'task-123' })
            logic.mount()
            await expectLogic(logic).toFinishAllListeners()

            logic.actions.setSelectedRunId(run.id, 'task-123')
            await expectLogic(logic).toFinishAllListeners()
            await flushStreaming()

            expect(streamFetchCalls()).toHaveLength(1)

            logic.actions.loadSelectedRunSuccess(run)
            await expectLogic(logic).toFinishAllListeners()
            await flushStreaming()

            expect(streamFetchCalls()).toHaveLength(1)
            expect(logic.values.streamEntries.map((entry) => entry.message)).toEqual(['hello'])

            logic.unmount()
        })

        it('ignores duplicate event ids that arrive in the same chunk', async () => {
            const run = createMockRun('run-1', TaskRunStatus.IN_PROGRESS)
            global.fetch = createFetchMock({
                runs: { [run.id]: run },
                streamResponses: [
                    createSseResponse(
                        [
                            [
                                'id: 1-0',
                                'event: message',
                                'data: {"type":"assistant","content":"hello"}',
                                '',
                                'id: 1-0',
                                'event: message',
                                'data: {"type":"assistant","content":"hello"}',
                                '',
                            ].join('\n'),
                        ],
                        true
                    ),
                ],
            })

            const logic = taskDetailSceneLogic({ taskId: 'task-123' })
            logic.mount()
            await expectLogic(logic).toFinishAllListeners()

            logic.actions.setSelectedRunId(run.id, 'task-123')
            await expectLogic(logic).toFinishAllListeners()
            await flushStreaming()

            expect(logic.values.lastStreamEventId).toBe('1-0')
            expect(logic.values.streamEntries.map((entry) => entry.id)).toEqual(['stream-1-0'])
            expect(logic.values.streamEntries.map((entry) => entry.message)).toEqual(['hello'])

            logic.unmount()
        })

        it('preserves tool state across resumed streams', async () => {
            const run = createMockRun('run-1', TaskRunStatus.IN_PROGRESS)
            global.fetch = createFetchMock({
                runs: { [run.id]: run },
                streamResponses: [
                    createSseResponse([createToolCallSseEvent('1-0', 'tool-1', 'in_progress')], true),
                    createSseResponse(
                        [
                            createToolCallSseEvent('1-0', 'tool-1', 'in_progress'),
                            createToolCallSseEvent('2-0', 'tool-1', 'completed', { content: 'done' }),
                        ],
                        true
                    ),
                ],
            })

            const logic = taskDetailSceneLogic({ taskId: 'task-123' })
            logic.mount()
            await expectLogic(logic).toFinishAllListeners()

            logic.actions.setSelectedRunId(run.id, 'task-123')
            await expectLogic(logic).toFinishAllListeners()
            await flushStreaming()

            const firstToolEntry = logic.values.streamEntries[0]
            expect(logic.values.streamEntries).toHaveLength(1)
            expect(firstToolEntry.type).toBe('tool')
            expect(firstToolEntry.id).toBe('stream-1-0')
            expect(firstToolEntry.toolStatus).toBe('running')

            logic.actions.stopStreaming()
            await expectLogic(logic).toFinishAllListeners()

            logic.actions.startStreaming()
            await expectLogic(logic).toFinishAllListeners()
            await flushStreaming()

            expect(logic.values.lastStreamEventId).toBe('2-0')
            expect(logic.values.streamEntries).toHaveLength(1)
            expect(logic.values.streamEntries[0]).toMatchObject({
                id: 'stream-1-0',
                type: 'tool',
                toolCallId: 'tool-1',
                toolStatus: 'completed',
                toolResult: { content: 'done' },
            })
            expect(logic.values.streamEntries[0]).not.toBe(firstToolEntry)

            logic.unmount()
        })
    })
})
