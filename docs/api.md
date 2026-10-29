# Vino HTTP API

Client-facing reference for the durable-agent server. Use this to build a web
client against the running server.

- **Base URL:** `http://127.0.0.1:8080` (default; configurable via `addr` in
  the server config)
- **Request bodies:** JSON
- **Streaming responses:** [Server-Sent Events
  (SSE)](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events)

## Conventions

- Set `Content-Type: application/json` on requests with a body.
- All JSON responses are encoded as a single object.
- Non-streaming errors use an HTTP error status with a JSON body:
  `{"error":"..."}`.
- Streaming errors (a failure after the run has started) are delivered as a
  final SSE `error` event on an already-`200` stream.

## Endpoints

### `GET /health`

Liveness probe.

**Response `200`**

```json
{"ok":true}
```

### `POST /runs`

Run the agent against the configured provider and stream the result as SSE.

**Request body** — all fields optional except `messages`:

| Field | Type | Meaning |
|---|---|---|
| `messages` | `array` (required) | Prior conversation turns, in order. See below. |
| `systemPrompt` | `string` | System prompt prepended to the conversation. |
| `maxIterations` | `integer` | Loop cap. `0`/absent → default of `50`. |
| `toolTimeout` | `string` | Per-tool timeout as a Go duration string (`"30s"`, `"1m"`). Empty/absent → no timeout. |

`messages[]` entries:

| Field | Type | Meaning |
|---|---|---|
| `role` | `string` | `"user"` or `"assistant"` (tool turns are generated server-side). |
| `content` | `string` | Message text. Must be non-empty. |

**Example request**

```json
{
  "systemPrompt": "You are a helpful coding assistant.",
  "messages": [
    {"role": "user", "content": "list the files in the current directory"}
  ],
  "maxIterations": 0,
  "toolTimeout": "30s"
}
```

**Response `200`** — `Content-Type: text/event-stream`. The body is a sequence
of SSE frames. Each frame is:

```
event: <name>
data: <JSON object>

```

### `GET /conversations`

List conversations, newest first. The harness's internal root conversation
(ID `1`) is excluded.

**Query params**

| Param | Meaning |
|---|---|
| `limit` | Page size. Default `50`, cap `200`. |
| `cursor` | Opaque cursor from a previous page's `nextCursor`. |
| `order` | `desc` (default) or `asc`, by conversation ID. |

**Response `200`**

```json
{
  "conversations": [
    {
      "id": "12",
      "parent": {"conversationId": "3", "at": "40"},
      "owner": {"conversationId": "3", "taskId": "41"},
      "latestMessage": {"role": "assistant", "content": "Here you go…", "truncated": true}
    }
  ],
  "nextCursor": "12"
}
```

- `parent` and `owner` are present only when the conversation is a fork or is
  task-owned.
- `latestMessage` is the last user/assistant turn (skipping tool turns),
  truncated to 200 runes with a `…` suffix; `truncated` is `false` when
  nothing was cut. Omitted when there is no such turn.
- `nextCursor` is `null` on the last page. IDs are strings.

### `GET /conversations/{id}`

One conversation with its full transcript.

**Response `200`**

```json
{
  "id": "12",
  "entries": [
    {"id": "10", "kind": "user", "message": {"role": "user", "content": "hi"}},
    {"id": "11", "kind": "assistant", "message": {"role": "assistant", "content": "hello"}}
  ]
}
```

`message` uses the `finalMessage` shape (`role`, `content`, `tool_calls`,
`tool_call_id`, `name`).

**Errors**

| Status | When |
|---|---|
| `404` | Unknown conversation, or the internal root conversation (`1`). |
| `400` | `{id}` is not a numeric ID. |

### `POST /conversations/{id}/runs`

Append turns to an existing conversation and stream the result as SSE. The
request body, validation, and SSE events are identical to `POST /runs`
(including `requestId` exactly-once); the given `messages` are appended to the
conversation's transcript in order.

**Errors**

| Status | When |
|---|---|
| `404` | Unknown conversation, or the internal root conversation (`1`). |
| `400` | Malformed `{id}`, or the same body errors as `POST /runs`. |

## SSE events

Events arrive in this order: a leading `run_started`, then zero or more
`text_delta`, then optionally `tool_call_started`/`tool_result` pairs (which
may repeat across iterations), terminated by exactly one of `run_finished` or
`error`.

| Event | Data | Meaning |
|---|---|---|
| `run_started` | `{"conversationId":"...","submissionId":"...","resumed":false}` | Always first. `resumed` is `true` when `requestId` matched an existing run. |
| `text_delta` | `{"text":"..."}` | A chunk of streamed assistant text. Concatenate `text` in order. |
| `tool_call_started` | `{"id":"...","name":"...","arguments":{...}}` | The model requested a tool call. `arguments` is the tool's JSON object. |
| `tool_result` | `{"id":"...","name":"...","result":"...","error":"..."}` | Outcome of a tool call. On success `error` is omitted and `result` holds output; on failure `error` is set. |
| `run_finished` | `{"finalMessage":{...},"conversationId":"..."}` | Run completed. `finalMessage` is the final assistant message (see below). |
| `error` | `{"error":"..."}` | Run aborted. Terminal; the stream closes after it. |

### `finalMessage` shape

```json
{
  "role": "assistant",
  "content": "Here are the files:",
  "tool_calls": [],
  "tool_call_id": "",
  "name": ""
}
```

`content`, `tool_calls`, `tool_call_id`, and `name` are omitted when empty.
For a completed run `role` is always `"assistant"` and `tool_calls` is empty.

## Errors

| Status | When | Body |
|---|---|---|
| `400` | Invalid JSON body, empty `messages`, bad `role`, empty `content`, malformed/negative `toolTimeout`, malformed conversation `{id}`, or bad list query params (`limit`/`order`/`cursor`) | `{"error":"..."}` |
| `404` | Unknown path, unknown conversation, or the internal root conversation (`1`) | `{"error":"..."}` (for conversation lookups) |
| `405` | Wrong HTTP method | (Go `ServeMux` response with an `Allow` header) |

Runtime failures (provider unreachable, max iterations exceeded, or a tool
error that aborts the run) do **not** use an HTTP error status — they are
reported as a final SSE `error` event on the `200` stream.

A client disconnect (e.g. the Stop button aborts the fetch) aborts the run's
execution: any in-flight tool call is cancelled and recorded in the transcript
as a cancelled tool result, and the run settles with an `error` event. The
transcript stays consistent, so the conversation can still be resumed.

## Full example transcript

```
event: run_started
data: {"conversationId":"12","submissionId":"14","resumed":false}

event: text_delta
data: {"text":"I'll list the files."}

event: tool_call_started
data: {"id":"call_1","name":"ls","arguments":{"path":"."}}

event: tool_result
data: {"id":"call_1","name":"ls","result":"main.go\ngo.mod\n"}

event: text_delta
data: {"text":"Here's the listing: main.go, go.mod."}

event: run_finished
data: {"finalMessage":{"role":"assistant","content":"Here's the listing: main.go, go.mod."},"conversationId":"12"}

```

## Web client integration

The run endpoint is `POST`-only, so the browser `EventSource` API cannot be
used directly. Use `fetch` with a streaming reader:

```js
// Returns an async generator of {event, data} objects.
async function* run(messages, { systemPrompt = "", maxIterations = 0, toolTimeout = "" } = {}) {
  const res = await fetch("http://127.0.0.1:8080/runs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ systemPrompt, messages, maxIterations, toolTimeout }),
  });

  if (!res.ok || !res.body) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || `HTTP ${res.status}`);
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    let sep;
    while ((sep = buffer.indexOf("\n\n")) !== -1) {
      const frame = buffer.slice(0, sep);
      buffer = buffer.slice(sep + 2);
      yield parseFrame(frame);
    }
  }
}

function parseFrame(frame) {
  let event = "message";
  let data = "";
  for (const line of frame.split("\n")) {
    if (line.startsWith("event: ")) event = line.slice(7);
    else if (line.startsWith("data: ")) data += line.slice(6);
  }
  try {
    return { event, data: JSON.parse(data) };
  } catch {
    return { event, data };
  }
}

// Usage
for await (const { event, data } of run([{ role: "user", content: "hi" }])) {
  if (event === "text_delta") console.log(data.text);
  else if (event === "run_finished") console.log("done:", data.finalMessage.content);
  else if (event === "error") console.error(data.error);
}
```

### CORS

Cross-origin browser calls are supported when the server is configured with a
CORS policy (see the server README's `cors` config section). When enabled:

- Every response — including the streaming `200` and non-2xx errors — carries
  `Access-Control-Allow-Origin`.
- `OPTIONS` preflight requests are answered with `204` plus
  `Access-Control-Allow-Methods`, `Access-Control-Allow-Headers`, and
  `Access-Control-Max-Age`. The `/runs` endpoints are `POST` with a JSON body,
  so browsers send a preflight first; it is answered automatically.

With a specific origin list, requests whose `Origin` is missing or not in the
list are rejected with `403` before any handler runs.

Without a CORS policy the server sends no CORS headers and does not handle
`OPTIONS` (preflights fall through to `405`), so cross-origin browsers are
blocked exactly as before.
