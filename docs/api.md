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

## SSE events

Events arrive in this order: zero or more `text_delta`, then optionally
`tool_call_started`/`tool_result` pairs (which may repeat across iterations),
terminated by exactly one of `run_finished` or `error`.

| Event | Data | Meaning |
|---|---|---|
| `text_delta` | `{"text":"..."}` | A chunk of streamed assistant text. Concatenate `text` in order. |
| `tool_call_started` | `{"id":"...","name":"...","arguments":{...}}` | The model requested a tool call. `arguments` is the tool's JSON object. |
| `tool_result` | `{"id":"...","name":"...","result":"...","error":"..."}` | Outcome of a tool call. On success `error` is omitted and `result` holds output; on failure `error` is set. |
| `run_finished` | `{"finalMessage":{...}}` | Run completed. `finalMessage` is the final assistant message (see below). |
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
| `400` | Invalid JSON body, empty `messages`, bad `role`, empty `content`, or malformed/negative `toolTimeout` | `{"error":"..."}` |
| `405` | Wrong HTTP method | (Go `ServeMux` response with an `Allow` header) |
| `404` | Unknown path | |

Runtime failures (provider unreachable, max iterations exceeded, tool error
that aborts the run, client disconnect) do **not** use an HTTP error status —
they are reported as a final SSE `error` event on the `200` stream.

## Full example transcript

```
event: text_delta
data: {"text":"I'll list the files."}

event: tool_call_started
data: {"id":"call_1","name":"ls","arguments":{"path":"."}}

event: tool_result
data: {"id":"call_1","name":"ls","result":"main.go\ngo.mod\n"}

event: text_delta
data: {"text":"Here's the listing: main.go, go.mod."}

event: run_finished
data: {"finalMessage":{"role":"assistant","content":"Here's the listing: main.go, go.mod."}}

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

### CORS note

The server does **not** send CORS headers and does not handle `OPTIONS`
preflight requests. A browser client calling this API cross-origin will be
blocked by CORS. Serve the web app from the same origin (via a reverse proxy),
or add CORS handling to the server before pointing a cross-origin browser app
at it.
