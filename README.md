# vino

A durable tool-using agent. This repo currently ships the agent kernel
(`internal/agent`), a real OpenAI-compatible LLM provider (`internal/llm`),
the concrete tool set (`internal/tools`), and an HTTP server (`cmd/server`)
that wires them together.

This guide covers spinning up the server with a proper config.

## Quick start

```bash
cp vino.example.json vino.json
cp .env.example .env
# edit .env (or vino.json, or export VINO_PROVIDER_OPENAI_API_KEY=sk-...)
go run ./cmd/server --config vino.json
```

If a `.env` file is present it is loaded automatically (override its path with
`--env`). See [Environment overrides](#environment-overrides).

You should see:

```
durable-agent server listening on 127.0.0.1:8080 (provider "openai", model "gpt-4o")
```

Verify it's up:

```bash
curl -s http://127.0.0.1:8080/health
# {"ok":true}
```

## Config file

The server reads one JSON config file. The path is the `--config` flag,
defaulting to `./vino.json`.

```json
{
  "addr": "127.0.0.1:8080",
  "default": "openai",
  "providers": {
    "openai": {
      "apiKey": "",
      "baseURL": "https://api.openai.com/v1",
      "model": "gpt-4o"
    }
  },
  "tools": {
    "rootDir": "",
    "workDir": ""
  }
}
```

| Field | Meaning | Default |
|---|---|---|
| `addr` | listen address | `127.0.0.1:8080` |
| `default` | provider name used for every run | *(required)* |
| `providers.<name>.apiKey` | provider auth token | *(required, or via env)* |
| `providers.<name>.baseURL` | OpenAI-compatible base URL | *(required)* |
| `providers.<name>.model` | model sent to the provider | *(required)* |
| `tools.rootDir` | jail root for path-taking tools | process cwd |
| `tools.workDir` | working dir for shell tools | `tools.rootDir` |
| `durablePath` | JSONL log the durable harness writes | `vino.jsonl` |
| `cors.origins` | allowed origins (`"*"` = any); empty → CORS off | empty (off) |
| `cors.allowCredentials` | emit `Access-Control-Allow-Credentials` | `false` |
| `cors.maxAge` | `Access-Control-Max-Age` in seconds | `600` |

### CORS

CORS is **opt-in**. Without a `cors.origins` list the server sends no CORS
headers and does not handle `OPTIONS` preflights, so cross-origin browser
clients are blocked as before.

```json
{
  "cors": {
    "origins": ["*"],
    "allowCredentials": false,
    "maxAge": 600
  }
}
```

- `origins` accepts `"*"` (any origin) or a list of exact origins such as
  `["http://localhost:5173"]`. A specific list matches the request's `Origin`
  and rejects any other origin (or a missing `Origin`) with `403` before the
  request is handled.
- `allowCredentials` emits `Access-Control-Allow-Credentials: true`; it cannot
  be combined with `"*"` (the server refuses to start).
- `maxAge` caches the preflight answer in the browser.

### Environment overrides

Secrets can stay out of the file. Any provider field can be overridden by an
environment variable named `VINO_PROVIDER_<NAME>_<FIELD>`, where `<NAME>` is
the provider name uppercased and `<FIELD>` is one of `API_KEY`, `BASE_URL`,
`MODEL`. An empty variable is ignored (the file value wins).

| Variable | Overrides |
|---|---|
| `VINO_PROVIDER_OPENAI_API_KEY` | `providers.openai.apiKey` |
| `VINO_PROVIDER_OPENAI_BASE_URL` | `providers.openai.baseURL` |
| `VINO_PROVIDER_OPENAI_MODEL` | `providers.openai.model` |

#### `.env` file

These variables can also come from a `.env` file (default path `.env`, override
with `--env`), so credentials don't have to be exported into every shell:

```bash
# .env
VINO_PROVIDER_OPENAI_API_KEY=sk-...
```

Rules:

- The `.env` file is optional — a missing file is not an error.
- The **real environment wins**: a variable already exported in the shell is
  never overwritten by `.env`.
- `.env` is loaded before the config file, so its values participate in
  provider resolution and validation exactly like exported variables.
- Supported syntax: full-line `#` comments, blank lines, `export ` prefix,
  single-/double-quoted values, and trailing ` #` inline comments.

### Validation

The server refuses to start if:

- `default` is empty or names a provider that isn't in `providers`.
- any configured provider is missing `apiKey` (after env merge), `baseURL`,
  or `model`.

> Provider and model are fixed at startup. Switch providers by changing the
> `default` field (or the env overrides) and restarting — there is no
> per-request provider selection.

## API

The full client-facing reference (request schemas, SSE event contracts, error
codes, and a JavaScript fetch example) lives in [`docs/api.md`](docs/api.md).
Both endpoints are served over HTTP, loopback-only by default.

### `GET /health`

Returns `200 {"ok":true}`.

### `POST /runs`

Runs the agent and streams results as SSE (`text/event-stream`).

Request body (all fields optional except `messages`):

```json
{
  "systemPrompt": "You are a helpful coding assistant.",
  "messages": [
    { "role": "user", "content": "list the files in the current directory" }
  ],
  "maxIterations": 0,
  "toolTimeout": "30s",
  "requestId": "job-42"
}
```

- `messages[].role` must be `user` or `assistant`; tool turns are generated
  server-side.
- `maxIterations` `0` means the default cap (50).
- `toolTimeout` is a Go duration string; empty means no timeout.
- `requestId` (optional) makes a run exactly-once: retrying a request that was
  cut off by a crash or dropped connection resumes the original run instead of
  starting a duplicate.

SSE events, in stream order:

| Event | Data |
|---|---|
| `text_delta` | `{"text":"..."}` |
| `tool_call_started` | `{"id","name","arguments"}` |
| `tool_result` | `{"id","name","result","error"}` |
| `run_finished` | `{"finalMessage":{...}}` |
| `error` | `{"error":"..."}` (final event on failure) |

Example:

```bash
curl -sN -X POST http://127.0.0.1:8080/runs \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"list the files in ."}]}'
```

## Safety

- The server binds to `127.0.0.1` by default and has **no auth** — it is for
  local development only.
- Runs have access to the full tool set, including `bash`, `read`, `write`,
  and `edit`, jailed under `tools.rootDir`. Do not bind it to a public
  interface.
- Enabling CORS with `"*"` while the server is loopback-bound and unauthenticated
  means **any website you visit can drive the local agent** from your browser
  (DNS rebinding). Prefer a specific `origins` list, and do not combine `"*"`
  with a publicly bound address.

## Development

```bash
go test ./...
go build ./cmd/server
```
