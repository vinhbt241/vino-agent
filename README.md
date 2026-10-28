# vino

A durable tool-using agent. This repo currently ships the agent kernel
(`internal/agent`), a real OpenAI-compatible LLM provider (`internal/llm`),
the concrete tool set (`internal/tools`), and an HTTP server (`cmd/server`)
that wires them together.

This guide covers spinning up the server with a proper config.

## Quick start

```bash
cp vino.example.json vino.json
export VINO_PROVIDER_OPENAI_API_KEY=sk-...   # or edit apiKey in vino.json
go run ./cmd/server --config vino.json
```

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

### Validation

The server refuses to start if:

- `default` is empty or names a provider that isn't in `providers`.
- any configured provider is missing `apiKey` (after env merge), `baseURL`,
  or `model`.

> Provider and model are fixed at startup. Switch providers by changing the
> `default` field (or the env overrides) and restarting — there is no
> per-request provider selection.

## API

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
  "toolTimeout": "30s"
}
```

- `messages[].role` must be `user` or `assistant`; tool turns are generated
  server-side.
- `maxIterations` `0` means the default cap (50).
- `toolTimeout` is a Go duration string; empty means no timeout.

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

## Development

```bash
go test ./...
go build ./cmd/server
```
