# @langgraph-toolkit/adapter-fastify

**Use Fastify for the host, not for graph configuration.** This plugin provides graph listing, JSON execution, and SSE streaming while the application resource owns state, MCP, checkpoint, actor, policy, and provider defaults.

## Install

```bash
npm install fastify @langgraph-toolkit/core @langgraph-toolkit/adapter-fastify
```

## Minimal host wiring

```ts
import Fastify from "fastify";
import langgraphFastify from "@langgraph-toolkit/adapter-fastify";
import { runtime } from "./database-chat/resource.js";

const app = Fastify();
await app.register(langgraphFastify, { runtime });

await app.listen({
  port: Number(process.env.PORT ?? 3000),
  host: "0.0.0.0",
});
```

The plugin exposes `GET /agents`, `POST /agents/:name/run`, and `GET /agents/:name/stream`. `decorateLangGraph` can expose the registry as `fastify.langgraph` when another plugin needs direct access.

## The same resource, a different host

| Resource definition | Fastify host |
|---|---|
| Defines nodes, graph-level runtime, MCP, policy, and checkpoint | Registers one plugin |
| Emits typed step, tool, thinking, token, and interrupt events | Serializes the stream |
| Accepts `question` and optional `threadId` | Parses request and reply |

This keeps the host small and lets the resource run unchanged under Express, NestJS, StruxJS, a worker, or a CLI.

## Public API and development

The public entrypoints are `langgraphFastify`, `decorateLangGraph`, `encodeStepEvent`, `LangGraphFastifyOptions`, and `GraphRuntimeError`. The plugin accepts a registry or runtime and does not require per-request checkpoint or policy parameters.

```bash
npm install
npm run build
npm test
```

See `examples/projects/fastify` for the complete CLI-scaffolded database-chat project and contributor contract.

## License

MIT
