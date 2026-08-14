# @langgraph-toolkit/adapter-fastify

Fastify plugin for a compiled Langgraph-Toolkit registry. It provides graph listing, JSON execution, and SSE streaming while leaving graph composition and runtime defaults in the application resource.

## Install

```bash
npm install fastify @langgraph-toolkit/core @langgraph-toolkit/adapter-fastify
```

## Minimal host

```ts
import Fastify from "fastify";
import langgraphFastify from "@langgraph-toolkit/adapter-fastify";
import { runtime } from "./database-chat/resource.js";

const app = Fastify();
await app.register(langgraphFastify, {
  runtime,
});

await app.listen({
  port: Number(process.env.PORT ?? 3000),
  host: "0.0.0.0",
});
```

The plugin exposes `GET /agents`, `POST /agents/:name/run`, and `GET /agents/:name/stream`. `decorateLangGraph` can expose the registry as `fastify.langgraph` when application plugins need direct access.

## Public API

The package exports `langgraphFastify`, `decorateLangGraph`, `encodeStepEvent`, `LangGraphFastifyOptions`, and `GraphRuntimeError`. The plugin accepts a registry or runtime and does not require per-request checkpoint or policy parameters.

## Development

```bash
npm install
npm run build
npm test
```

See `examples/projects/fastify` for a complete CLI-scaffolded database-chat project.

The contributor contract is covered by `tests/fastify.test.ts`. Run it with `npm test` before opening a change.

## License

MIT
