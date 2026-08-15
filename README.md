# @langgraph-toolkit/adapter-fastify

**Use Fastify for the host, not for graph configuration.** This adapter provides graph listing, JSON execution, and SSE streaming while the application resource owns state, MCP, checkpoint, actor, policy, and provider defaults.

## Install

```bash
npm install fastify @langgraph-toolkit/core @langgraph-toolkit/adapter-fastify
```

## Zero-config factory

```ts
import Fastify from "fastify";
import { createFastifyAdapter } from "@langgraph-toolkit/adapter-fastify";
import { resource } from "./resource.js";

const adapter = createFastifyAdapter(resource.runtime);
const app = Fastify();

await app.register(adapter.plugin);
await app.listen({ port: Number(process.env.PORT ?? 3000), host: "0.0.0.0" });
```

The factory returns `{ graph, runtime, plugin }` and defaults to `/agents`, `/agents/:name/run`, and `/agents/:name/stream`. Pass `runPath`, `streamPath`, or `apiKey` only when the host requires a custom boundary.

## Host-native escape hatch

Register `langgraphFastify` directly when the application needs custom plugin composition. `decorateLangGraph` exposes the registry as `fastify.langgraph`; `encodeStepEvent` remains available for native streaming integrations.

The same resource can move to Express, NestJS, StruxJS, a worker, or a CLI without changing its nodes.

## Development

```bash
npm install
npm run build
npm test
```

## License

MIT
