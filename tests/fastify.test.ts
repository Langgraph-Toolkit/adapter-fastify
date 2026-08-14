import { describe, expect, it } from "vitest";
import Fastify from "fastify";
import { GraphRegistry, defineGraph, defineState } from "@langgraph-toolkit/core";
import { langgraphFastify } from "../src/index.js";

function makeRegistry(): GraphRegistry {
  const registry = new GraphRegistry();
  registry.register(
    defineGraph({
      name: "ping",
      state: defineState({ done: false }),
      nodes: {
        finish: async () => ({ done: true }),
      },
    }),
  );
  return registry;
}

async function makeApp(apiKey?: string) {
  const app = Fastify();
  await app.register(langgraphFastify, { runtime: makeRegistry(), apiKey });
  await app.ready();
  return app;
}

describe("adapter-fastify", () => {
  it("lists and runs a registered graph", async () => {
    const app = await makeApp();
    const list = await app.inject({ method: "GET", url: "/agents" });
    expect(list.statusCode).toBe(200);
    expect(list.json()).toEqual(["ping"]);

    const run = await app.inject({ method: "POST", url: "/agents/ping/run", payload: {} });
    expect(run.statusCode).toBe(200);
    expect(run.json<{ state: { done: boolean } }>().state.done).toBe(true);
    await app.close();
  });

  it("emits typed SSE events and enforces an API key", async () => {
    const app = await makeApp("secret");
    const unauthorized = await app.inject({ method: "POST", url: "/agents/ping/run", payload: {} });
    expect(unauthorized.statusCode).toBe(401);

    const stream = await app.inject({ method: "GET", url: "/agents/ping/stream", headers: { "x-api-key": "secret" } });
    expect(stream.statusCode).toBe(200);
    expect(stream.body).toContain("event: node_start");
    expect(stream.body).toContain("event: node_end");
    await app.close();
  });
});
