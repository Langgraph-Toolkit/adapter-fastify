/**
 * @langgraph-toolkit/adapter-fastify
 *
 * Fastify plugin: retains migration run/stream routes and provides a canonical
 * per-resource lifecycle plugin through createFastifyAdapter().
 */
import { randomUUID } from "node:crypto";
import type { FastifyInstance, FastifyPluginCallback } from "fastify";
import { GraphRuntimeError } from "@langgraph-toolkit/core";
import type { CompiledGraph, GraphDefinition, JsonObject, JsonValue, StepEvent } from "@langgraph-toolkit/core";
import { GraphRegistry } from "@langgraph-toolkit/core/runtime";
import { createGraphLifecycle, ToolkitRuntime, type GraphLifecycle } from "@langgraph-toolkit/core/runtime";

/** Options for langgraphFastify plugin; defaults map to /agents/:name. */
export interface LangGraphFastifyOptions {
  /** Runtime facade holding the graphs this plugin exposes. */
  readonly runtime?: ToolkitRuntime;
  /** Backward-compatible registry option. */
  readonly graphs?: GraphRegistry;
  /** Run endpoint, e.g. "/agents/:name/run" (POST JSON). */
  readonly runPath?: string;
  /** Stream endpoint, e.g. "/agents/:name/stream" (GET SSE). */
  readonly streamPath?: string;
  /** Require x-api-key header (string equality or validator). */
  readonly apiKey?: string | ((key: string) => boolean);
}

/** Zero-config options for createFastifyAdapter(). */
export interface FastifyAdapterOptions extends Omit<LangGraphFastifyOptions, "graphs" | "runtime"> {}

/** Fastify resource returned by createFastifyAdapter(). */
export interface FastifyAdapter<TGraph extends object = object> {
  readonly graph: TGraph;
  readonly runtime: GraphRegistry;
  /** Canonical graph lifecycle backing the mounted HTTP routes. */
  readonly lifecycle: GraphLifecycle;
  readonly plugin: FastifyPluginCallback<LangGraphFastifyOptions>;
}

declare module "fastify" {
  interface FastifyInstance {
    langgraph: GraphRegistry;
  }
}

function encodeSse(type: string, data: object | JsonValue): string {
  return `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
}

function validateApiKey(request: { headers: Record<string, string | string[] | undefined> }, apiKey?: LangGraphFastifyOptions["apiKey"]): boolean {
  if (!apiKey) return true;
  const header = request.headers["x-api-key"];
  const key = Array.isArray(header) ? String(header[0]) : String(header ?? "");
  if (typeof apiKey === "string") return key === apiKey;
  return apiKey(key);
}

function parseJsonObject(value: string): JsonObject {
  try {
    const parsed = JSON.parse(value) as JsonValue;
    return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed) ? parsed as JsonObject : {};
  } catch {
    return {};
  }
}

function resolveGraphs(options: LangGraphFastifyOptions): GraphRegistry {
  const graphs = options.runtime ?? options.graphs;
  if (graphs === undefined) throw new GraphRuntimeError("langgraphFastify requires runtime or graphs.");
  return graphs;
}

/** Fastify 4 requires the decorator to be attached before plugin registration. */
export function decorateLangGraph(fastify: FastifyInstance, registry: GraphRegistry): void {
  if (!fastify.hasDecorator("langgraph")) fastify.decorate("langgraph", registry);
}

export const langgraphFastify: FastifyPluginCallback<LangGraphFastifyOptions> = (
  app: FastifyInstance,
  options,
  done,
) => {
  const graphs = resolveGraphs(options);
  decorateLangGraph(app, graphs);
  done();

  const runPath = options.runPath ?? "/agents/:name/run";
  const streamPath = options.streamPath ?? "/agents/:name/stream";
  const collectionPath = runPath.endsWith("/:name/run") ? runPath.slice(0, -"/:name/run".length) : "/agents";

  app.get(collectionPath, async (_request, reply) => {
    reply.send(app.langgraph.list());
  });

  app.post(runPath, async (request, reply) => {
    if (!validateApiKey(request, options.apiKey)) {
      reply.code(401).send({ error: "Unauthorized" });
      return;
    }
    const params = request.params as JsonObject;
    const name = typeof params.name === "string" ? params.name : "";
    if (!app.langgraph.has(name)) {
      reply.code(404).send({ error: `Graph "${name}" not registered` });
      return;
    }
    const controller = new AbortController();
    request.raw.on("aborted", () => controller.abort());
    try {
      const body = (request.body ?? {}) as JsonObject;
      const result = await app.langgraph.run(name, body, {
        threadId: typeof body.threadId === "string" ? body.threadId : undefined,
        signal: controller.signal,
      });
      reply.send(result);
    } catch (err) {
      reply.code(500).send({ error: err instanceof Error ? err.message : "Graph execution failed" });
    }
  });

  app.get(streamPath, async (request, reply) => {
    if (!validateApiKey(request, options.apiKey)) {
      reply.code(401).send({ error: "Unauthorized" });
      return;
    }
    const params = request.params as JsonObject;
    const name = typeof params.name === "string" ? params.name : "";
    if (!app.langgraph.has(name)) {
      reply.code(404).send({ error: `Graph "${name}" not registered` });
      return;
    }
    reply.code(200).header("Content-Type", "text/event-stream").header("Cache-Control", "no-cache").header("Connection", "keep-alive");
    const raw = reply.raw;
    raw.setHeader("Content-Type", "text/event-stream");
    raw.setHeader("Cache-Control", "no-cache");
    raw.setHeader("Connection", "keep-alive");
    const controller = new AbortController();
    request.raw.on("aborted", () => controller.abort());
    try {
      const query = request.query as JsonObject;
      const events = app.langgraph.stream(name, typeof query.input === "string" ? parseJsonObject(query.input) : {}, {
        threadId: typeof query.threadId === "string" ? query.threadId : undefined,
        signal: controller.signal,
      });
      for await (const event of events) {
        raw.write(encodeSse(event.type, event));
        if (event.type === "error" || event.type === "cancelled") break;
      }
    } catch (err) {
      raw.write(encodeSse("error", { message: err instanceof Error ? err.message : "Graph stream failed" }));
    } finally {
      raw.end();
    }
  });
};

export function encodeStepEvent(event: StepEvent): string {
  return encodeSse(event.type, event);
}

/** Options internal to one canonical lifecycle resource plugin. */
interface FastifyLifecycleOptions {
  readonly lifecycle: GraphLifecycle;
  readonly name: string;
  readonly apiKey?: LangGraphFastifyOptions["apiKey"];
}

/**
 * Register canonical lifecycle routes relative to the host mount point.
 *
 * Routes match every framework adapter: POST /invoke, /stream, /resume,
 * /cancel, /replay, /fork and GET /state, /history.
 */
export const lifecycleFastify: FastifyPluginCallback<FastifyLifecycleOptions> = (app, options, done) => {
  const authorized = (request: { headers: Record<string, string | string[] | undefined> }, reply: { code: (statusCode: number) => { send: (payload: JsonObject) => void } }): boolean => {
    if (validateApiKey(request, options.apiKey)) return true;
    reply.code(401).send({ error: "Unauthorized" });
    return false;
  };
  const fail = (reply: { code: (statusCode: number) => { send: (payload: JsonObject) => void } }, error: unknown): void => {
    reply.code(error instanceof GraphRuntimeError ? 409 : 500).send({ error: error instanceof Error ? error.message : "Graph lifecycle failed" });
  };

  app.post("/invoke", async (request, reply) => {
    if (!authorized(request, reply)) return;
    try {
      reply.send(await options.lifecycle.invoke(options.name, lifecycleRequest(request.body as JsonValue)));
    } catch (error) {
      fail(reply, error);
    }
  });
  app.post("/stream", async (request, reply) => {
    if (!authorized(request, reply)) return;
    const value = lifecycleRequest(request.body as JsonValue);
    const threadId = value.threadId ?? randomUUID();
    reply.hijack();
    const raw = reply.raw;
    raw.setHeader("Content-Type", "text/event-stream");
    raw.setHeader("Cache-Control", "no-cache");
    raw.setHeader("Connection", "keep-alive");
    const cancelOnClose = (): void => { options.lifecycle.cancel(options.name, threadId); };
    request.raw.once("aborted", cancelOnClose);
    raw.once("close", cancelOnClose);
    try {
      for await (const event of options.lifecycle.stream(options.name, { ...value, threadId })) {
        raw.write(encodeSse(event.type, event));
      }
    } catch (error) {
      raw.write(encodeSse("error", { message: error instanceof Error ? error.message : "Graph stream failed" }));
    } finally {
      raw.end();
    }
  });
  app.post("/resume", async (request, reply) => {
    if (!authorized(request, reply)) return;
    try {
      const body = bodyObject(request.body as JsonValue);
      const response = "response" in body ? body.response : body.answer;
      if (response === undefined) throw new GraphRuntimeError("resume requires a JSON response or answer field.");
      reply.send(await options.lifecycle.resume(options.name, { threadId: requiredString(body, "threadId"), response }));
    } catch (error) {
      fail(reply, error);
    }
  });
  app.post("/cancel", async (request, reply) => {
    if (!authorized(request, reply)) return;
    try {
      const threadId = requiredString(bodyObject(request.body as JsonValue), "threadId");
      reply.send({ cancelled: options.lifecycle.cancel(options.name, threadId), threadId });
    } catch (error) {
      fail(reply, error);
    }
  });
  app.get("/state", async (request, reply) => {
    if (!authorized(request, reply)) return;
    try {
      reply.send(await options.lifecycle.state(options.name, queryString(request.query as JsonObject, "threadId")));
    } catch (error) {
      fail(reply, error);
    }
  });
  app.get("/history", async (request, reply) => {
    if (!authorized(request, reply)) return;
    try {
      reply.send(await options.lifecycle.history(options.name, queryString(request.query as JsonObject, "threadId")));
    } catch (error) {
      fail(reply, error);
    }
  });
  app.post("/replay", async (request, reply) => {
    if (!authorized(request, reply)) return;
    try {
      const body = bodyObject(request.body as JsonValue);
      reply.send(await options.lifecycle.replay(options.name, {
        ...lifecycleRequest(body),
        threadId: requiredString(body, "threadId"),
        checkpointId: requiredString(body, "checkpointId"),
      }));
    } catch (error) {
      fail(reply, error);
    }
  });
  app.post("/fork", async (request, reply) => {
    if (!authorized(request, reply)) return;
    try {
      const body = bodyObject(request.body as JsonValue);
      reply.send(await options.lifecycle.fork(options.name, {
        threadId: requiredString(body, "threadId"),
        checkpointId: requiredString(body, "checkpointId"),
        targetThreadId: requiredString(body, "targetThreadId"),
      }));
    } catch (error) {
      fail(reply, error);
    }
  });
  done();
};

/** Create a Fastify plugin bound to one graph resource or registry. */
export function createFastifyAdapter<TGraph extends object>(graph: TGraph, options: FastifyAdapterOptions = {}): FastifyAdapter<TGraph> {
  const runtime = normalizeGraph(graph);
  const name = resolveGraphName(graph, runtime);
  const lifecycle = createGraphLifecycle(runtime);
  return {
    graph,
    runtime,
    lifecycle,
    plugin: (app, _pluginOptions, done) => lifecycleFastify(app, { lifecycle, name, apiKey: options.apiKey }, done),
  };
}

function normalizeGraph<TGraph extends object>(graph: TGraph): GraphRegistry {
  if (graph instanceof ToolkitRuntime) return graph;
  const runtime = new GraphRegistry();
  const source = graph as object;
  const collection = source as { readonly list?: () => string[]; readonly get?: (name: string) => CompiledGraph<object> | undefined };
  if (typeof collection.list === "function" && typeof collection.get === "function") {
    for (const name of collection.list()) {
      const compiled = collection.get(name);
      if (compiled && !runtime.has(compiled.name)) runtime.add(compiled);
    }
    return runtime;
  }
  const executable = source as { readonly name?: string; readonly definition?: GraphDefinition<object>; readonly run?: (input: object) => Promise<object>; readonly stream?: (input: object) => AsyncIterable<object> };
  if (typeof executable.name === "string" && executable.definition !== undefined && typeof executable.run === "function" && typeof executable.stream === "function") {
    runtime.add(graph as CompiledGraph<object>);
    return runtime;
  }
  const builder = source as { readonly build?: () => CompiledGraph<object> };
  if (typeof builder.build === "function") {
    runtime.add(builder.build());
    return runtime;
  }
  throw new GraphRuntimeError("createFastifyAdapter requires a compiled graph, graph builder, runtime, or registry.");
}

function resolveGraphName<TGraph extends object>(graph: TGraph, runtime: GraphRegistry): string {
  const named = graph as { readonly name?: string };
  if (typeof named.name === "string" && runtime.has(named.name)) return named.name;
  const names = runtime.list();
  if (names.length === 1) return names[0] as string;
  throw new GraphRuntimeError("createFastifyAdapter requires one compiled graph. Use langgraphFastify when mounting a graph collection.");
}

function bodyObject(value: JsonValue): JsonObject {
  if (typeof value === "object" && value !== null && !Array.isArray(value)) return value as JsonObject;
  return {};
}

function lifecycleRequest(value: JsonValue): { readonly input: JsonObject; readonly threadId?: string } {
  const body = bodyObject(value);
  const input = "input" in body ? bodyObject(body.input) : withoutLifecycleFields(body);
  return typeof body.threadId === "string" ? { input, threadId: body.threadId } : { input };
}

function withoutLifecycleFields(body: JsonObject): JsonObject {
  const result: Record<string, JsonValue> = {};
  for (const [key, value] of Object.entries(body)) {
    if (!["threadId", "checkpointId", "targetThreadId", "response", "answer"].includes(key)) result[key] = value;
  }
  return result;
}

function requiredString(body: JsonObject, key: string): string {
  const value = body[key];
  if (typeof value !== "string" || value.length === 0) throw new GraphRuntimeError(`${key} is required.`);
  return value;
}

function queryString(query: JsonObject, key: string): string {
  const value = query[key];
  if (typeof value !== "string" || value.length === 0) throw new GraphRuntimeError(`${key} query parameter is required.`);
  return value;
}

export default langgraphFastify;
