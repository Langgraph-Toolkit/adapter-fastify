/**
 * @langgraph-toolkit/adapter-fastify
 *
 * Fastify plugin: registers graph run/stream routes, adds an app.langgraph
 * decorator exposing the registry, and uses reply.raw for native SSE.
 */
import type { FastifyInstance, FastifyPluginCallback } from "fastify";
import { GraphRuntimeError } from "@langgraph-toolkit/core";
import type { GraphRegistry, JsonObject, JsonValue, StepEvent, ToolkitRuntime } from "@langgraph-toolkit/core";

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

export default langgraphFastify;
