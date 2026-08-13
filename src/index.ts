/**
 * @langgraph/adapter-fastify
 *
 * Fastify plugin: registers graph run/stream routes, adds an app.langgraph
 * decorator exposing the registry, and uses reply.raw for native SSE.
 *
 * Install: npm install fastify @langgraph/adapter-fastify
 * Peer: fastify
 */
import type { FastifyInstance, FastifyPluginCallback } from "fastify";
import type { GraphRegistry, JsonObject, JsonValue, StepEvent } from "@langgraph/toolkit";

/** Options for langgraphFastify plugin; defaults map to /agents/:name. */
export interface LangGraphFastifyOptions {
  /** Named registry holding the graphs this plugin exposes. */
  graphs: GraphRegistry;
  /** Run endpoint, e.g. "/agents/:name/run" (POST JSON). */
  runPath?: string;
  /** Stream endpoint, e.g. "/agents/:name/stream" (GET SSE). */
  streamPath?: string;
  /** Require x-api-key header (string equality or validator). */
  apiKey?: string | ((key: string) => boolean);
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

/**
 * Fastify 4 wraps the root instance when registering plugins, so the
 * decorator must be attached to the root BEFORE register().
 */
export function decorateLangGraph(fastify: FastifyInstance, registry: GraphRegistry): void {
  if (!fastify.hasDecorator("langgraph")) {
    fastify.decorate("langgraph", registry);
  }
}

export const langgraphFastify: FastifyPluginCallback<LangGraphFastifyOptions> = (
  app: FastifyInstance,
  options,
  done,
) => {
  done();

  const runPath = options.runPath ?? "/agents/:name/run";
  const streamPath = options.streamPath ?? "/agents/:name/stream";

  app.post(runPath, async (request, reply) => {
    if (!validateApiKey(request, options.apiKey)) {
      reply.code(401).send({ error: "Unauthorized" });
      return;
    }
    const params = request.params as JsonObject;
    const name = typeof params.name === "string" ? params.name : "";
    const compiled = app.langgraph.get(name);
    if (!compiled) {
      reply.code(404).send({ error: `Graph "${name}" not registered` });
      return;
    }
    const controller = new AbortController();
    request.raw.on("aborted", () => controller.abort());
    try {
      const body = (request.body ?? {}) as JsonObject;
      const result = await compiled.run(body, {
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
    const compiled = app.langgraph.get(name);
    if (!compiled) {
      reply.code(404).send({ error: `Graph "${name}" not registered` });
      return;
    }
    reply
      .code(200)
      .header("Content-Type", "text/event-stream")
      .header("Cache-Control", "no-cache")
      .header("Connection", "keep-alive");
    const raw = reply.raw;
    const controller = new AbortController();
    request.raw.on("aborted", () => controller.abort());
    try {
      const query = request.query as JsonObject;
      const events = compiled.stream(
        typeof query.input === "string" ? parseJsonObject(query.input) : {},
        {
          threadId: typeof query.threadId === "string" ? query.threadId : undefined,
          signal: controller.signal,
        },
      );
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
