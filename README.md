# @langgraph/toolkit

Framework-agnostic LangGraph toolkit. Define a graph once, run it on **StruxJS, Express, Fastify, or NestJS** with zero changes to the graph code. The architecture follows the same split as Redux: a pure TypeScript core plus thin host bindings.

| Layer | Package | Purpose |
|---|---|---|
| Core | `packages/core` | `defineGraph()` DSL, `compile()` rule enforcement, `run()` / `stream()`, permission + cost control, `testEdgeRisk`, e2e harness |
| StruxJS | `packages/adapter-struxjs` | ServiceProvider, agent scanner, Strux checkpointer, console commands |
| Express | `packages/adapter-express` | SSE middleware + router (`/run`, `/stream`) |
| Fastify | `packages/adapter-fastify` | Plugin + `decorateLangGraph()` |
| NestJS | `packages/adapter-nestjs` | DynamicModule + Injectable `LangGraphService` |
| Checkpointers | `packages/adapter-checkpointers` | `SqlCheckpointer` (SQLite/Postgres/MySQL), `RedisCheckpointer`, `MongoCheckpointer` via driver injection |

## Quick start

```ts
import {
  defineGraph, node, edge, conditional, safety,
  attachExecutor, compile, messagesValue, reducedValue,
  GraphRegistry, MemoryCheckpointer,
} from "@langgraph/toolkit";

const def = defineGraph<{ messages: unknown[]; plan: string[]; confirmed: boolean }>({
  name: "database-chat",
  state: {
    messages: messagesValue(),
    plan: reducedValue<string[]>([], (prev, next) => [...prev, ...next]),
    confirmed: false as never,
  } as never,
  stateDefaults: { confirmed: false as never },
  nodes: {
    planner: node(async (state) => ({ plan: makePlan(state) })),
    confirmGate: node(async (state) => ({ confirmed: true })),
    executor: node(async (state) => ({ messages: [{ role: "assistant", content: "done" }] })),
  },
  entry: "planner",
  edges: [
    edge("planner", "confirmGate"),
    conditional("confirmGate", (s) => (s.confirmed ? "executor" : "END"), ["executor", "END"]),
  ],
  safety: safety(25),
  interruptBefore: ["confirmGate"],   // human approval gate
});

const registry = new GraphRegistry();
registry.register(def); // compile + attach executor + register

const result = await registry.run("database-chat", { messages: [{ role: "user", content: "..." }] }, {
  threadId: "thread-1",
  actor: e2eActor("alice", ["admin"]),
  checkpoint: new SqlCheckpointer(makeSyncSqlDriver(db)),
  policy: rolePolicy({ "database-chat": ["admin", "operator"] }),
  tierResolver: planTierResolver({ free: { strong: "cheap" }, pro: { strong: "strong" } }),
});
// result.stoppedReason === "interrupt"  ->  ask the human, then resume
await registry.run("database-chat", { messages: [] }, {
  threadId: "thread-1",
  checkpoint: cp,
  resumeFrom: "confirmGate",
  humanResponse: true,
});
```

## Graph Engineer rules enforced

| Rule | Mechanism |
|---|---|
| N1 Nodes identify themselves | Every graph has `name`; every node is a named key in `nodes` |
| N2 Node reads declared state only | `compile()` validates nodes against declared state; nodes return field deltas merged by reducers |
| N3 Node failure is explicit | `run()` returns `stoppedReason: "error"` with the error attached, never silent |
| E1 Edges are first-class | `edge()` / `conditional()` objects, not magic strings in node bodies |
| E2 Routing is deterministic | Conditional routes declare allowed targets; unknown branches are runtime errors |
| E3 Output is verifiable | `verify` + `codeAnchor()` / `testAnchor()` gates; panel must have a non-LLM anchor |
| L1 Loops must converge | Cycles without `converge` are rejected at compile time; dry-loop convergence at runtime |
| L2 Runaways are bounded | `safety(recursionLimit)` enforced on every round; `cancelled()` available in node context |
| T3 Model access via tier alias | Nodes declare `tier`; `ToolkitModelRegistry` resolves driver (OpenAI, HuggingFace, mock) without vendor imports in graph code |
| P5 Interruptions checkpoint | `interruptBefore` persists state before pausing; `resumeFrom` + `humanResponse` continue |
| A1 Policy at run start | `RunPolicy` returns `allow | deny | interrupt`; `deny` throws `PermissionDeniedError` | `actor`, `policy: rolePolicy(...)`, `combinePolicies(...)` |
| A2 Tier downgrade at runtime | `TierResolver` can lower a node's tier; unbound tiers are rejected | `tierResolver`, `planTierResolver(...)` |
| A3 Token budget per actor-tier | `withTokenBudget(provider, budget, actor)` charges per tier and throws `TokenBudgetExceededError` | `tokenBudget`, `resetTokenLedger()` |
| A4 Dangerous nodes auto-interrupt | `node(fn, { risk: "dangerous" })` interrupts unless in `interruptBefore` | `risk("dangerous")`, `ctx.actor` |

## Host bindings

Each adapter is under 400 lines. The graph code never imports the host.

### StruxJS (primary host)

```ts
import { LangGraphServiceProvider } from "@langgraph/adapter-struxjs";
// app/Agents/<workflow>/index.js is auto-scanned at boot, mirroring the StruxJS model.
app.registerProviders([LangGraphServiceProvider]);
```

### Express

```ts
import { langgraphRouter, sseMiddleware } from "@langgraph/adapter-express";
app.use(express.json());
app.use(sseMiddleware);
app.use(langgraphRouter({ graphs: registry, path: "/agents/:name" }));
// GET  /agents/:name/stream  -> text/event-stream
// POST /agents/:name/run     -> JSON run result
```

### Fastify

```ts
import { langgraphFastify, decorateLangGraph } from "@langgraph/adapter-fastify";
decorateLangGraph(fastify, registry); // call BEFORE register
await fastify.register(langgraphFastify, { path: "/agents/:name" });
```

### NestJS

```ts
import { LangGraphModule, LangGraphService } from "@langgraph/adapter-nestjs";
// register the module with your registry once; inject LangGraphService anywhere
```

## Checkpointers on any database

Checkpointers live in `@langgraph/adapter-checkpointers` and take an injected driver, so the package carries zero database dependencies at build time (Redux Toolkit style): use better-sqlite3, pg, or mysql2 for SQL; ioredis for Redis; the mongodb driver for MongoDB.

```ts
import { SqlCheckpointer, RedisCheckpointer, MongoCheckpointer, makeSyncSqlDriver } from "@langgraph/adapter-checkpointers";
import Database from "better-sqlite3";

// SQLite (or Postgres/MySQL: pass the dialect)
const sql = new SqlCheckpointer(makeSyncSqlDriver(new Database("./cp.sqlite")));
// Redis: driver { get, set, del, lpush, lrange }
const redis = new RedisCheckpointer(myRedisDriver, { prefix: "lg:", ttlSeconds: 3600 });
// MongoDB: driver { findOne, updateOne, find }
const mongo = new MongoCheckpointer(myMongoDriver);

await registry.run("database-chat", input, { threadId: "t1", checkpoint: sql });
```

## Permissions, tiers, and cost control

Every run can carry an `Actor`, a `RunPolicy`, a `TierResolver`, and a `TokenBudget`. The executor enforces them in order: policy at run start (A1), tier resolution before model lookup (A2), token charging per actor-tier (A3), and automatic interrupt before any `risk("dangerous")` node (A4).

```ts
import {
  rolePolicy, combinePolicies, planTierResolver,
  withTokenBudget, testEdgeRisk, e2eActor,
  PermissionDeniedError, TokenBudgetExceededError,
} from "@langgraph/toolkit";

const policy = rolePolicy({ "database-chat": ["admin", "operator"] });
const tiers = planTierResolver({
  free: { strong: "cheap" },   // free users get downgraded transparently
  pro:  { strong: "strong" },
});
const chat = withTokenBudget(myProvider, {
  perTier: { __default__: { limit: 100_000, windowMs: 3_600_000 } },
}, actor);
```

Probe the whole permission surface before shipping with the built-in risk harness. It replays each actor against the graph and reports violations like `policy_deny_bypassed`, `tier_escalation`, `dangerous_node_uninterrupted`, `budget_not_enforced`, and `rogue_field_leak`:

```ts
const probe = await testEdgeRisk(compiledGraph, {
  actors: [e2eActor("bob", ["guest"]), e2eActor("alice", ["admin"])],
  policy, tierResolver: tiers, tokenBudget: budget,
});
if (probe.violations.length > 0) throw new Error(JSON.stringify(probe.violations));
```

## E2E testing for contributors

The core ships an end-to-end harness (`packages/core/src/e2e.ts`) so contributors can test a graph against a real host: `e2eRun(url, req)`, `e2eStream(url, req)`, `expectDone(result)`, `expectInterrupted(result)`, `expectTerminal(stream)`, and `e2eScenarioResume(...)` for full interrupt-then-resume round trips. Reference harnesses are in `scripts/`:

- `e2e-http-test.ts` - boots Express, Fastify, and NestJS, hits `/run` on all three, asserts `ALL_E2E_PASS`
- `e2e-strux-test.mts` - StruxJS lifecycle: interrupt, checkpoint persist, resume, SSE, workflow scanning, queue dispatch
- `e2e-edge-test.mts` - 20 edge cases: safety, timeout, cancellation, anchors, verifiers, cycles, state merging

## Queue workers

Queue dispatch is a core capability, not a fifth host. Any queue (Strux Queue, BullMQ, SQS) plugs in via `QueueAdapter`:

```ts
import { registerQueueAdapter, dispatchToQueue } from "@langgraph/toolkit";
registerQueueAdapter("default", { enqueue: (job) => bullQueue.add("graph", job) });
await dispatchToQueue(registry, "database-chat", { messages: [...] });
```

## Development

```bash
npm install          # workspaces: all packages + examples
npm run build        # compile every package (tsc, zero errors required)
npm run test         # vitest: 57+ unit tests across core + adapter packages
bash scripts/run-examples.sh   # e2e: run the same graph on 4 hosts
npx tsx scripts/e2e-http-test.ts      # E2E HTTP on Express/Fastify/NestJS
npx tsx scripts/e2e-strux-test.mts    # E2E StruxJS lifecycle (10 scenarios)
npx tsx scripts/e2e-edge-test.mts     # E2E edge cases (20 scenarios)
```

Examples in `examples/` share one graph (`examples/shared/agent.ts`) to prove the core is identical across hosts. Contributors: read `CONTRIBUTING.md` for adapter patterns, the e2e harness, and the PR checklist.

## Design philosophy

1. **Core is pure TypeScript.** No Express, Fastify, Nest, or Strux imports inside `packages/core`.
2. **Fail fast at compile time.** `compile()` rejects unanchored loops, missing entries, ghost branches, and unknown state fields before anything runs.
3. **Reducers over mutable state.** `messagesValue()` and `reducedValue()` make merges explicit; nodes return deltas, never full snapshots.
4. **Interruptions first.** Human-in-the-loop is a first-class primitive (`interruptBefore` + checkpoint + resume), the backbone of safe admin/ops agents.
5. **Tier aliases over vendor imports.** Graph code says `tier: "strong"`, not `import OpenAI`. Swapping providers is a config change.
6. **Permission is a first-class run option.** Actors, policies, tiers, and token budgets are plain options on `run()`, and `testEdgeRisk` proves they hold before production.
7. **Any database, any queue, any model.** Driver injection keeps adapters dependency-free: SQLite, Postgres, MySQL, Redis, MongoDB; BullMQ, SQS, Strux Queue; OpenAI, HuggingFace, and any OpenAI-compatible endpoint.
