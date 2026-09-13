// Actor module — registers every foreign actor the runtime hosts.
import { payload as etlPayload, result as etlResult, handler as etlHandler } from "./etl_small.ts";
import { payload as fanPayload, result as fanResult, handler as fanHandler } from "./fanout.ts";
import { payload as spinPayload, result as spinResult, handler as spinHandler } from "./spinny.ts";

export async function registerAll(defineActor: (def: any) => void) {
  defineActor({
    name: "etl_small",
    queue: "etl",
    payload: etlPayload,
    result: etlResult,
    retry: { max_attempts: 3 },
    // Serialized rate-limit config, passed through verbatim. The real
    // integration maps these to Python-side TokenBucket/SlidingWindow
    // objects at registration time (noted as friction in the README).
    rate_limits: [{ kind: "token_bucket", name: "etl_small_global", capacity: 10, refill_per_sec: 5 }],
    handler: etlHandler,
  });
  defineActor({
    name: "fanout",
    queue: "etl",
    payload: fanPayload,
    result: fanResult,
    retry: { max_attempts: 3 },
    handler: fanHandler,
  });
  defineActor({
    name: "spinny",
    queue: "cpu",
    payload: spinPayload,
    result: spinResult,
    retry: { max_attempts: 1 },
    handler: spinHandler,
  });
}
