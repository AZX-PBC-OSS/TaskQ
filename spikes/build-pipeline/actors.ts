/**
 * Actor authoring shape — what a TaskQ TS author writes in their editor.
 *
 * `defineActor` is the single registration primitive: it pairs a Zod-typed
 * handler with the metadata the Python worker needs (queue, retry, concurrency)
 * and freezes the payload/result JSON Schemas at definition time. The Zod
 * schema is the source of truth: everything downstream (manifest, pydantic
 * codegen, envelope validation) derives from it.
 */
import { z } from "zod";

/** Handled to every invocation. Populated by the worker over the NDJSON protocol. */
export interface ActorContext {
  /** 1-based attempt number for this job execution. */
  attempt: number;
  /** TaskQ job id (UUID). */
  jobId: string;
  /** Queue the job was received on (after enqueue-time override). */
  queue: string;
}

export interface RetryOptions {
  /** Total attempts including the first. Minimum 1. */
  maxAttempts: number;
  /** Base delay between attempts, seconds. TaskQ applies its backoff curve on top. */
  delay: number;
}

interface ActorDefinition<P, R> {
  /** Unique actor name. This is what Python's enqueue(actor="...") targets. */
  name: string;
  /** Home queue. Defaults to "default". */
  queue?: string;
  retry?: Partial<RetryOptions>;
  /** Max in-flight invocations of this actor in one runtime process. */
  maxConcurrent?: number;
  /** Zod schemas — the source of truth for the wire format and codegen. */
  schema: { payload: z.ZodType<P>; result: z.ZodType<R> };
  handler: (payload: P, ctx: ActorContext) => Promise<R> | R;
}

export interface Actor<P, R> {
  name: string;
  schema: { payload: z.ZodType<P>; result: z.ZodType<R> };
  handler: (payload: P, ctx: ActorContext) => Promise<R> | R;
  /** Everything the runtime manifest / codegen need. JSON-schema-computed once. */
  manifest: {
    name: string;
    queue: string;
    retry: RetryOptions;
    maxConcurrent: number;
    payloadJsonSchema: z.core.JSONSchema.BaseSchema;
    resultJsonSchema: z.core.JSONSchema.BaseSchema;
  };
}

const DEFAULT_RETRY: RetryOptions = { maxAttempts: 3, delay: 1 };

export function defineActor<P, R>(def: ActorDefinition<P, R>): Actor<P, R> {
  const retry = { ...DEFAULT_RETRY, ...def.retry };
  if (retry.maxAttempts < 1) {
    throw new Error(`actor "${def.name}": retry.maxAttempts must be >= 1`);
  }
  return {
    name: def.name,
    schema: def.schema,
    handler: def.handler,
    manifest: {
      name: def.name,
      queue: def.queue ?? "default",
      retry,
      maxConcurrent: def.maxConcurrent ?? 1,
      payloadJsonSchema: z.toJSONSchema(def.schema.payload, { io: "input" }),
      resultJsonSchema: z.toJSONSchema(def.schema.result, { io: "output" }),
    },
  };
}

// ---------------------------------------------------------------------------
// Example actors — the intended DX: one file, fully typed end to end.
// ---------------------------------------------------------------------------

export const sendWelcomeEmail = defineActor({
  name: "send-welcome-email",
  queue: "emails",
  retry: { maxAttempts: 5, delay: 2 },
  maxConcurrent: 4,
  schema: {
    payload: z.object({
      userId: z.uuid(),
      email: z.email(),
      locale: z.enum(["en", "de", "fr"]).default("en"),
    }),
    result: z.object({
      messageId: z.string(),
      acceptedAt: z.string(),
    }),
  },
  async handler(payload, ctx) {
    // Real bodies would call an email provider; the spike only proves typing.
    return {
      messageId: `msg_${payload.userId.slice(0, 8)}`,
      acceptedAt: new Date().toISOString(),
    };
  },
});

export const resizeImage = defineActor({
  name: "resize-image",
  queue: "media",
  retry: { maxAttempts: 3, delay: 5 },
  maxConcurrent: 2,
  schema: {
    payload: z.object({
      objectKey: z.string().min(1),
      width: z.int().positive().max(8192),
      format: z.enum(["webp", "avif", "jpeg"]),
    }),
    result: z.object({
      objectKey: z.string(),
      bytes: z.int(),
      width: z.int(),
    }),
  },
  async handler(payload) {
    return { objectKey: payload.objectKey, bytes: 0, width: payload.width };
  },
});

/** Discovery: the runtime imports this array to build its manifest. */
export const actors = [sendWelcomeEmail, resizeImage];
