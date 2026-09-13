// Shared types for TaskQ foreign actors.
import { z } from "zod";

export type ActorDef = {
  name: string;
  queue: string;
  payload: z.ZodType;
  result: z.ZodType;
  retry?: { max_attempts: number };
  rate_limits?: Array<Record<string, unknown>>;
  handler: (payload: any, ctx: any) => Promise<unknown>;
};

export type SubJob = { actor: string; payload: unknown };
