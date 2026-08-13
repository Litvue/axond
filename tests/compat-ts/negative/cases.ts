/**
 * Code that must *not* compile, which is how the lane's type checking is itself
 * tested: see `../src/typecheck.test.ts`. Never part of the build — the lane's
 * `tsconfig.json` includes `src` only.
 */

import OpenAI from "openai";

import { brokenUpstream } from "./broken-declaration.js";

// Declared by a `.d.ts` that does not type-check: an error only because the
// SDKs' own declarations are checked rather than trusted.
void brokenUpstream;

const client = new OpenAI({ apiKey: "test-inbound-key" });

// `messages` is required by the SDK's own definition of this call. A response or
// request shape the vendor no longer describes must fail the build here.
void client.chat.completions.create({ model: "chat-golden" });
