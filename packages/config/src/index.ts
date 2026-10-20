import { Identifier } from '@obs/schemas';
import { z } from 'zod';

export type Env = Readonly<Record<string, string | undefined>>;

/** What loadConfig returns: frozen all the way down, and typed that way so mutation never compiles. */
export type DeepReadonly<T> = T extends (infer U)[]
  ? readonly DeepReadonly<U>[]
  : T extends object
    ? { readonly [K in keyof T]: DeepReadonly<T[K]> }
    : T;

/** Thrown at boot when the environment is invalid. Lists variable names, never their values. */
export class ConfigError extends Error {
  override readonly name = 'ConfigError';

  constructor(readonly problems: readonly string[]) {
    super(
      `${String(problems.length)} invalid environment variable${problems.length === 1 ? '' : 's'}:\n  ${problems.join('\n  ')}`,
    );
  }
}

function deepFreeze<T>(value: T): DeepReadonly<T> {
  if (typeof value === 'object' && value !== null) {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value as DeepReadonly<T>; // frozen above, recursively, which is exactly what the type says
}

export const WITHHELD = 'invalid value (details withheld: they would reveal it)';

/** One variable's problems, phrased so that the value itself can never appear in them. */
function problemsFor(name: string, schema: z.core.$ZodType, value: string | undefined): string[] {
  let result: z.ZodSafeParseResult<unknown>;
  try {
    result = z.safeParse(schema, value);
  } catch {
    // A throwing transform (JSON.parse, new URL) often quotes its input in the error.
    return [`${name}: invalid value (parser threw)`];
  }
  if (result.success) return [];
  if (value === undefined) return [`${name}: required`];
  return result.error.issues.map((issue) => {
    const revealing = [value, value.trim()].some((v) => v !== '' && issue.message.includes(v));
    return `${name}: ${revealing ? WITHHELD : issue.message}`;
  });
}

/**
 * Parses the variables named in `shape` from `env` (each service's main.ts passes process.env).
 * An empty value counts as unset, so compose's `${VAR:-}` falls back to the default. Every problem
 * is collected into one ConfigError so a broken deployment shows all its mistakes at once.
 *
 * Contract for shapes: schemas are pure and deterministic (each runs twice: once per variable,
 * once for the typed object), and error messages never interpolate the input (config can hold
 * secrets).
 * Messages that contain the raw or trimmed value are withheld as a backstop, but a message built
 * from a transformed value cannot be recognized.
 */
export function loadConfig<S extends z.ZodRawShape>(
  shape: S,
  env: Env,
): DeepReadonly<z.output<z.ZodObject<S>>> {
  const relevant = Object.fromEntries(
    Object.keys(shape).map((name) => [name, env[name] === '' ? undefined : env[name]]),
  );
  // Parse each variable alone, so one throwing parser or revealing message stays contained.
  const problems = Object.entries(shape).flatMap(([name, schema]) =>
    problemsFor(name, schema, relevant[name]),
  );
  if (problems.length > 0) throw new ConfigError([...new Set(problems)].sort());
  // Every variable parsed on its own; parse once more as an object for the typed result.
  return deepFreeze(z.object(shape).parse(relevant));
}

const PORT_PATTERN = /^[1-9][0-9]{0,4}$/;
const MAX_PORT = 65_535;

/** TCP port 1-65535, decimal digits only. */
export const port = z
  .string()
  .regex(PORT_PATTERN, { error: 'must be a port number 1-65535' })
  .transform(Number)
  .pipe(z.int().max(MAX_PORT, { error: 'must be a port number 1-65535' }));

/** A positive safe integer, decimal digits only. */
export const positiveInt = z
  .string()
  .regex(/^[1-9][0-9]*$/, { error: 'must be a positive integer' })
  .transform(Number)
  .pipe(z.int({ error: 'must be a positive safe integer' }));

/** Exactly `true` or `false`; anything else is a typo, not a default. */
export const boolFlag = z
  .enum(['true', 'false'], { error: 'must be true or false' })
  .transform((value) => value === 'true');

/**
 * `host:port`. Host labels are DNS-style (letters, digits, `-`, `.`, starting and ending with a
 * letter or digit); underscores are not valid in hostnames and fail loudly at boot.
 */
const BROKER = /^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?:([1-9][0-9]{0,4})$/;

/** Comma-separated `host:port` list, e.g. `kafka:29092,kafka-2:29092`. */
export const brokerList = z
  .string()
  .transform((list) => list.split(',').map((broker) => broker.trim()))
  .pipe(
    z.array(
      z.string().refine(
        (broker) => {
          const match = BROKER.exec(broker);
          return match !== null && Number(match[1]) <= MAX_PORT;
        },
        { error: 'each broker must be host:port' },
      ),
    ),
  );

/**
 * An http(s) URL, e.g. the Elasticsearch node. No surrounding whitespace, and no credentials: a
 * URL with user info would turn a logged config value into a secret, so credentials get their own
 * variables.
 */
export const httpUrl = z
  .string()
  // Checked on the raw string: z.url() tolerates surrounding whitespace.
  .regex(/^\S+$/, { error: 'must not contain whitespace' })
  .pipe(z.url({ protocol: /^https?$/, error: 'must be an http(s) URL' }))
  .refine((url) => !/^https?:\/\/[^/?#]*@/i.test(url), {
    error: 'must not contain credentials; use separate variables',
  });

export const LOG_LEVELS = ['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'] as const;
export const logLevel = z.enum(LOG_LEVELS, { error: `must be one of ${LOG_LEVELS.join(', ')}` });

/** Shared env fragments; spread them into a service's shape alongside its own variables. */
export const kafkaConfig = Object.freeze({
  KAFKA_BROKERS: brokerList,
  KAFKA_CLIENT_ID: Identifier,
});

export const esConfig = Object.freeze({
  ES_URL: httpUrl,
});
