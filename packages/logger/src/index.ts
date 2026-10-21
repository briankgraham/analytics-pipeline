import { ClientRequest, IncomingMessage, ServerResponse } from 'node:http';
import { hostname } from 'node:os';
import type { LOG_LEVELS } from '@obs/config';
import { pino, type DestinationStream, type Logger } from 'pino';

export type { Logger } from 'pino';
export type LogLevel = (typeof LOG_LEVELS)[number];

export const REDACTED = '[redacted]';
export const TRUNCATED = '[truncated]';
export const CIRCULAR = '[circular]';
export const UNSERIALIZABLE = '[unserializable]';
export const MAX_DEPTH = 8;

/**
 * Key names whose values are censored at any depth, in any casing, inside arrays too. This is by
 * name rather than path because the same credential shows up in many shapes: Node `req.headers`,
 * Koa `ctx.request.header`, client options with `Authorization`, nested config objects.
 *
 * Naming rule for services: a raw API key is always bound as `apiKey`, never `key`. Kafka record
 * keys (`key`) are routing data that debugging needs, so `key` is deliberately not censored.
 * Name-based censoring cannot see inside free text (messages, `%s` arguments, URLs inside strings):
 * never interpolate a credential into one. URL objects and request paths do get their sensitive
 * query parameters censored.
 */
export const SENSITIVE_KEY =
  /^(authorization|proxy-authorization|cookie|set-cookie|x-api-key|x-auth-token|api[-_]?key|password|passwd|secret|client[-_]?secret|private[-_]?key|credentials?|token|access[-_]?token|refresh[-_]?token)$/i;

const isPlainObject = (value: object): boolean => {
  const proto: unknown = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
};

function decodedName(name: string): string {
  try {
    return decodeURIComponent(name.replaceAll('+', ' '));
  } catch {
    return name; // malformed percent-encoding: match on the raw name
  }
}

/** `a=1&token=x` with every sensitive parameter's value replaced by the literal marker. */
function redactParams(params: string): string {
  return params
    .split('&')
    .map((pair) => {
      const name = pair.split('=', 1).join('');
      return SENSITIVE_KEY.test(decodedName(name)) ? `${name}=${REDACTED}` : pair;
    })
    .join('&');
}

// Greedy up to the last `@` of the authority, so an unencoded `@` in the password cannot leak a tail.
const USERINFO_PASSWORD = /^([a-z][a-z0-9+.-]*:\/\/[^/?#@:]*:)[^/?#]*@/i;

/** String fields that hold a URL or request target (Koa's `ctx.request.toJSON().url`, etc.). */
const URL_KEY = /^(url|originalUrl|href|path)$/;

/**
 * A request target or URL as the client sent it, censored without being re-resolved: everything
 * before `?` stays verbatim (so path-confusion probes like `//evil.example/x` stay visible), query
 * and fragment parameters with sensitive names get the literal marker, and so does a userinfo
 * password.
 */
export function redactTarget(target: string): string {
  const hashAt = target.indexOf('#');
  const beforeHash = hashAt < 0 ? target : target.slice(0, hashAt);
  const queryAt = beforeHash.indexOf('?');
  let result =
    queryAt < 0
      ? beforeHash
      : `${beforeHash.slice(0, queryAt)}?${redactParams(beforeHash.slice(queryAt + 1))}`;
  if (hashAt >= 0) result += `#${redactParams(target.slice(hashAt + 1))}`;
  return result.replace(USERINFO_PASSWORD, `$1${REDACTED}@`);
}

/**
 * Allowlisted shapes for objects whose own fields would leak: Node HTTP messages keep the raw
 * headers in `rawHeaders`, an unkeyed array that name-based censoring cannot see, plus sockets.
 * Returns `value` itself when no special shape applies.
 */
function loggable(value: object): unknown {
  if (value instanceof URL) return redactTarget(value.href);
  if (value instanceof IncomingMessage) {
    return { method: value.method, url: redactTarget(value.url ?? ''), headers: value.headers };
  }
  if (value instanceof ServerResponse) {
    return { statusCode: value.statusCode, headers: value.getHeaders() };
  }
  if (value instanceof ClientRequest) {
    return {
      method: value.method,
      host: value.host,
      path: redactTarget(value.path),
      headers: value.getHeaders(),
    };
  }
  return value;
}

/**
 * A censored copy of a log payload; the caller's objects are never mutated. Every value is
 * covered, at any key and depth:
 * - plain objects and arrays are walked;
 * - Errors go through the allowlist serializer, wherever they appear (not only under `err`);
 * - URLs and Node HTTP requests and responses are reduced to allowlisted, censored shapes;
 * - objects with `toJSON` (Koa `ctx` and `ctx.request`, Dates, Buffers) are censored in the
 *   form JSON.stringify would write;
 * - other class instances have their own enumerable fields walked.
 * A throwing getter or `toJSON` yields [unserializable] instead of failing the log call.
 */
export function censor(value: unknown, depth = 0, seen = new WeakSet<object>()): unknown {
  if (typeof value !== 'object' || value === null) return value;
  if (seen.has(value)) return CIRCULAR;
  if (depth >= MAX_DEPTH) return TRUNCATED;
  seen.add(value);
  let result: unknown;
  try {
    result = value instanceof Error ? serializeError(value, depth) : walk(value, depth, seen);
  } catch {
    result = UNSERIALIZABLE;
  }
  seen.delete(value);
  return result;
}

function walk(value: object, depth: number, seen: WeakSet<object>): unknown {
  if (Array.isArray(value)) return value.map((item: unknown) => censor(item, depth + 1, seen));
  if (isPlainObject(value)) return censorFields(value, depth, seen);
  const shaped = loggable(value);
  if (shaped !== value) return censor(shaped, depth + 1, seen);
  const toJSON: unknown = Reflect.get(value, 'toJSON');
  return typeof toJSON === 'function'
    ? censor(Reflect.apply(toJSON, value, []), depth + 1, seen)
    : censorFields(value, depth, seen);
}

/** One object level of `censor`: sensitive keys are replaced, everything else is walked. */
export function censorFields(
  fields: object,
  depth = 0,
  seen = new WeakSet<object>([fields]),
): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(fields).map(([key, child]: [string, unknown]) => [
      key,
      SENSITIVE_KEY.test(key)
        ? REDACTED
        : URL_KEY.test(key) && typeof child === 'string'
          ? redactTarget(child)
          : censor(child, depth + 1, seen),
    ]),
  );
}

export interface SerializedError {
  type: string;
  message: string;
  stack?: string;
  code?: string | number;
  cause?: unknown;
}

/**
 * Errors are logged by allowlist: type, message, stack, code, and (recursively) cause. Client
 * errors carry request metadata (Elasticsearch `meta` holds the request headers, including
 * `authorization`), so arbitrary own properties are never copied into a log line. Something
 * thrown that is not an Error is kept, censored, rather than dropped: it is the only diagnostic.
 */
export function serializeError(error: unknown, depth = 0): unknown {
  if (!(error instanceof Error)) return censor(error, depth);
  const serialized: SerializedError = { type: error.constructor.name, message: error.message };
  if (error.stack !== undefined) serialized.stack = error.stack;
  const code = 'code' in error ? error.code : undefined;
  if (typeof code === 'string' || typeof code === 'number') serialized.code = code;
  if (error.cause !== undefined) {
    serialized.cause = depth + 1 < MAX_DEPTH ? serializeError(error.cause, depth + 1) : TRUNCATED;
  }
  return serialized;
}

export interface LoggerOptions {
  /** Service name, stamped on every record. */
  service: string;
  level?: LogLevel;
  /** Where JSON lines go; stdout by default. Tests pass an in-memory sink. */
  destination?: DestinationStream;
}

/**
 * Structured JSON logs: label levels, ISO timestamps, allowlisted error serialization, and
 * key-name censoring of every record and every child binding.
 *
 * Child loggers: create them with `child()`, never `setBindings()` (which bypasses censoring).
 * Children are built from the root (see censoringChildren), so `msgPrefix` and `serializers`
 * options set on an intermediate child are not inherited by its own children; bindings and level
 * are.
 *
 * The default sink is asynchronous (buffered). pino flushes it on normal process exit; graceful
 * shutdown should still call `logger.flush()` last, since a SIGKILL or OOM loses the buffer.
 */
export function createLogger({ service, level = 'info', destination }: LoggerOptions): Logger {
  const logger = pino(
    {
      level,
      base: { service, pid: process.pid, hostname: hostname() },
      timestamp: pino.stdTimeFunctions.isoTime,
      formatters: {
        level: (label) => ({ level: label }),
        bindings: (bindings) => censorFields(bindings),
        log: (record) => censorFields(record),
      },
      serializers: { err: serializeError },
    },
    destination ?? pino.destination(1),
  );
  return censoringChildren(logger);
}

/**
 * pino runs `formatters.bindings` only for the root bindings; child bindings are written as given.
 * So `child()` censors its bindings before pino sees them. Every logger inherits this wrapper from
 * the root (pino children use their parent as prototype), and every child is created from the
 * root with the caller's own (already censored) bindings and level carried over, so grandchildren
 * keep their parents' context.
 */
function censoringChildren<Levels extends string>(root: Logger<Levels>): Logger<Levels> {
  const fromRoot = root.child.bind(root);
  const rootKeys = new Set(Object.keys(root.bindings()));
  root.child = function (this: Logger<Levels>, bindings, options) {
    const inherited = Object.entries(this.bindings()).filter(([key]) => !rootKeys.has(key));
    return fromRoot(
      { ...Object.fromEntries(inherited), ...censorFields(bindings) },
      { level: this.level, ...options },
    );
  };
  return root;
}
