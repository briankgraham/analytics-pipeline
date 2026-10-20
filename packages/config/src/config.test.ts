import fc from 'fast-check';
import { z } from 'zod';
import { describe, expect, expectTypeOf, it } from 'vitest';
import {
  boolFlag,
  brokerList,
  ConfigError,
  esConfig,
  httpUrl,
  kafkaConfig,
  loadConfig,
  logLevel,
  port,
  positiveInt,
  WITHHELD,
} from './index.js';

function configError(run: () => unknown): ConfigError {
  try {
    run();
  } catch (error) {
    if (error instanceof ConfigError) return error;
    throw error;
  }
  throw new Error('expected a ConfigError');
}

describe('loadConfig', () => {
  const shape = { PORT: port, LOG_LEVEL: logLevel.default('info') };

  it('parses, applies defaults, and ignores unrelated variables', () => {
    expect(loadConfig(shape, { PORT: '8080', HOME: '/root', PATH: '/bin' })).toEqual({
      PORT: 8080,
      LOG_LEVEL: 'info',
    });
  });

  it('freezes the result deeply, and types it that way', () => {
    const config = loadConfig(kafkaConfig, {
      KAFKA_BROKERS: 'kafka:29092',
      KAFKA_CLIENT_ID: 'indexer',
    });
    expect(Object.isFrozen(config)).toBe(true);
    expect(Object.isFrozen(config.KAFKA_BROKERS)).toBe(true);
    expectTypeOf(config.KAFKA_BROKERS).toEqualTypeOf<readonly string[]>();
    expectTypeOf(config).toEqualTypeOf<{
      readonly KAFKA_BROKERS: readonly string[];
      readonly KAFKA_CLIENT_ID: string;
    }>();
  });

  it('treats an empty value as unset, so defaults apply and required vars are reported', () => {
    expect(loadConfig(shape, { PORT: '1', LOG_LEVEL: '' }).LOG_LEVEL).toBe('info');
    expect(configError(() => loadConfig(shape, { PORT: '' })).problems).toEqual(['PORT: required']);
  });

  it('reports every problem at once, sorted by variable name', () => {
    const error = configError(() =>
      loadConfig({ ...shape, KAFKA_BROKERS: brokerList }, { PORT: 'x', LOG_LEVEL: 'loud' }),
    );
    expect(error.problems).toEqual([
      'KAFKA_BROKERS: required',
      'LOG_LEVEL: must be one of fatal, error, warn, info, debug, trace, silent',
      'PORT: must be a port number 1-65535',
    ]);
    expect(error.message).toContain('3 invalid environment variables');
    expect(error.name).toBe('ConfigError');
  });

  it('says "variable" for a single problem', () => {
    expect(configError(() => loadConfig(shape, {})).message).toMatch(
      /^1 invalid environment variable:/,
    );
  });

  it('reports a problem once even when several entries fail the same way', () => {
    expect(configError(() => loadConfig({ B: brokerList }, { B: 'x,y' })).problems).toEqual([
      'B: each broker must be host:port',
    ]);
  });

  describe('never reveals a value', () => {
    it.each(['-', 'e', 'port', ' port '])(
      'withholds the whole message when it contains the value %j',
      (value) => {
        const error = configError(() => loadConfig({ PORT: port }, { PORT: value }));
        expect(error.problems).toEqual([`PORT: ${WITHHELD}`]);
      },
    );

    it('withholds a schema message that interpolates the input', () => {
      const leaky = z
        .string()
        .refine(() => false, { error: (issue) => `got ${String(issue.input)}` });
      const error = configError(() => loadConfig({ TOKEN: leaky }, { TOKEN: 'sekret' }));
      expect(error.problems).toEqual([`TOKEN: ${WITHHELD}`]);
      expect(error.message).not.toContain('sekret');
    });

    it('turns a throwing parser into a contained problem, keeping the others', () => {
      const throwing = z.string().transform((s): unknown => JSON.parse(s));
      const error = configError(() =>
        loadConfig({ JSON_VAR: throwing, PORT: port }, { JSON_VAR: '{sekret', PORT: '0' }),
      );
      expect(error.problems).toEqual([
        'JSON_VAR: invalid value (parser threw)',
        'PORT: must be a port number 1-65535',
      ]);
      expect(error.message).not.toContain('sekret');
    });

    it('holds for arbitrary values: every invalid value errors without appearing', () => {
      const secretShape = { PORT: port, ES_URL: httpUrl, FLAG: boolFlag, N: positiveInt };
      fc.assert(
        fc.property(
          // Only values invalid for all four variables, so every one must be reported.
          fc
            .string({ minLength: 1 })
            .filter(
              (s) =>
                s.trim() !== '' &&
                !/^[1-9][0-9]*$/.test(s) &&
                s !== 'true' &&
                s !== 'false' &&
                !/^https?:\/\//i.test(s),
            ),
          (secret) => {
            const error = configError(() =>
              loadConfig(secretShape, { PORT: secret, ES_URL: secret, FLAG: secret, N: secret }),
            );
            const names = new Set(error.problems.map((p) => p.slice(0, p.indexOf(':'))));
            expect([...names].sort()).toEqual(['ES_URL', 'FLAG', 'N', 'PORT']);
            for (const problem of error.problems) {
              const text = problem.slice(problem.indexOf(': ') + 2);
              // The fixed withheld text never derives from the value, so it may contain any letter.
              if (text !== WITHHELD) {
                expect(text.includes(secret) || text.includes(secret.trim())).toBe(false);
              }
            }
          },
        ),
      );
    });
  });
});

describe('coercers', () => {
  const parse = <T extends z.ZodType>(schema: T, value: string) => schema.safeParse(value);

  it.each([
    ['1', 1],
    ['65535', 65535],
  ])('port accepts %s', (value, expected) => {
    expect(parse(port, value)).toMatchObject({ success: true, data: expected });
  });

  it.each(['0', '65536', '-1', '80.5', '8080x', ' 80', '0x50', '1e3'])(
    'port rejects %j',
    (value) => {
      expect(parse(port, value).success).toBe(false);
    },
  );

  it.each([
    ['1', 1],
    ['500', 500],
  ])('positiveInt accepts %s', (value, expected) => {
    expect(parse(positiveInt, value)).toMatchObject({ success: true, data: expected });
  });

  it.each(['0', '-3', '1.5', 'ten', '9007199254740993'])('positiveInt rejects %j', (value) => {
    expect(parse(positiveInt, value).success).toBe(false);
  });

  it.each([
    ['true', true],
    ['false', false],
  ])('boolFlag accepts %s', (value, expected) => {
    expect(parse(boolFlag, value)).toMatchObject({ success: true, data: expected });
  });

  it.each(['1', 'yes', 'TRUE', 'False'])('boolFlag rejects %j (only true/false)', (value) => {
    expect(parse(boolFlag, value).success).toBe(false);
  });

  it('brokerList splits, trims, and validates host:port entries', () => {
    expect(parse(brokerList, 'kafka:29092, localhost:9092,10.0.0.5:9092')).toMatchObject({
      success: true,
      data: ['kafka:29092', 'localhost:9092', '10.0.0.5:9092'],
    });
  });

  it.each([
    'kafka',
    'kafka:0',
    'kafka:99999',
    'a:1,,b:2',
    ',',
    'http://kafka:9092',
    'ka fka:1',
    '.:9092',
    '-:9092',
    'kafka-:9092',
    'kafka_1:9092',
  ])('brokerList rejects %j', (value) => {
    expect(parse(brokerList, value).success).toBe(false);
  });

  it.each(['http://elasticsearch:9200', 'https://es.example.com', 'http://es:9200/path?x=a@b'])(
    'httpUrl accepts %s',
    (url) => {
      expect(parse(httpUrl, url).success).toBe(true);
    },
  );

  it.each([
    'elasticsearch:9200',
    'ftp://es',
    'not a url',
    'http://',
    ' http://es:9200',
    'http://es:9200 ',
    'http://user:pass@es:9200',
    'https://token@es:9200',
  ])('httpUrl rejects %j', (url) => {
    expect(parse(httpUrl, url).success).toBe(false);
  });

  it.each(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])(
    'logLevel accepts %s',
    (level) => {
      expect(parse(logLevel, level).success).toBe(true);
    },
  );

  it('logLevel rejects unknown levels', () => {
    expect(parse(logLevel, 'verbose').success).toBe(false);
  });
});

describe('shared fragments', () => {
  it('are frozen so no user can swap a schema for everyone else', () => {
    expect(Object.isFrozen(kafkaConfig)).toBe(true);
    expect(Object.isFrozen(esConfig)).toBe(true);
  });

  it('kafkaConfig reads brokers and a client id', () => {
    expect(
      loadConfig(kafkaConfig, { KAFKA_BROKERS: 'kafka:29092', KAFKA_CLIENT_ID: 'indexer' }),
    ).toEqual({ KAFKA_BROKERS: ['kafka:29092'], KAFKA_CLIENT_ID: 'indexer' });
    expect(
      configError(() => loadConfig(kafkaConfig, { KAFKA_CLIENT_ID: 'Bad Id' })).problems,
    ).toEqual(['KAFKA_BROKERS: required', expect.stringMatching(/^KAFKA_CLIENT_ID: /) as string]);
  });

  it('esConfig reads the node URL', () => {
    expect(loadConfig(esConfig, { ES_URL: 'http://elasticsearch:9200' })).toEqual({
      ES_URL: 'http://elasticsearch:9200',
    });
  });

  it('fragments compose with service-specific variables', () => {
    const config = loadConfig(
      { ...kafkaConfig, ...esConfig, PORT: port },
      {
        KAFKA_BROKERS: 'kafka:29092',
        KAFKA_CLIENT_ID: 'ingest-api',
        ES_URL: 'http://elasticsearch:9200',
        PORT: '3000',
      },
    );
    expect(config.PORT).toBe(3000);
    expect(config.KAFKA_BROKERS).toEqual(['kafka:29092']);
  });
});
