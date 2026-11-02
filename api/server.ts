// The API server. It owns everything the API needs and starts it in a fixed order: the clients first,
// then the middleware and routes that use them, and the port last, so no request arrives before
// everything is ready. Closing goes the other way.
import type { Server } from 'node:http';
import { isDeepStrictEqual } from 'node:util';
import type Koa from 'koa';
import { KafkaClient } from '../kafka/client.ts';
import { createLogSender } from '../kafka/send-log.ts';
import { closeInOrder } from '../services/close-in-order.ts';
import type { Service } from '../services/service.ts';
import { createApp } from './app.ts';
import type { ApiSettings } from './settings.ts';

// Listening on 127.0.0.1 means only this machine can reach the API.
const host = '127.0.0.1';

const closedMessage = 'ApiServer is closed: get a new one with getInstance()';

// Opens the port. Throws if it can't, for example when another program is already using it.
function openPort(app: Koa, port: number): Promise<Server> {
  return new Promise((resolve, reject) => {
    const server = app.listen(port, host);
    server.once('error', reject);
    server.once('listening', () => {
      server.off('error', reject);
      resolve(server);
    });
  });
}

function closePort(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

export class ApiServer implements Service {
  private static instance: ApiServer | undefined;

  private readonly settings: ApiSettings;
  private readonly kafka: KafkaClient;
  private server: Server | undefined;
  private starting: Promise<void> | undefined;
  private closing: Promise<void> | undefined;
  private closed = false;

  private constructor(settings: ApiSettings) {
    this.settings = settings;
    this.kafka = KafkaClient.getInstance(settings.kafkaBrokers, 'api');
  }

  // Returns the one ApiServer. Throws if it's asked for with different settings, because those would
  // otherwise be silently ignored.
  static getInstance(settings: ApiSettings): ApiServer {
    const existing = ApiServer.instance;
    if (existing === undefined) {
      ApiServer.instance = new ApiServer(settings);
      return ApiServer.instance;
    }
    if (!isDeepStrictEqual(existing.settings, settings)) {
      throw new Error('ApiServer is already running with different settings');
    }
    return existing;
  }

  // Starts everything. Calling it again does nothing new, even while the first call is still working.
  // After a failure, whatever did start stays open until close, and the next call tries again.
  init(): Promise<void> {
    if (this.closed) {
      return Promise.reject(new Error(closedMessage));
    }
    this.starting ??= this.start().catch((error: unknown) => {
      this.starting = undefined;
      throw error;
    });
    return this.starting;
  }

  private async start(): Promise<void> {
    const { port, topic } = this.settings;

    await this.kafka.init();
    const app = createApp({ sendLog: createLogSender(this.kafka, topic) });
    if (this.closed) {
      throw new Error(closedMessage);
    }
    this.server = await openPort(app, port);
    console.log(`api: listening on http://${host}:${port}, sending logs to "${topic}"`);
  }

  // Waits for any start-up still working, stops taking requests and lets the ones in progress finish,
  // then closes the clients they were using. Calling it again gives the same answer,
  // when the first call finishes.
  close(): Promise<void> {
    this.closing ??= this.closeEverything();
    return this.closing;
  }

  private async closeEverything(): Promise<void> {
    this.closed = true;
    if (ApiServer.instance === this) {
      ApiServer.instance = undefined;
    }
    await this.starting?.catch(() => {});

    const server = this.server;
    this.server = undefined;
    await closeInOrder([
      async () => {
        if (server !== undefined) {
          await closePort(server);
        }
      },
      () => this.kafka.close(),
    ]);
  }
}
