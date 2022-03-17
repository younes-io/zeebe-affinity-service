import dayjs from 'dayjs';
import WebSocket from 'ws';
import {
  AffinityAPIMessageType,
  broadcastProcessOutcome,
  RegisterClientMessage,
  RegisterWorkerMessage,
  ProcessOutcomeMessage,
} from './WebSocketAPI';

import { v4 as uuid } from 'uuid';

interface ZBAffinityServerOptions {
  statsInterval?: number;
  logLevel?: 'INFO' | 'DEBUG';
}

interface WebSocketWithAlive extends WebSocket {
  isAlive: boolean;
  uuid: string;
  isWorker: boolean;
  isClient: boolean;
}

function heartbeat() {
  this.isAlive = true;
}

export class ZBAffinityServer {
  workers: { [key: string]: WebSocketWithAlive } = {};
  clients: { [key: string]: WebSocketWithAlive } = {};
  connections: { [key: string]: WebSocketWithAlive } = {};
  wss!: WebSocket.Server;
  options: ZBAffinityServerOptions;
  removeDeadConnections!: NodeJS.Timer;
  logLevel: string;

  constructor(options?: ZBAffinityServerOptions) {
    this.options = options || {};
    if (this.options.statsInterval) {
      setInterval(() => this.outputStats(), this.options.statsInterval);
    }
    this.logLevel = this.options.logLevel || 'INFO';
  }

  listen(port: number, cb?: () => void) {
    this.wss = new WebSocket.Server({
      port,
      perMessageDeflate: false,
    });

    this.removeDeadConnections = setInterval(() => {
      this.debug('Reaping dead connections');

      const reaper = (ws) => {
        this.debug({
          ws: {
            isAlive: ws.isAlive,
            isClient: ws.isClient,
            isWorker: ws.isWorker,
          },
        });
        if (ws.isAlive === false) {
          if (ws.isClient) {
            delete this.clients[ws.uuid];
          }
          if (ws.isWorker) {
            delete this.workers[ws.uuid];
          }
          delete this.connections[ws.uuid];
          return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
      };
      Object.values(this.connections).forEach(reaper);
    }, 30000);

    this.wss.on('connection', (w) => {
      const ws = w as WebSocketWithAlive;
      ws.isAlive = true;
      ws.on('pong', heartbeat);
      ws.ping(); // @DEBUG
      ws.on('message', (message) => {
        const msg:
          | RegisterClientMessage
          | RegisterWorkerMessage
          | ProcessOutcomeMessage = JSON.parse(message.toString());
        switch (msg.type) {
          case AffinityAPIMessageType.REGISTER_CLIENT:
            ws.isClient = true;
            // eslint-disable-next-line no-case-declarations
            const clientId = ws.uuid || uuid();
            ws.uuid = clientId;
            this.clients[clientId] = ws;
            this.connections[clientId] = ws;
            this.debug('New client connected');
            break;
          case AffinityAPIMessageType.REGISTER_WORKER:
            ws.isWorker = true;
            // eslint-disable-next-line no-case-declarations
            const workerId = ws.uuid || uuid();
            ws.uuid = workerId;
            this.workers[workerId] = ws;
            this.connections[workerId] = ws;
            this.debug('New worker connected');
            break;
          case AffinityAPIMessageType.PROCESS_OUTCOME:
            broadcastProcessOutcome(this.clients, msg);
            break;
        }
      });
    });
    if (cb) {
      cb();
    }
  }

  stats() {
    return {
      time: dayjs().format('{YYYY} MM-DDTHH:mm:ss SSS [Z] A'), // display
      workerCount: Object.keys(this.workers).length,
      clientCount: Object.keys(this.clients).length,
      cpu: process.cpuUsage(),
      memory: process.memoryUsage(),
    };
  }

  outputStats() {
    const stats = this.stats();
    console.log(stats.time); // display
    console.log(`Worker count: ${stats.workerCount}`);
    console.log(`Client count: ${stats.clientCount}`);
    console.log(`CPU:`, stats.cpu);
    console.log(`Memory used:`, stats.memory);
  }

  private log(...args) {
    console.log(args);
  }

  private debug(...args) {
    if (this.logLevel === 'DEBUG') {
      console.log(args);
    }
  }
}
