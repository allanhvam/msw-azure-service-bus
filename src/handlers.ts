import { ws } from "msw";
import { AmqpProtocolEmulator } from "./amqp/emulator.js";
import { parseServiceBusAmqpRequest, toBytes } from "./amqp/frame.js";
import type { AmqpPerformative } from "./amqp/types/protocol.js";
import type { QueueMessage } from "./amqp/types/emulator.js";

export type ServiceBusAmqpRequest = {
  timestamp: string;
  connectionUrl: string;
  messageType: "protocol-header" | "amqp-frame" | "text" | "binary";
  byteLength: number;
  hexPreview: string;
  textPreview?: string;
  bodyText?: string;
  frame?: {
    channel: number;
    size: number;
    doff: number;
    type: number;
    performative?: AmqpPerformative;
  };
};

export type ServiceBusOptions = {
  verbose?: boolean;
  lockDurationInMs?: number;
  maxDeliveryCount?: number;
};

const mockQueues = new Map<string, QueueMessage[]>();
const amqpEmulator = new AmqpProtocolEmulator(mockQueues);

export function handlers({ options = {} }: { options?: ServiceBusOptions } = {}) {
  const { verbose = false, lockDurationInMs = 60_000, maxDeliveryCount = 10 } = options;
  const serviceBusWs = ws.link(/^wss:\/\/.*\/\$servicebus\/websocket(?:\?.*)?$/);

  amqpEmulator.setOptions({
    debugEnabled: verbose,
    lockDurationInMs,
    maxDeliveryCount,
  });

  return [
    serviceBusWs.addEventListener("connection", ({ client }) => {
      const connectionId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      amqpEmulator.createConnection(connectionId);

      client.addEventListener("message", async ({ data }) => {
        const bytes = await toBytes(data);
        const parsedRequest = parseServiceBusAmqpRequest(data, bytes);

        const amqpRequest: ServiceBusAmqpRequest = {
          timestamp: new Date().toISOString(),
          connectionUrl: client.url.toString(),
          messageType: parsedRequest.messageType,
          byteLength: parsedRequest.byteLength,
          hexPreview: parsedRequest.hexPreview,
          textPreview: parsedRequest.textPreview,
          bodyText: parsedRequest.bodyText,
          frame: parsedRequest.frame,
        };

        if (verbose) {
          console.log("[MSW ServiceBus] AMQP request", {
            messageType: amqpRequest.messageType,
            byteLength: amqpRequest.byteLength,
            hexPreview: amqpRequest.hexPreview,
            bodyText: amqpRequest.bodyText,
            performative: amqpRequest.frame?.performative,
            channel: amqpRequest.frame?.channel,
          });
        }

        amqpEmulator.handleMessage(connectionId, client, bytes, parsedRequest.parsedFrame);

        if (verbose) {
          console.log("Message observed:");
          console.dir(amqpRequest);
        }
      });

      client.addEventListener("close", () => {
        amqpEmulator.removeConnection(connectionId);

        if (verbose) {
          console.log("[MSW ServiceBus] connection closed", { connectionId });
        }
      });
    }),
  ];
}
