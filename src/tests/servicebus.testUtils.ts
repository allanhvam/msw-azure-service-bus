import { after, before } from "node:test";
import { ServiceBusClient, type ServiceBusReceivedMessage } from "@azure/service-bus";
import { setupServer } from "msw/node";
import { handlers } from "../handlers.js";

export function createConnectionString(queueName: string): string {
  return `Endpoint=sb://mock.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mock-key-${queueName}`;
}

export function makeQueueName(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export function createClient(connectionString: string): ServiceBusClient {
  return new ServiceBusClient(connectionString, {
    webSocketOptions: { webSocket: WebSocket },
  });
}

export async function sendToQueue(
  connectionString: string,
  queueName: string,
  messages: Array<{ messageId: string; body: unknown; contentType?: string }>,
): Promise<void> {
  const client = createClient(connectionString);

  try {
    const sender = client.createSender(queueName);
    await sender.sendMessages(messages);
    await sender.close();
  } finally {
    await client.close();
  }
}

export async function receiveFromQueue(
  connectionString: string,
  queueName: string,
  count: number,
  maxWaitTimeInMs: number,
): Promise<ServiceBusReceivedMessage[]> {
  const client = createClient(connectionString);

  try {
    const receiver = client.createReceiver(queueName, { receiveMode: "receiveAndDelete" });
    const messages = await receiver.receiveMessages(count, { maxWaitTimeInMs });
    await receiver.close();
    return messages;
  } finally {
    await client.close();
  }
}

export function setupServiceBusMock(): void {
  let mockServer: ReturnType<typeof setupServer> | undefined;

  before(() => {
    mockServer = setupServer(...handlers({
      options: {
        verbose: false,
        lockDurationInMs: 3000,
        maxDeliveryCount: 3,
      },
    }));
    mockServer.listen({ onUnhandledRequest: "bypass" });
  });

  after(() => {
    mockServer?.close();
  });
}

export const connectionString = createConnectionString("default");