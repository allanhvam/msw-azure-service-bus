import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { AzureCliCredential, ChainedTokenCredential, ManagedIdentityCredential } from "@azure/identity";
import { ServiceBusClient } from "@azure/service-bus";
import { HttpResponse, http } from "msw";
import { setupServer } from "msw/node";
import { handlers } from "../handlers.js";

function makeQueueName(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

describe("Service Bus emulator auth integration", () => {
  let mockServer: ReturnType<typeof setupServer> | undefined;

  before(() => {
    mockServer = setupServer(
      ...handlers({ options: { verbose: false } }),
      http.get("http://169.254.169.254/metadata/identity/oauth2/token", ({ request }) => {
        console.log(`managed-identity: GET ${request.url}`);

        return HttpResponse.json({
          access_token: "eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0.eyJhdWQiOiJodHRwczovL3NlcnZpY2VidXMuYXp1cmUubmV0LyIsImlzcyI6Im1zdy1lbXVsYXRvciIsImV4cCI6NDc0MzU3NDAwMH0.",
          refresh_token: "",
          expires_in: "3599",
          expires_on: `${Math.floor(Date.now() / 1000) + 3599}`,
          not_before: `${Math.floor(Date.now() / 1000) - 60}`,
          resource: "https://management.azure.com/",
          token_type: "Bearer",
        });
      }),
    );
    mockServer.listen({ onUnhandledRequest: "error" });
  });

  after(() => {
    mockServer?.close();
  });

  test("send and receive a message with chained credentials", { timeout: 120000 }, async () => {
    const credential = new ChainedTokenCredential(
      new ManagedIdentityCredential(),
      new AzureCliCredential(),
    );

    const options = {
      fullyQualifiedNamespace: "mock.servicebus.windows.net",
      queueName: makeQueueName("identity"),
    };

    const { fullyQualifiedNamespace, queueName } = options;
    const serviceBusClient = new ServiceBusClient(
      fullyQualifiedNamespace,
      credential,
      {
        webSocketOptions: {
          webSocket: WebSocket,
        },
      },
    );

    try {
      const sender = serviceBusClient.createSender(queueName);
      await sender.sendMessages({ messageId: "identity-1", body: "hello-from-identity" });

      const receiver = serviceBusClient.createReceiver(queueName, { receiveMode: "receiveAndDelete" });
      const received = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });
      await receiver.close();
      await sender.close();

      assert.equal(received.length, 1);
      assert.equal(received[0].messageId, "identity-1");
      assert.equal(received[0].body, "hello-from-identity");
    } finally {
      await serviceBusClient.close();
    }
  });
});
