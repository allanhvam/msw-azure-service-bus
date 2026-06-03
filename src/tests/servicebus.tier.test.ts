import assert from "node:assert/strict";
import { after, before, beforeEach, describe, test } from "node:test";
import { ServiceBusError } from "@azure/service-bus";
import { setupServer } from "msw/node";
import { handlers, type ServiceBusOptions } from "../handlers.js";
import { getMaxMessageSizeBytes } from "../amqp/messages/message-size.js";
import {
  createConnectionString,
  makeQueueName,
  receiveFromQueue,
  sendToQueue,
} from "./servicebus.testUtils.js";

const emulatorOptions: Pick<ServiceBusOptions, "lockDurationInMs" | "maxDeliveryCount"> = {
  lockDurationInMs: 3000,
  maxDeliveryCount: 3,
};

describe("Service Bus emulator tier quotas", () => {
  let mockServer: ReturnType<typeof setupServer> | undefined;

  before(() => {
    mockServer = setupServer(...handlers({ options: { ...emulatorOptions, tier: "basic" } }));
    mockServer.listen({ onUnhandledRequest: "bypass" });
  });

  after(() => {
    mockServer?.close();
  });

  function useHandlers(options?: ServiceBusOptions): void {
    mockServer?.resetHandlers(...handlers({ options: { ...emulatorOptions, ...options } }));
  }

  describe("basic tier", () => {
    beforeEach(() => {
      useHandlers({ tier: "basic" });
    });

    test("throws MessageSizeExceeded for messages larger than the basic tier limit", { timeout: 20000 }, async () => {
      const testQueue = makeQueueName("tier-basic-limit");
      const connectionString = createConnectionString(testQueue);
      const oversizedBody = "x".repeat(getMaxMessageSizeBytes("basic"));

      await assert.rejects(
        sendToQueue(connectionString, testQueue, [{ messageId: "oversized", body: oversizedBody }]),
        (error: unknown) => error instanceof ServiceBusError && error.code === "MessageSizeExceeded",
      );

      const messages = await receiveFromQueue(connectionString, testQueue, 1, 500);
      assert.equal(messages.length, 0);
    });

    test("accepts messages within the basic tier limit", { timeout: 20000 }, async () => {
      const testQueue = makeQueueName("tier-basic-ok");
      const connectionString = createConnectionString(testQueue);
      const withinLimitBody = "x".repeat(200 * 1024);

      await sendToQueue(connectionString, testQueue, [{ messageId: "within-limit", body: withinLimitBody }]);
      const messages = await receiveFromQueue(connectionString, testQueue, 1, 10_000);

      assert.equal(messages.length, 1);
      assert.equal(messages[0].body, withinLimitBody);
    });
  });

  describe("premium tier", () => {
    beforeEach(() => {
      useHandlers({ tier: "premium" });
    });

    test("accepts messages larger than the basic/standard limit", { timeout: 20000 }, async () => {
      const testQueue = makeQueueName("tier-premium-large");
      const connectionString = createConnectionString(testQueue);
      const largeBody = "x".repeat(300 * 1024);

      await sendToQueue(connectionString, testQueue, [{ messageId: "premium-large", body: largeBody }]);
      const messages = await receiveFromQueue(connectionString, testQueue, 1, 10_000);

      assert.equal(messages.length, 1);
      assert.equal(messages[0].body, largeBody);
    });
  });

  describe("standard tier", () => {
    beforeEach(() => {
      useHandlers({ tier: "standard" });
    });

    test("throws MessageSizeExceeded using the same limit as basic", { timeout: 20000 }, async () => {
      const testQueue = makeQueueName("tier-standard-limit");
      const connectionString = createConnectionString(testQueue);
      const oversizedBody = "x".repeat(getMaxMessageSizeBytes("standard"));

      await assert.rejects(
        sendToQueue(connectionString, testQueue, [{ messageId: "standard-oversized", body: oversizedBody }]),
        (error: unknown) => error instanceof ServiceBusError && error.code === "MessageSizeExceeded",
      );
    });
  });

  describe("default tier", () => {
    beforeEach(() => {
      useHandlers();
    });

    test("defaults to basic and throws MessageSizeExceeded when tier is omitted", { timeout: 20000 }, async () => {
      const testQueue = makeQueueName("tier-default-basic");
      const connectionString = createConnectionString(testQueue);
      const oversizedBody = "x".repeat(getMaxMessageSizeBytes("basic"));

      await assert.rejects(
        sendToQueue(connectionString, testQueue, [{ messageId: "default-tier-oversized", body: oversizedBody }]),
        (error: unknown) => error instanceof ServiceBusError && error.code === "MessageSizeExceeded",
      );
    });
  });
});