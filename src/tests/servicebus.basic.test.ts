import assert from "node:assert/strict";
import { describe, test } from "node:test";
import {
  connectionString,
  createClient,
  makeQueueName,
  receiveFromQueue,
  sendToQueue,
  setupServiceBusMock,
} from "./servicebus.testUtils.js";

describe("Service Bus emulator integration - basic messaging", () => {
  setupServiceBusMock();

  test("send and receive a single message", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("single");
    const expectedBody = "body-123";

    await sendToQueue(connectionString, testQueue, [{ messageId: "message-1", body: expectedBody }]);
    const messages = await receiveFromQueue(connectionString, testQueue, 1, 10000);

    assert.equal(messages.length, 1);
    assert.equal(messages[0].messageId, "message-1");
    assert.equal(messages[0].body, expectedBody);
  });

  test("send and receive a single message with contentType", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("single-json");
    const expectedBody = "body-123";

    await sendToQueue(connectionString, testQueue, [
      { messageId: "message-1-json", body: expectedBody, contentType: "application/json" },
    ]);
    const messages = await receiveFromQueue(connectionString, testQueue, 1, 10000);

    assert.equal(messages.length, 1);
    assert.equal(messages[0].messageId, "message-1-json");
    assert.equal(messages[0].body, expectedBody);
    assert.equal(messages[0].contentType, "application/json");
  });

  test("send and receive a single JSON object message with contentType", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("single-json-object");
    const expectedBody = { kind: "object", value: 42 };

    await sendToQueue(connectionString, testQueue, [
      { messageId: "message-1-json-object", body: expectedBody, contentType: "application/json" },
    ]);
    const messages = await receiveFromQueue(connectionString, testQueue, 1, 10000);

    assert.equal(messages.length, 1);
    assert.equal(messages[0].messageId, "message-1-json-object");
    assert.deepEqual(messages[0].body, expectedBody);
    assert.equal(messages[0].contentType, "application/json");
  });

  test("send and receive multiple messages", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("multi");

    await sendToQueue(connectionString, testQueue, [
      { messageId: "message-2", body: "body-2" },
      { messageId: "message-3", body: "body-3" },
    ]);

    const received = await receiveFromQueue(connectionString, testQueue, 2, 10000);

    assert.equal(received.length, 2);
    const receivedById = new Map(received.map((message) => [message.messageId, message.body]));
    assert.equal(receivedById.get("message-2"), "body-2");
    assert.equal(receivedById.get("message-3"), "body-3");
  });

  test("empty queue receive returns zero messages", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("empty");
    const messages = await receiveFromQueue(connectionString, testQueue, 1, 1000);
    assert.equal(messages.length, 0);
  });

  test("preserves message ordering", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("order");

    await sendToQueue(connectionString, testQueue, [
      { messageId: "order-1", body: "first" },
      { messageId: "order-2", body: "second" },
      { messageId: "order-3", body: "third" },
    ]);

    const messages = await receiveFromQueue(connectionString, testQueue, 3, 10000);
    assert.equal(messages.length, 3);
    assert.deepEqual(
      messages.map((message) => ({ messageId: message.messageId, body: message.body })),
      [
        { messageId: "order-1", body: "first" },
        { messageId: "order-2", body: "second" },
        { messageId: "order-3", body: "third" },
      ],
    );
  });

  test("supports multiple body types", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("types");

    await sendToQueue(connectionString, testQueue, [
      { messageId: "type-1", body: "text" },
      { messageId: "type-2", body: { kind: "object", value: 42 } },
      { messageId: "type-3", body: 12345 },
    ]);

    const messages = await receiveFromQueue(connectionString, testQueue, 3, 10000);

    assert.equal(messages.length, 3);
    const byId = new Map(messages.map((message) => [message.messageId, message.body]));
    assert.equal(byId.get("type-1"), "text");
    assert.deepEqual(byId.get("type-2"), { kind: "object", value: 42 });
    assert.equal(byId.get("type-3"), 12345);
  });

  test("delivers duplicate message ids as separate messages", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("dup-id");
    await sendToQueue(connectionString, testQueue, [
      { messageId: "same-id", body: "first-copy" },
      { messageId: "same-id", body: "second-copy" },
    ]);

    const messages = await receiveFromQueue(connectionString, testQueue, 2, 10000);

    assert.equal(messages.length, 2);
    assert.equal(messages[0].messageId, "same-id");
    assert.equal(messages[1].messageId, "same-id");
    assert.deepEqual(messages.map((message) => message.body), ["first-copy", "second-copy"]);
  });

  test("handles large payloads", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("large");
    const largeBody = "x".repeat(100 * 1024);

    await sendToQueue(connectionString, testQueue, [{ messageId: "large-1", body: largeBody }]);
    const messages = await receiveFromQueue(connectionString, testQueue, 1, 10000);

    assert.equal(messages.length, 1);
    assert.equal(messages[0].messageId, "large-1");
    assert.equal(messages[0].body, largeBody);
  });

  test("supports concurrent senders", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("concurrent");
    const senderClientA = createClient(connectionString);
    const senderClientB = createClient(connectionString);

    try {
      const senderA = senderClientA.createSender(testQueue);
      const senderB = senderClientB.createSender(testQueue);

      await Promise.all([
        senderA.sendMessages({ messageId: "concurrent-a", body: "from-a" }),
        senderB.sendMessages({ messageId: "concurrent-b", body: "from-b" }),
      ]);

      await senderA.close();
      await senderB.close();
    } finally {
      await senderClientA.close();
      await senderClientB.close();
    }

    const messages = await receiveFromQueue(connectionString, testQueue, 2, 10000);
    assert.equal(messages.length, 2);

    const byId = new Map(messages.map((message) => [message.messageId, message.body]));
    assert.equal(byId.get("concurrent-a"), "from-a");
    assert.equal(byId.get("concurrent-b"), "from-b");
  });

  test("receiveMessages request count can exceed available messages", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("credit");

    await sendToQueue(connectionString, testQueue, [
      { messageId: "credit-1", body: "c1" },
      { messageId: "credit-2", body: "c2" },
    ]);

    const messages = await receiveFromQueue(connectionString, testQueue, 5, 3000);

    assert.equal(messages.length, 2);
    const ids = messages.map((message) => message.messageId as string).sort();
    assert.deepEqual(ids, ["credit-1", "credit-2"]);
  });

  test("message survives connection lifecycle change", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("lifecycle");

    await sendToQueue(connectionString, testQueue, [{ messageId: "life-1", body: "survives" }]);
    const messages = await receiveFromQueue(connectionString, testQueue, 1, 10000);

    assert.equal(messages.length, 1);
    assert.equal(messages[0].messageId, "life-1");
    assert.equal(messages[0].body, "survives");
  });

  test("queues are isolated", { timeout: 20000 }, async () => {
    const queueA = makeQueueName("isolation-a");
    const queueB = makeQueueName("isolation-b");

    await sendToQueue(connectionString, queueA, [{ messageId: "iso-1", body: "only-a" }]);

    const fromQueueB = await receiveFromQueue(connectionString, queueB, 1, 1000);
    assert.equal(fromQueueB.length, 0);

    const fromQueueA = await receiveFromQueue(connectionString, queueA, 1, 10000);
    assert.equal(fromQueueA.length, 1);
    assert.equal(fromQueueA[0].messageId, "iso-1");
    assert.equal(fromQueueA[0].body, "only-a");
  });
});