import assert from "node:assert/strict";
import { describe, test } from "node:test";
import {
  connectionString,
  createClient,
  makeQueueName,
  sendToQueue,
  setupServiceBusMock,
} from "./servicebus.testUtils.js";

describe("Service Bus emulator integration - peek lock and settlements", () => {
  setupServiceBusMock();

  test("receiveAndDelete mode does not expose lock token", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("mode-rad");

    await sendToQueue(connectionString, testQueue, [{ messageId: "rad-1", body: "payload" }]);

    const client = createClient(connectionString);
    try {
      const receiver = client.createReceiver(testQueue, { receiveMode: "receiveAndDelete" });
      const messages = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });
      await receiver.close();

      assert.equal(messages.length, 1);
      assert.equal(messages[0].messageId, "rad-1");
      assert.equal(messages[0].lockToken, undefined);
      assert.equal(messages[0].lockedUntilUtc, undefined);
    } finally {
      await client.close();
    }
  });

  test("peekLock mode exposes lock token and lockedUntilUtc", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("mode-peek");

    await sendToQueue(connectionString, testQueue, [{ messageId: "peek-1", body: "payload" }]);

    const client = createClient(connectionString);
    try {
      const receiver = client.createReceiver(testQueue);
      const messages = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });
      await receiver.close();

      assert.equal(messages.length, 1);
      assert.equal(messages[0].messageId, "peek-1");
      assert.equal(typeof messages[0].lockToken, "string");
      assert.ok(messages[0].lockToken && messages[0].lockToken.length > 0);
      assert.ok(messages[0].lockedUntilUtc instanceof Date);
    } finally {
      await client.close();
    }
  });

  test("peekLock complete removes the message", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("peek-complete");
    await sendToQueue(connectionString, testQueue, [{ messageId: "peek-complete-1", body: "payload" }]);

    const client = createClient(connectionString);
    try {
      const receiver = client.createReceiver(testQueue);
      const firstBatch = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });

      assert.equal(firstBatch.length, 1);
      await receiver.completeMessage(firstBatch[0]);

      const secondBatch = await receiver.receiveMessages(1, { maxWaitTimeInMs: 1000 });
      assert.equal(secondBatch.length, 0);

      await receiver.close();
    } finally {
      await client.close();
    }
  });

  test("peekLock abandon requeues the message", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("peek-abandon");
    await sendToQueue(connectionString, testQueue, [{ messageId: "peek-abandon-1", body: "payload" }]);

    const client = createClient(connectionString);
    try {
      const receiver = client.createReceiver(testQueue);
      const firstBatch = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });

      assert.equal(firstBatch.length, 1);
      await receiver.abandonMessage(firstBatch[0]);

      const secondBatch = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });
      assert.equal(secondBatch.length, 1);
      assert.equal(secondBatch[0].messageId, "peek-abandon-1");

      await receiver.close();
    } finally {
      await client.close();
    }
  });

  test("peekLock deadLetter moves message to DLQ", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("peek-dlq-explicit");
    const deadLetterQueue = `${testQueue}/$DeadLetterQueue`;
    await sendToQueue(connectionString, testQueue, [{ messageId: "peek-dlq-explicit-1", body: "payload" }]);

    const deadLetterClient = createClient(connectionString);
    try {
      const receiver = deadLetterClient.createReceiver(testQueue);
      const firstBatch = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });

      assert.equal(firstBatch.length, 1);
      assert.equal(firstBatch[0].messageId, "peek-dlq-explicit-1");
      await receiver.deadLetterMessage(firstBatch[0]);
      await receiver.close();
    } finally {
      await deadLetterClient.close();
    }

    const verifyClient = createClient(connectionString);
    try {
      const dlqReceiver = verifyClient.createReceiver(deadLetterQueue, { receiveMode: "receiveAndDelete" });
      const deadLettered = await dlqReceiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });
      assert.equal(deadLettered.length, 1);
      assert.equal(deadLettered[0].messageId, "peek-dlq-explicit-1");
      assert.equal(deadLettered[0].body, "payload");
      assert.equal(deadLettered[0].applicationProperties?.DeadLetterReason, "DeadLettered");
      await dlqReceiver.close();
    } finally {
      await verifyClient.close();
    }
  });

  test("max delivery count exceeded moves message to DLQ", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("peek-dlq-max");
    const deadLetterQueue = `${testQueue}/$DeadLetterQueue`;
    await sendToQueue(connectionString, testQueue, [{ messageId: "peek-dlq-max-1", body: "payload" }]);

    for (let attempt = 0; attempt < 3; attempt += 1) {
      const attemptClient = createClient(connectionString);
      try {
        const receiver = attemptClient.createReceiver(testQueue);
        const batch = await receiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
        assert.equal(batch.length, 1);
        assert.equal(batch[0].messageId, "peek-dlq-max-1");
        await receiver.abandonMessage(batch[0]);
      } finally {
        await attemptClient.close();
      }
    }

    const mainQueueClient = createClient(connectionString);
    try {
      const queueReceiver = mainQueueClient.createReceiver(testQueue, { receiveMode: "receiveAndDelete" });
      const fromMainQueue = await queueReceiver.receiveMessages(1, { maxWaitTimeInMs: 1000 });
      assert.equal(fromMainQueue.length, 0);
      await queueReceiver.close();
    } finally {
      await mainQueueClient.close();
    }

    const dlqClient = createClient(connectionString);
    try {
      const dlqReceiver = dlqClient.createReceiver(deadLetterQueue, { receiveMode: "receiveAndDelete" });
      const deadLettered = await dlqReceiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
      assert.equal(deadLettered.length, 1);
      assert.equal(deadLettered[0].messageId, "peek-dlq-max-1");
      assert.equal(deadLettered[0].body, "payload");
      await dlqReceiver.close();
    } finally {
      await dlqClient.close();
    }
  });

  test("peekLock lock expiry causes redelivery", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("peek-expiry");
    await sendToQueue(connectionString, testQueue, [{ messageId: "peek-expiry-1", body: "payload" }]);

    const firstClient = createClient(connectionString);
    try {
      const receiver = firstClient.createReceiver(testQueue, { receiveMode: "peekLock" });
      const firstBatch = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });

      assert.equal(firstBatch.length, 1);
      assert.equal(firstBatch[0].messageId, "peek-expiry-1");

      await new Promise((resolve) => setTimeout(resolve, 3500));

      await receiver.close();
    } finally {
      await firstClient.close();
    }

    const secondClient = createClient(connectionString);
    try {
      const receiver = secondClient.createReceiver(testQueue, { receiveMode: "receiveAndDelete" });

      const secondBatch = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });
      assert.equal(secondBatch.length, 1);
      assert.equal(secondBatch[0].messageId, "peek-expiry-1");

      await receiver.close();
    } finally {
      await secondClient.close();
    }
  });

  test("close completes quickly with pending peekLock delivery", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("close-pending-lock");
    await sendToQueue(connectionString, testQueue, [{ messageId: "close-pending-lock-1", body: "payload" }]);

    const client = createClient(connectionString);
    try {
      const receiver = client.createReceiver(testQueue);
      const firstBatch = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });

      assert.equal(firstBatch.length, 1);

      await receiver.close();
    } finally {
      await client.close();
    }
  });

  test("completing an already-completed message throws", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("double-complete");
    await sendToQueue(connectionString, testQueue, [{ messageId: "dc-1", body: "payload" }]);

    const client = createClient(connectionString);
    try {
      const receiver = client.createReceiver(testQueue);
      const messages = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });

      assert.equal(messages.length, 1);
      await receiver.completeMessage(messages[0]);

      await assert.rejects(
        () => receiver.completeMessage(messages[0]),
        { message: /already settled|deleted/i },
      );

      const remaining = await receiver.receiveMessages(1, { maxWaitTimeInMs: 1000 });
      assert.equal(remaining.length, 0);

      await receiver.close();
    } finally {
      await client.close();
    }
  });

  test("abandoned message is redelivered to the same queue", { timeout: 20000 }, async () => {
    const testQueue = makeQueueName("redelivery");
    await sendToQueue(connectionString, testQueue, [{ messageId: "rd-1", body: "payload" }]);

    const client = createClient(connectionString);
    try {
      const receiver = client.createReceiver(testQueue);

      const firstBatch = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });
      assert.equal(firstBatch.length, 1);
      assert.equal(firstBatch[0].messageId, "rd-1");
      await receiver.abandonMessage(firstBatch[0]);

      const secondBatch = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });
      assert.equal(secondBatch.length, 1);
      assert.equal(secondBatch[0].messageId, "rd-1");
      await receiver.completeMessage(secondBatch[0]);

      const thirdBatch = await receiver.receiveMessages(1, { maxWaitTimeInMs: 1000 });
      assert.equal(thirdBatch.length, 0);

      await receiver.close();
    } finally {
      await client.close();
    }
  });
});