import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  encodeAmqpFrame,
  encodeAmqpMessage,
  encodeBinary,
  encodeBoolean,
  encodeDescribedList,
  encodeNull,
  encodeString,
  encodeUByte,
  encodeUInt,
  parseFlowFrame,
  parseTransferFrame,
} from "../amqp/codec.js";
import { AmqpProtocolEmulator } from "../amqp/emulator.js";
import { parseAmqpFrame } from "../amqp/frame.js";

function createProtocolHeader(protocolId: number, major: number, minor: number, revision: number): Uint8Array {
  return new Uint8Array([0x41, 0x4d, 0x51, 0x50, protocolId, major, minor, revision]);
}

function createOpenFrame(): Uint8Array {
  return encodeAmqpFrame(0, encodeDescribedList(0x10, [encodeString("phase2-client")]));
}

function createBeginFrame(channel: number): Uint8Array {
  return encodeAmqpFrame(channel, encodeDescribedList(0x11, [encodeUInt(channel), encodeUInt(1), encodeUInt(5000), encodeUInt(5000)]));
}

function createAttachSenderFrame(channel: number, handle: number, queueName: string): Uint8Array {
  const target = encodeDescribedList(0x29, [encodeString(queueName), encodeNull(), encodeNull(), encodeNull(), encodeBoolean(false)]);
  return encodeAmqpFrame(
    channel,
    encodeDescribedList(0x12, [
      encodeString(`sender-${queueName}`),
      encodeUInt(handle),
      encodeBoolean(false),
      encodeUByte(0),
      encodeUByte(0),
      encodeNull(),
      target,
    ]),
  );
}

function createAttachReceiverFrame(channel: number, handle: number, queueName: string, rcvSettleMode = 0): Uint8Array {
  const source = encodeDescribedList(0x28, [encodeString(queueName), encodeNull(), encodeNull(), encodeNull(), encodeBoolean(false)]);
  return encodeAmqpFrame(
    channel,
    encodeDescribedList(0x12, [
      encodeString(`receiver-${queueName}`),
      encodeUInt(handle),
      encodeBoolean(true),
      encodeUByte(0),
      encodeUByte(rcvSettleMode),
      source,
      encodeNull(),
    ]),
  );
}

function createDispositionFrame(options: {
  channel: number;
  role: boolean;
  first: number;
  last?: number;
  settled?: boolean;
  state?: "accepted" | "released" | "modified" | "rejected";
}): Uint8Array {
  const stateValue = (() => {
    if (!options.state) {
      return encodeNull();
    }

    const descriptor = options.state === "accepted"
      ? 0x24
      : options.state === "rejected"
        ? 0x25
        : options.state === "released"
          ? 0x26
          : 0x27;

    return new Uint8Array([0x00, 0x53, descriptor, 0x45]);
  })();

  return encodeAmqpFrame(
    options.channel,
    encodeDescribedList(0x15, [
      encodeBoolean(options.role),
      encodeUInt(options.first),
      options.last === undefined ? encodeUInt(options.first) : encodeUInt(options.last),
      encodeBoolean(options.settled ?? true),
      stateValue,
    ]),
  );
}

function getFirstTransferDeliveryId(frames: Uint8Array[]): number | undefined {
  for (const bytes of frames) {
    const frame = parseAmqpFrame(bytes);
    if (!frame || frame.performative !== "transfer") {
      continue;
    }

    const transfer = parseTransferFrame(frame.body);
    if (typeof transfer?.deliveryId === "number") {
      return transfer.deliveryId;
    }
  }

  return undefined;
}

function containsAscii(bytes: Uint8Array, text: string): boolean {
  const value = new TextDecoder().decode(bytes);
  return value.includes(text);
}

function getFirstAttachFrameBody(frames: Uint8Array[]): Uint8Array | undefined {
  for (const bytes of frames) {
    const frame = parseAmqpFrame(bytes);
    if (frame?.performative === "attach") {
      return frame.body;
    }
  }

  return undefined;
}

function createFlowFrame(options: {
  channel: number;
  nextIncomingId?: number;
  incomingWindow?: number;
  nextOutgoingId?: number;
  outgoingWindow?: number;
  handle?: number;
  deliveryCount?: number;
  linkCredit?: number;
  drain?: boolean;
  echo?: boolean;
}): Uint8Array {
  return encodeAmqpFrame(
    options.channel,
    encodeDescribedList(0x13, [
      options.nextIncomingId === undefined ? encodeUInt(1) : encodeUInt(options.nextIncomingId),
      options.incomingWindow === undefined ? encodeUInt(5000) : encodeUInt(options.incomingWindow),
      options.nextOutgoingId === undefined ? encodeUInt(1) : encodeUInt(options.nextOutgoingId),
      options.outgoingWindow === undefined ? encodeUInt(5000) : encodeUInt(options.outgoingWindow),
      options.handle === undefined ? encodeNull() : encodeUInt(options.handle),
      options.deliveryCount === undefined ? encodeNull() : encodeUInt(options.deliveryCount),
      options.linkCredit === undefined ? encodeNull() : encodeUInt(options.linkCredit),
      encodeNull(),
      options.drain === undefined ? encodeNull() : encodeBoolean(options.drain),
      options.echo === undefined ? encodeNull() : encodeBoolean(options.echo),
    ]),
  );
}

function createTransferFrame(options: {
  channel: number;
  handle: number;
  deliveryId: number;
  deliveryTag: string;
  payload?: Uint8Array;
  settled?: boolean;
  more?: boolean;
  resume?: boolean;
  aborted?: boolean;
}): Uint8Array {
  const fields = [
    encodeUInt(options.handle),
    encodeUInt(options.deliveryId),
    encodeBinary(new TextEncoder().encode(options.deliveryTag)),
    encodeUInt(0),
    encodeBoolean(options.settled ?? false),
    encodeBoolean(options.more ?? false),
    encodeNull(),
    encodeNull(),
    encodeBoolean(options.resume ?? false),
    encodeBoolean(options.aborted ?? false),
    encodeBoolean(false),
  ];

  const performative = encodeDescribedList(0x14, fields);
  const payload = options.payload ?? new Uint8Array();
  return encodeAmqpFrame(options.channel, payload.length > 0 ? new Uint8Array([...performative, ...payload]) : performative);
}

function createBareSection(descriptorCode: number, value: Uint8Array): Uint8Array {
  return new Uint8Array([0x00, 0x53, descriptorCode, ...value]);
}

function createMalformedBareMessageOutOfOrder(): Uint8Array {
  const properties = createBareSection(0x73, encodeDescribedList(0x73, []));
  const messageAnnotations = createBareSection(0x72, encodeDescribedList(0x72, []));
  const body = createBareSection(0x77, encodeString("payload"));
  return new Uint8Array([...properties, ...messageAnnotations, ...body]);
}

function createMalformedBareMessageMixedBodyTypes(): Uint8Array {
  const dataSection = createBareSection(0x75, encodeBinary(new Uint8Array([0x01, 0x02])));
  const valueSection = createBareSection(0x77, encodeString("value"));
  return new Uint8Array([...dataSection, ...valueSection]);
}

function createUnsupportedBareMessageDescriptor(): Uint8Array {
  const unknownSection = createBareSection(0x79, encodeNull());
  const body = createBareSection(0x77, encodeString("payload"));
  return new Uint8Array([...unknownSection, ...body]);
}

function createMalformedApplicationPropertiesMessage(): Uint8Array {
  const invalidAppProps = createBareSection(0x74, encodeString("not-a-map"));
  const body = createBareSection(0x77, encodeString("payload"));
  return new Uint8Array([...invalidAppProps, ...body]);
}

function createStubClient(sentFrames: Uint8Array[]): { send: (data: ArrayBuffer) => void; close: () => void } {
  return {
    send: (data: ArrayBuffer) => {
      sentFrames.push(new Uint8Array(data));
    },
    close: () => {
      return;
    },
  };
}

function bootstrapSenderLink(params: {
  emulator: AmqpProtocolEmulator;
  connectionId: string;
  client: { send: (data: ArrayBuffer) => void; close: () => void };
  channel: number;
  handle: number;
  queueName: string;
}): void {
  params.emulator.createConnection(params.connectionId);

  params.emulator.handleMessage(params.connectionId, params.client, createProtocolHeader(0x00, 1, 0, 0), undefined);
  params.emulator.handleMessage(params.connectionId, params.client, undefined, parseAmqpFrame(createOpenFrame()));
  params.emulator.handleMessage(params.connectionId, params.client, undefined, parseAmqpFrame(createBeginFrame(params.channel)));
  params.emulator.handleMessage(
    params.connectionId,
    params.client,
    undefined,
    parseAmqpFrame(createAttachSenderFrame(params.channel, params.handle, params.queueName)),
  );
}

function bootstrapReceiverLink(params: {
  emulator: AmqpProtocolEmulator;
  connectionId: string;
  client: { send: (data: ArrayBuffer) => void; close: () => void };
  channel: number;
  handle: number;
  queueName: string;
  receiveMode?: "receiveAndDelete" | "peekLock";
}): void {
  params.emulator.createConnection(params.connectionId);

  params.emulator.handleMessage(params.connectionId, params.client, createProtocolHeader(0x00, 1, 0, 0), undefined);
  params.emulator.handleMessage(params.connectionId, params.client, undefined, parseAmqpFrame(createOpenFrame()));
  params.emulator.handleMessage(params.connectionId, params.client, undefined, parseAmqpFrame(createBeginFrame(params.channel)));
  params.emulator.handleMessage(
    params.connectionId,
    params.client,
    undefined,
    parseAmqpFrame(
      createAttachReceiverFrame(
        params.channel,
        params.handle,
        params.queueName,
        params.receiveMode === "peekLock" ? 1 : 0,
      ),
    ),
  );
}

describe("AMQP emulator protocol conformance", () => {
  test("unsupported AMQP version is rejected deterministically", { timeout: 20000 }, () => {
    const emulator = new AmqpProtocolEmulator(new Map());
    emulator.createConnection("phase1-version");

    const sentFrames: Uint8Array[] = [];
    let didClose = false;
    const client = {
      send: (data: ArrayBuffer) => {
        sentFrames.push(new Uint8Array(data));
      },
      close: () => {
        didClose = true;
      },
    };

    emulator.handleMessage("phase1-version", client, createProtocolHeader(0x00, 1, 1, 0), undefined);

    assert.equal(sentFrames.length, 1);
    assert.deepEqual(Array.from(sentFrames[0]), Array.from(createProtocolHeader(0x00, 1, 0, 0)));
    assert.equal(didClose, true);
  });

  test("begin before open triggers close performative", { timeout: 20000 }, () => {
    const emulator = new AmqpProtocolEmulator(new Map());
    emulator.createConnection("phase1-order");

    const sentFrames: Uint8Array[] = [];
    const client = {
      send: (data: ArrayBuffer) => {
        sentFrames.push(new Uint8Array(data));
      },
    };

    emulator.handleMessage("phase1-order", client, createProtocolHeader(0x00, 1, 0, 0), undefined);

    const beginWithoutOpenFrame = encodeAmqpFrame(
      1,
      encodeDescribedList(0x11, [encodeUInt(1), encodeUInt(1), encodeUInt(5000), encodeUInt(5000)]),
    );
    const parsedBegin = parseAmqpFrame(beginWithoutOpenFrame);
    assert.ok(parsedBegin);

    emulator.handleMessage("phase1-order", client, undefined, parsedBegin);

    const closeResponse = sentFrames
      .map((frameBytes) => parseAmqpFrame(frameBytes))
      .find((frame) => frame?.performative === "close");

    assert.ok(closeResponse);
  });

  test("fragmented transfer chains are assembled into one queued message", { timeout: 20000 }, () => {
    const queues = new Map<string, Array<{ messageId: string; body: unknown; deliveryCount: number }>>();
    const emulator = new AmqpProtocolEmulator(queues as Map<string, never[]>);
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);
    const queueName = "phase2-fragment";

    bootstrapSenderLink({ emulator, connectionId: "phase2-fragment", client, channel: 1, handle: 0, queueName });

    const encodedMessage = encodeAmqpMessage({ messageId: "frag-1", body: "fragmented-body" });
    const split = Math.max(1, Math.floor(encodedMessage.length / 2));
    const firstChunk = encodedMessage.slice(0, split);
    const secondChunk = encodedMessage.slice(split);

    emulator.handleMessage(
      "phase2-fragment",
      client,
      undefined,
      parseAmqpFrame(createTransferFrame({ channel: 1, handle: 0, deliveryId: 1, deliveryTag: "frag-tag", payload: firstChunk, more: true })),
    );
    emulator.handleMessage(
      "phase2-fragment",
      client,
      undefined,
      parseAmqpFrame(createTransferFrame({ channel: 1, handle: 0, deliveryId: 1, deliveryTag: "frag-tag", payload: secondChunk, more: false })),
    );

    const queue = queues.get(queueName) ?? [];
    assert.equal(queue.length, 1);
    assert.equal(queue[0].body, "fragmented-body");
  });

  test("aborted transfer discards payload and does not enqueue", { timeout: 20000 }, () => {
    const queues = new Map<string, Array<{ messageId: string; body: unknown; deliveryCount: number }>>();
    const emulator = new AmqpProtocolEmulator(queues as Map<string, never[]>);
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);
    const queueName = "phase2-abort";

    bootstrapSenderLink({ emulator, connectionId: "phase2-abort", client, channel: 1, handle: 0, queueName });

    const encodedMessage = encodeAmqpMessage({ messageId: "abort-1", body: "must-not-arrive" });
    const firstChunk = encodedMessage.slice(0, Math.max(1, Math.floor(encodedMessage.length / 2)));

    emulator.handleMessage(
      "phase2-abort",
      client,
      undefined,
      parseAmqpFrame(createTransferFrame({ channel: 1, handle: 0, deliveryId: 2, deliveryTag: "abort-tag", payload: firstChunk, more: true })),
    );
    emulator.handleMessage(
      "phase2-abort",
      client,
      undefined,
      parseAmqpFrame(createTransferFrame({ channel: 1, handle: 0, deliveryId: 2, deliveryTag: "abort-tag", aborted: true, more: false })),
    );

    const queue = queues.get(queueName) ?? [];
    assert.equal(queue.length, 0);
  });

  test("resume transfer is ignored when unsettled delivery is unknown", { timeout: 20000 }, () => {
    const queues = new Map<string, Array<{ messageId: string; body: unknown; deliveryCount: number }>>();
    const emulator = new AmqpProtocolEmulator(queues as Map<string, never[]>);
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);
    const queueName = "phase2-resume";

    bootstrapSenderLink({ emulator, connectionId: "phase2-resume", client, channel: 1, handle: 0, queueName });

    const encodedMessage = encodeAmqpMessage({ messageId: "resume-1", body: "ignored" });

    emulator.handleMessage(
      "phase2-resume",
      client,
      undefined,
      parseAmqpFrame(createTransferFrame({
        channel: 1,
        handle: 0,
        deliveryId: 3,
        deliveryTag: "unknown-resume-tag",
        payload: encodedMessage,
        resume: true,
      })),
    );

    const queue = queues.get(queueName) ?? [];
    assert.equal(queue.length, 0);
  });

  test("attach with handle already in use ends session", { timeout: 20000 }, () => {
    const emulator = new AmqpProtocolEmulator(new Map());
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);

    bootstrapSenderLink({ emulator, connectionId: "phase3-handle-in-use", client, channel: 1, handle: 7, queueName: "phase3-handle" });

    emulator.handleMessage(
      "phase3-handle-in-use",
      client,
      undefined,
      parseAmqpFrame(createAttachSenderFrame(1, 7, "phase3-handle")),
    );

    const endResponse = sentFrames
      .map((frameBytes) => parseAmqpFrame(frameBytes))
      .find((frame) => frame?.performative === "end");

    assert.ok(endResponse);
  });

  test("flow with unattached handle ends session", { timeout: 20000 }, () => {
    const emulator = new AmqpProtocolEmulator(new Map());
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);

    emulator.createConnection("phase3-unattached");
    emulator.handleMessage("phase3-unattached", client, createProtocolHeader(0x00, 1, 0, 0), undefined);
    emulator.handleMessage("phase3-unattached", client, undefined, parseAmqpFrame(createOpenFrame()));
    emulator.handleMessage("phase3-unattached", client, undefined, parseAmqpFrame(createBeginFrame(2)));

    const flowWithUnknownHandle = encodeAmqpFrame(
      2,
      encodeDescribedList(0x13, [
        encodeUInt(1),
        encodeUInt(5000),
        encodeUInt(1),
        encodeUInt(5000),
        encodeUInt(999),
        encodeUInt(0),
        encodeUInt(1),
      ]),
    );

    emulator.handleMessage("phase3-unattached", client, undefined, parseAmqpFrame(flowWithUnknownHandle));

    const endResponse = sentFrames
      .map((frameBytes) => parseAmqpFrame(frameBytes))
      .find((frame) => frame?.performative === "end");

    assert.ok(endResponse);
  });

  test("drain consumes remaining credit when source has no messages", { timeout: 20000 }, () => {
    const emulator = new AmqpProtocolEmulator(new Map());
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);

    bootstrapReceiverLink({
      emulator,
      connectionId: "phase3-drain",
      client,
      channel: 4,
      handle: 2,
      queueName: "phase3-drain",
    });

    const baseline = sentFrames.length;
    emulator.handleMessage(
      "phase3-drain",
      client,
      undefined,
      parseAmqpFrame(createFlowFrame({ channel: 4, handle: 2, deliveryCount: 0, linkCredit: 3, drain: true })),
    );

    const responseFrames = sentFrames.slice(baseline).map((bytes) => parseAmqpFrame(bytes)).filter((frame) => frame?.performative === "flow");
    const linkFlow = responseFrames
      .map((frame) => (frame ? parseFlowFrame(frame.body) : undefined))
      .find((flow) => flow?.handle === 2 && flow.linkCredit === 0 && flow.deliveryCount === 3);

    assert.ok(linkFlow);
  });

  test("echo requests link flow state response", { timeout: 20000 }, () => {
    const emulator = new AmqpProtocolEmulator(new Map());
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);

    bootstrapReceiverLink({
      emulator,
      connectionId: "phase3-echo",
      client,
      channel: 5,
      handle: 3,
      queueName: "phase3-echo",
    });

    const baseline = sentFrames.length;
    emulator.handleMessage(
      "phase3-echo",
      client,
      undefined,
      parseAmqpFrame(createFlowFrame({ channel: 5, handle: 3, deliveryCount: 0, linkCredit: 0, echo: true })),
    );

    const responseFrames = sentFrames.slice(baseline).map((bytes) => parseAmqpFrame(bytes)).filter((frame) => frame?.performative === "flow");
    const linkFlow = responseFrames
      .map((frame) => (frame ? parseFlowFrame(frame.body) : undefined))
      .find((flow) => flow?.handle === 3);

    assert.ok(linkFlow);
  });

  test("transfer beyond remote outgoing window ends session", { timeout: 20000 }, () => {
    const emulator = new AmqpProtocolEmulator(new Map());
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);

    bootstrapSenderLink({ emulator, connectionId: "phase3-window", client, channel: 6, handle: 1, queueName: "phase3-window" });

    emulator.handleMessage(
      "phase3-window",
      client,
      undefined,
      parseAmqpFrame(createFlowFrame({ channel: 6, nextOutgoingId: 1, outgoingWindow: 0 })),
    );

    const baseline = sentFrames.length;
    const payload = encodeAmqpMessage({ messageId: "window-1", body: "blocked" });
    emulator.handleMessage(
      "phase3-window",
      client,
      undefined,
      parseAmqpFrame(createTransferFrame({ channel: 6, handle: 1, deliveryId: 10, deliveryTag: "window-tag", payload })),
    );

    const endResponse = sentFrames
      .slice(baseline)
      .map((frameBytes) => parseAmqpFrame(frameBytes))
      .find((parsed) => parsed?.performative === "end");

    assert.ok(endResponse);
  });

  test("transfer on receiver link is rejected", { timeout: 20000 }, () => {
    const emulator = new AmqpProtocolEmulator(new Map());
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);

    bootstrapReceiverLink({ emulator, connectionId: "phase3-role-transfer", client, channel: 7, handle: 2, queueName: "phase3-role-transfer" });

    const baseline = sentFrames.length;
    const payload = encodeAmqpMessage({ messageId: "role-1", body: "invalid" });
    emulator.handleMessage(
      "phase3-role-transfer",
      client,
      undefined,
      parseAmqpFrame(createTransferFrame({ channel: 7, handle: 2, deliveryId: 1, deliveryTag: "role-tag", payload })),
    );

    const endResponse = sentFrames
      .slice(baseline)
      .map((frameBytes) => parseAmqpFrame(frameBytes))
      .find((parsed) => parsed?.performative === "end");

    assert.ok(endResponse);
  });

  test("malformed bare message section order ends session", { timeout: 20000 }, () => {
    const queues = new Map<string, Array<{ messageId: string; body: unknown; deliveryCount: number }>>();
    const emulator = new AmqpProtocolEmulator(queues as Map<string, never[]>);
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);
    const queueName = "phase4-order";

    bootstrapSenderLink({ emulator, connectionId: "phase4-order", client, channel: 8, handle: 1, queueName });

    const baseline = sentFrames.length;
    emulator.handleMessage(
      "phase4-order",
      client,
      undefined,
      parseAmqpFrame(createTransferFrame({
        channel: 8,
        handle: 1,
        deliveryId: 1,
        deliveryTag: "phase4-order-tag",
        payload: createMalformedBareMessageOutOfOrder(),
      })),
    );

    const endResponse = sentFrames
      .slice(baseline)
      .map((frameBytes) => parseAmqpFrame(frameBytes))
      .find((parsed) => parsed?.performative === "end");

    assert.ok(endResponse);
    assert.equal((queues.get(queueName) ?? []).length, 0);
  });

  test("mixed AMQP body section types end session", { timeout: 20000 }, () => {
    const queues = new Map<string, Array<{ messageId: string; body: unknown; deliveryCount: number }>>();
    const emulator = new AmqpProtocolEmulator(queues as Map<string, never[]>);
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);
    const queueName = "phase4-body-mix";

    bootstrapSenderLink({ emulator, connectionId: "phase4-body-mix", client, channel: 9, handle: 2, queueName });

    const baseline = sentFrames.length;
    emulator.handleMessage(
      "phase4-body-mix",
      client,
      undefined,
      parseAmqpFrame(createTransferFrame({
        channel: 9,
        handle: 2,
        deliveryId: 2,
        deliveryTag: "phase4-body-mix-tag",
        payload: createMalformedBareMessageMixedBodyTypes(),
      })),
    );

    const endResponse = sentFrames
      .slice(baseline)
      .map((frameBytes) => parseAmqpFrame(frameBytes))
      .find((parsed) => parsed?.performative === "end");

    assert.ok(endResponse);
    assert.equal((queues.get(queueName) ?? []).length, 0);
  });

  test("unsupported bare section descriptor ends session", { timeout: 20000 }, () => {
    const queues = new Map<string, Array<{ messageId: string; body: unknown; deliveryCount: number }>>();
    const emulator = new AmqpProtocolEmulator(queues as Map<string, never[]>);
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);
    const queueName = "phase4-unsupported-section";

    bootstrapSenderLink({ emulator, connectionId: "phase4-unsupported-section", client, channel: 17, handle: 1, queueName });

    const baseline = sentFrames.length;
    emulator.handleMessage(
      "phase4-unsupported-section",
      client,
      undefined,
      parseAmqpFrame(createTransferFrame({
        channel: 17,
        handle: 1,
        deliveryId: 1,
        deliveryTag: "unsupported-section-tag",
        payload: createUnsupportedBareMessageDescriptor(),
      })),
    );

    const endResponse = sentFrames
      .slice(baseline)
      .map((frameBytes) => parseAmqpFrame(frameBytes))
      .find((parsed) => parsed?.performative === "end");

    assert.ok(endResponse);
    assert.equal((queues.get(queueName) ?? []).length, 0);
  });

  test("malformed application-properties metadata ends session", { timeout: 20000 }, () => {
    const queues = new Map<string, Array<{ messageId: string; body: unknown; deliveryCount: number }>>();
    const emulator = new AmqpProtocolEmulator(queues as Map<string, never[]>);
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);
    const queueName = "phase4-malformed-metadata";

    bootstrapSenderLink({ emulator, connectionId: "phase4-malformed-metadata", client, channel: 18, handle: 1, queueName });

    const baseline = sentFrames.length;
    emulator.handleMessage(
      "phase4-malformed-metadata",
      client,
      undefined,
      parseAmqpFrame(createTransferFrame({
        channel: 18,
        handle: 1,
        deliveryId: 1,
        deliveryTag: "malformed-metadata-tag",
        payload: createMalformedApplicationPropertiesMessage(),
      })),
    );

    const endResponse = sentFrames
      .slice(baseline)
      .map((frameBytes) => parseAmqpFrame(frameBytes))
      .find((parsed) => parsed?.performative === "end");

    assert.ok(endResponse);
    assert.equal((queues.get(queueName) ?? []).length, 0);
  });

  test("queue transfer accepts x-opt message annotations", { timeout: 20000 }, () => {
    const queues = new Map<string, Array<{ messageId: string; body: unknown; deliveryCount: number }>>();
    const emulator = new AmqpProtocolEmulator(queues as Map<string, never[]>);
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);
    const queueName = "phase4-annotations-allow";

    bootstrapSenderLink({ emulator, connectionId: "phase4-annotations-allow", client, channel: 10, handle: 1, queueName });

    const payload = encodeAmqpMessage({
      messageId: "annot-allow-1",
      body: "ok",
      messageAnnotations: {
        "x-opt-sequence-number": 1,
      },
    });

    emulator.handleMessage(
      "phase4-annotations-allow",
      client,
      undefined,
      parseAmqpFrame(createTransferFrame({
        channel: 10,
        handle: 1,
        deliveryId: 1,
        deliveryTag: "annot-allow-tag",
        payload,
      })),
    );

    const queue = queues.get(queueName) ?? [];
    assert.equal(queue.length, 1);
    assert.equal(queue[0].messageId, "annot-allow-1");
    assert.equal(queue[0].body, "ok");
  });

  test("queue transfer rejects non x-opt message annotations", { timeout: 20000 }, () => {
    const queues = new Map<string, Array<{ messageId: string; body: unknown; deliveryCount: number }>>();
    const emulator = new AmqpProtocolEmulator(queues as Map<string, never[]>);
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);
    const queueName = "phase4-annotations-reject";

    bootstrapSenderLink({ emulator, connectionId: "phase4-annotations-reject", client, channel: 11, handle: 1, queueName });

    const baseline = sentFrames.length;
    const payload = encodeAmqpMessage({
      messageId: "annot-reject-1",
      body: "bad",
      messageAnnotations: {
        custom: "not-allowed",
      },
    });

    emulator.handleMessage(
      "phase4-annotations-reject",
      client,
      undefined,
      parseAmqpFrame(createTransferFrame({
        channel: 11,
        handle: 1,
        deliveryId: 1,
        deliveryTag: "annot-reject-tag",
        payload,
      })),
    );

    const endResponse = sentFrames
      .slice(baseline)
      .map((frameBytes) => parseAmqpFrame(frameBytes))
      .find((parsed) => parsed?.performative === "end");

    assert.ok(endResponse);
    assert.equal((queues.get(queueName) ?? []).length, 0);
  });

  test("attach response source advertises default outcome and supported outcomes", { timeout: 20000 }, () => {
    const emulator = new AmqpProtocolEmulator(new Map());
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);

    bootstrapReceiverLink({
      emulator,
      connectionId: "phase4-attach-source-defaults",
      client,
      channel: 15,
      handle: 1,
      queueName: "phase4-attach-source-defaults",
      receiveMode: "peekLock",
    });

    const attachBody = getFirstAttachFrameBody(sentFrames);
    assert.ok(attachBody);
    assert.equal(containsAscii(attachBody as Uint8Array, "session-end"), true);
    assert.equal(containsAscii(attachBody as Uint8Array, "amqp:accepted:list"), true);
    assert.equal(containsAscii(attachBody as Uint8Array, "amqp:rejected:list"), true);
    assert.equal(containsAscii(attachBody as Uint8Array, "amqp:released:list"), true);
    assert.equal(containsAscii(attachBody as Uint8Array, "amqp:modified:list"), true);
    assert.ok((attachBody as Uint8Array).includes(0x26));
  });

  test("attach response target advertises session-end expiry default", { timeout: 20000 }, () => {
    const emulator = new AmqpProtocolEmulator(new Map());
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);

    bootstrapSenderLink({
      emulator,
      connectionId: "phase4-attach-target-defaults",
      client,
      channel: 16,
      handle: 1,
      queueName: "phase4-attach-target-defaults",
    });

    const attachBody = getFirstAttachFrameBody(sentFrames);
    assert.ok(attachBody);
    assert.equal(containsAscii(attachBody as Uint8Array, "session-end"), true);
  });

  test("accepted disposition removes peekLock delivery", { timeout: 20000 }, () => {
    const queues = new Map<string, Array<{ messageId: string; body: unknown; deliveryCount: number }>>();
    const queueName = "phase4-outcome-accepted";
    queues.set(queueName, [{ messageId: "accepted-1", body: "payload", deliveryCount: 0 }]);

    const emulator = new AmqpProtocolEmulator(queues as Map<string, never[]>);
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);

    bootstrapReceiverLink({
      emulator,
      connectionId: "phase4-outcome-accepted",
      client,
      channel: 12,
      handle: 1,
      queueName,
      receiveMode: "peekLock",
    });

    let baseline = sentFrames.length;
    emulator.handleMessage(
      "phase4-outcome-accepted",
      client,
      undefined,
      parseAmqpFrame(createFlowFrame({ channel: 12, handle: 1, deliveryCount: 0, linkCredit: 1 })),
    );

    const deliveryId = getFirstTransferDeliveryId(sentFrames.slice(baseline));
    assert.equal(typeof deliveryId, "number");

    baseline = sentFrames.length;
    emulator.handleMessage(
      "phase4-outcome-accepted",
      client,
      undefined,
      parseAmqpFrame(createDispositionFrame({ channel: 12, role: true, first: deliveryId as number, state: "accepted", settled: true })),
    );

    assert.equal((queues.get(queueName) ?? []).length, 0);

    emulator.handleMessage(
      "phase4-outcome-accepted",
      client,
      undefined,
      parseAmqpFrame(createFlowFrame({ channel: 12, handle: 1, deliveryCount: 1, linkCredit: 1 })),
    );

    const followUpDelivery = getFirstTransferDeliveryId(sentFrames.slice(baseline));
    assert.equal(followUpDelivery, undefined);
  });

  test("released then modified requeue delivery with incremented delivery-count", { timeout: 20000 }, () => {
    const queues = new Map<string, Array<{ messageId: string; body: unknown; deliveryCount: number }>>();
    const queueName = "phase4-outcome-requeue";
    queues.set(queueName, [{ messageId: "requeue-1", body: "payload", deliveryCount: 0 }]);

    const emulator = new AmqpProtocolEmulator(queues as Map<string, never[]>);
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);

    bootstrapReceiverLink({
      emulator,
      connectionId: "phase4-outcome-requeue",
      client,
      channel: 13,
      handle: 1,
      queueName,
      receiveMode: "peekLock",
    });

    emulator.handleMessage(
      "phase4-outcome-requeue",
      client,
      undefined,
      parseAmqpFrame(createFlowFrame({ channel: 13, handle: 1, deliveryCount: 0, linkCredit: 1 })),
    );

    const firstDeliveryId = getFirstTransferDeliveryId(sentFrames);
    assert.equal(typeof firstDeliveryId, "number");

    emulator.handleMessage(
      "phase4-outcome-requeue",
      client,
      undefined,
      parseAmqpFrame(createDispositionFrame({ channel: 13, role: true, first: firstDeliveryId as number, state: "released", settled: true })),
    );

    assert.equal((queues.get(queueName) ?? [])[0]?.deliveryCount, 1);

    const secondBaseline = sentFrames.length;
    emulator.handleMessage(
      "phase4-outcome-requeue",
      client,
      undefined,
      parseAmqpFrame(createFlowFrame({ channel: 13, handle: 1, deliveryCount: 1, linkCredit: 1 })),
    );

    const secondDeliveryId = getFirstTransferDeliveryId(sentFrames.slice(secondBaseline));
    assert.equal(typeof secondDeliveryId, "number");

    emulator.handleMessage(
      "phase4-outcome-requeue",
      client,
      undefined,
      parseAmqpFrame(createDispositionFrame({ channel: 13, role: true, first: secondDeliveryId as number, state: "modified", settled: true })),
    );

    assert.equal((queues.get(queueName) ?? [])[0]?.deliveryCount, 2);
  });

  test("rejected disposition dead-letters delivery", { timeout: 20000 }, () => {
    const queues = new Map<string, Array<{ messageId: string; body: unknown; deliveryCount: number; deadLetterReason?: string }>>();
    const queueName = "phase4-outcome-rejected";
    const deadLetterQueueName = `${queueName}/$DeadLetterQueue`;
    queues.set(queueName, [{ messageId: "reject-1", body: "payload", deliveryCount: 0 }]);

    const emulator = new AmqpProtocolEmulator(queues as Map<string, never[]>);
    const sentFrames: Uint8Array[] = [];
    const client = createStubClient(sentFrames);

    bootstrapReceiverLink({
      emulator,
      connectionId: "phase4-outcome-rejected",
      client,
      channel: 14,
      handle: 1,
      queueName,
      receiveMode: "peekLock",
    });

    emulator.handleMessage(
      "phase4-outcome-rejected",
      client,
      undefined,
      parseAmqpFrame(createFlowFrame({ channel: 14, handle: 1, deliveryCount: 0, linkCredit: 1 })),
    );

    const deliveryId = getFirstTransferDeliveryId(sentFrames);
    assert.equal(typeof deliveryId, "number");

    emulator.handleMessage(
      "phase4-outcome-rejected",
      client,
      undefined,
      parseAmqpFrame(createDispositionFrame({ channel: 14, role: true, first: deliveryId as number, state: "rejected", settled: true })),
    );

    const activeQueue = queues.get(queueName) ?? [];
    const deadLetterQueue = queues.get(deadLetterQueueName) ?? [];

    assert.equal(activeQueue.length, 0);
    assert.equal(deadLetterQueue.length, 1);
    assert.equal(deadLetterQueue[0].messageId, "reject-1");
    assert.equal(deadLetterQueue[0].deadLetterReason, "DeadLettered");
  });
});
