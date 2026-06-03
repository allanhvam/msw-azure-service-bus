import type { ParsedAttach } from "../types/protocol.js";
import {
  concatBytes,
  encodeAmqpFrame,
  encodeBinary,
  encodeBoolean,
  encodeDescribedList,
  encodeNull,
  encodeString,
  encodeSymbol,
  encodeSymbolArray,
  encodeUByte,
  encodeUInt,
  encodeUShort,
  toArrayBuffer,
} from "../codec.js";
import { encodeCbsResponseMessage, encodeManagementResponseMessage } from "../messages/amqpValidation.js";
import type { ClientConnection } from "./client.js";

const textEncoder = new TextEncoder();

const CONTAINER_ID = "msw-servicebus-emulator";

export function sendBinary(client: ClientConnection, bytes: Uint8Array): void {
  client.send(toArrayBuffer(bytes));
}

export function sendOpen(client: ClientConnection, channel: number): void {
  const performative = encodeDescribedList(0x10, [
    encodeString(CONTAINER_ID),
    encodeNull(),
    encodeUInt(262144),
    encodeUShort(65535),
  ]);
  sendBinary(client, encodeAmqpFrame(channel, performative));
}

export function sendBegin(client: ClientConnection, channel: number): void {
  const performative = encodeDescribedList(0x11, [encodeUInt(channel), encodeUInt(1), encodeUInt(5000), encodeUInt(5000)]);
  sendBinary(client, encodeAmqpFrame(channel, performative));
}

export function sendAttach(client: ClientConnection, channel: number): void {
  const performative = encodeDescribedList(0x12, [encodeString("msw-link"), encodeUInt(0), encodeBoolean(true)]);
  sendBinary(client, encodeAmqpFrame(channel, performative));
}

export function sendFlow(client: ClientConnection, channel: number): void {
  const performative = encodeDescribedList(0x13, [encodeUInt(1), encodeUInt(5000), encodeUInt(1), encodeUInt(5000)]);
  sendBinary(client, encodeAmqpFrame(channel, performative));
}

export function sendLinkFlow(client: ClientConnection, channel: number, handle: number, deliveryCount: number, linkCredit: number): void {
  const performative = encodeDescribedList(0x13, [
    encodeUInt(1),
    encodeUInt(5000),
    encodeUInt(1),
    encodeUInt(5000),
    encodeUInt(handle),
    encodeUInt(deliveryCount),
    encodeUInt(linkCredit),
  ]);
  sendBinary(client, encodeAmqpFrame(channel, performative));
}

export function sendDisposition(client: ClientConnection, channel: number, deliveryId: number, role: boolean): void {
  const acceptedState = encodeDescribedList(0x24, []);
  const performative = encodeDescribedList(0x15, [
    encodeBoolean(role),
    encodeUInt(deliveryId),
    encodeUInt(deliveryId),
    encodeBoolean(true),
    acceptedState,
  ]);
  sendBinary(client, encodeAmqpFrame(channel, performative));
}

export function sendDispositionAck(client: ClientConnection, channel: number, first: number, last: number): void {
  const performative = encodeDescribedList(0x15, [
    encodeBoolean(false),
    encodeUInt(first),
    encodeUInt(last),
    encodeBoolean(true),
  ]);

  sendBinary(client, encodeAmqpFrame(channel, performative));
}

export function sendDispositionReleased(client: ClientConnection, channel: number, deliveryId: number): void {
  const releasedState = encodeDescribedList(0x26, []);
  const performative = encodeDescribedList(0x15, [
    encodeBoolean(false),
    encodeUInt(deliveryId),
    encodeUInt(deliveryId),
    encodeBoolean(true),
    releasedState,
  ]);

  sendBinary(client, encodeAmqpFrame(channel, performative));
}

export function sendDispositionRejected(
  client: ClientConnection,
  channel: number,
  deliveryId: number,
  condition: string,
  description?: string,
): void {
  const errorFields: Uint8Array[] = [encodeSymbol(condition)];
  if (description) {
    errorFields.push(encodeString(description));
  }

  const rejectedState = encodeDescribedList(0x25, [encodeDescribedList(0x1d, errorFields)]);
  const performative = encodeDescribedList(0x15, [
    encodeBoolean(true),
    encodeUInt(deliveryId),
    encodeUInt(deliveryId),
    encodeBoolean(true),
    rejectedState,
  ]);

  sendBinary(client, encodeAmqpFrame(channel, performative));
}

export function sendClose(
  client: ClientConnection,
  channel: number,
  error?: { condition: string; description?: string },
): void {
  const fields = [] as Uint8Array[];

  if (error) {
    const errorFields: Uint8Array[] = [encodeSymbol(error.condition)];
    if (error.description) {
      errorFields.push(encodeString(error.description));
    }

    fields.push(encodeDescribedList(0x1d, errorFields));
  }

  const performative = encodeDescribedList(0x18, fields);
  sendBinary(client, encodeAmqpFrame(channel, performative));
}

export function sendDetach(client: ClientConnection, channel: number, handle: number, closed: boolean): void {
  const performative = encodeDescribedList(0x16, [encodeUInt(handle), encodeBoolean(closed)]);
  sendBinary(client, encodeAmqpFrame(channel, performative));
}

export function sendEnd(client: ClientConnection, channel: number): void {
  const performative = encodeDescribedList(0x17, []);
  sendBinary(client, encodeAmqpFrame(channel, performative));
}

export function sendEndWithError(
  client: ClientConnection,
  channel: number,
  condition: string,
  description?: string,
): void {
  const errorFields: Uint8Array[] = [encodeSymbol(condition)];
  if (description) {
    errorFields.push(encodeString(description));
  }

  const errorValue = encodeDescribedList(0x1d, errorFields);
  const performative = encodeDescribedList(0x17, [errorValue]);
  sendBinary(client, encodeAmqpFrame(channel, performative));
}

export function sendSaslMechanisms(client: ClientConnection): void {
  const performative = encodeDescribedList(0x40, [encodeSymbolArray(["ANONYMOUS", "PLAIN"])]);
  sendBinary(client, encodeAmqpFrame(0, performative, 1));
}

export function sendSaslOutcome(client: ClientConnection): void {
  const performative = encodeDescribedList(0x44, [encodeUByte(0)]);
  sendBinary(client, encodeAmqpFrame(0, performative, 1));
}

export function sendAttachResponse(client: ClientConnection, channel: number, attach: ParsedAttach): void {
  const handleValue = attach.handle ?? 0;
  const responseRole = !(attach.role ?? false);

  // Source always carries the queue address regardless of role
  const sourceAddress = responseRole ? attach.targetAddress : attach.sourceAddress;
  const targetAddress = responseRole ? undefined : attach.targetAddress;

  const defaultReleasedOutcome = encodeDescribedList(0x26, []);
  const supportedOutcomes = encodeSymbolArray([
    "amqp:accepted:list",
    "amqp:rejected:list",
    "amqp:released:list",
    "amqp:modified:list",
  ]);

  const source = sourceAddress
    ? encodeDescribedList(0x28, [
      encodeString(sourceAddress),
      encodeNull(),
      encodeSymbol("session-end"),
      encodeUInt(0),
      encodeBoolean(false),
      encodeNull(),
      encodeNull(),
      encodeNull(),
      defaultReleasedOutcome,
      supportedOutcomes,
      encodeNull(),
    ])
    : encodeNull();

  const target = targetAddress
    ? encodeDescribedList(0x29, [
      encodeString(targetAddress),
      encodeNull(),
      encodeSymbol("session-end"),
      encodeUInt(0),
      encodeBoolean(false),
      encodeNull(),
      encodeNull(),
    ])
    : encodeNull();

  const performative = encodeDescribedList(0x12, [
    encodeString(attach.name ?? "msw-link"),
    encodeUInt(handleValue),
    encodeBoolean(responseRole),
    encodeUByte(0),
    encodeUByte(0),
    source,
    target,
  ]);

  sendBinary(client, encodeAmqpFrame(channel, performative));
}

function sendResponseTransfer(
  client: ClientConnection,
  channel: number,
  senderHandle: number,
  deliveryId: number,
  tagPrefix: string,
  encodedMessage: Uint8Array,
): void {
  const deliveryTag = textEncoder.encode(`${tagPrefix}-${deliveryId}`);

  const performative = encodeDescribedList(0x14, [
    encodeUInt(senderHandle),
    encodeUInt(deliveryId),
    encodeBinary(deliveryTag),
    encodeUInt(0),
    encodeBoolean(true),
    encodeBoolean(false),
    encodeNull(),
    encodeNull(),
    encodeBoolean(false),
    encodeBoolean(false),
    encodeBoolean(false),
  ]);

  sendBinary(client, encodeAmqpFrame(channel, concatBytes(performative, encodedMessage)));
}

export function sendCbsResponseTransfer(
  client: ClientConnection,
  channel: number,
  senderHandle: number,
  deliveryId: number,
  correlationId: unknown,
): void {
  sendResponseTransfer(client, channel, senderHandle, deliveryId, "cbs", encodeCbsResponseMessage(correlationId));
}

export function sendManagementResponseTransfer(
  client: ClientConnection,
  channel: number,
  senderHandle: number,
  deliveryId: number,
  correlationId: unknown,
): void {
  sendResponseTransfer(client, channel, senderHandle, deliveryId, "mgmt", encodeManagementResponseMessage(correlationId));
}
