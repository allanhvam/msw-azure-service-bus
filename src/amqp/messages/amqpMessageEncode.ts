import {
  concatBytes,
  encodeBinary,
  encodeBoolean,
  encodeDescribedList,
  encodeNull,
  encodeString,
  encodeUInt,
} from "../protocol/primitives.js";

function encodeInt32(value: number): Uint8Array {
  if (value >= -128 && value <= 127) {
    return new Uint8Array([0x54, value & 0xff]);
  }

  return new Uint8Array([0x71, (value >>> 24) & 0xff, (value >>> 16) & 0xff, (value >>> 8) & 0xff, value & 0xff]);
}

function encodeFloat64(value: number): Uint8Array {
  const bytes = new Uint8Array(9);
  bytes[0] = 0x82;
  new DataView(bytes.buffer).setFloat64(1, value, false);
  return bytes;
}

function encodeTimestamp(ms: number): Uint8Array {
  const bytes = new Uint8Array(9);
  bytes[0] = 0x83;
  let remaining = BigInt(ms);
  if (remaining < 0n) {
    remaining = (1n << 64n) + remaining;
  }

  for (let index = 8; index >= 1; index -= 1) {
    bytes[index] = Number(remaining & 0xffn);
    remaining >>= 8n;
  }

  return bytes;
}

function encodeMapEntries(entries: Array<[string, unknown]>): Uint8Array {
  if (entries.length === 0) {
    return new Uint8Array([0xc1, 0x01, 0x00]);
  }

  const encodedEntries = entries.flatMap(([key, value]) => [encodeString(key), encodeAmqpValue(value)]);
  const body = concatBytes(...encodedEntries);
  const count = entries.length * 2;

  if (body.length <= 0xfe && count <= 0xff) {
    return concatBytes(new Uint8Array([0xc1, body.length + 1, count]), body);
  }

  return concatBytes(
    new Uint8Array([
      0xd1,
      ((body.length + 4) >>> 24) & 0xff,
      ((body.length + 4) >>> 16) & 0xff,
      ((body.length + 4) >>> 8) & 0xff,
      (body.length + 4) & 0xff,
      (count >>> 24) & 0xff,
      (count >>> 16) & 0xff,
      (count >>> 8) & 0xff,
      count & 0xff,
    ]),
    body,
  );
}

function encodeList(values: unknown[]): Uint8Array {
  if (values.length === 0) {
    return new Uint8Array([0x45]);
  }

  const encodedItems = values.map((value) => encodeAmqpValue(value));
  const body = concatBytes(...encodedItems);

  if (body.length <= 0xfe && values.length <= 0xff) {
    return concatBytes(new Uint8Array([0xc0, body.length + 1, values.length]), body);
  }

  const size = body.length + 4;
  return concatBytes(
    new Uint8Array([
      0xd0,
      (size >>> 24) & 0xff,
      (size >>> 16) & 0xff,
      (size >>> 8) & 0xff,
      size & 0xff,
      (values.length >>> 24) & 0xff,
      (values.length >>> 16) & 0xff,
      (values.length >>> 8) & 0xff,
      values.length & 0xff,
    ]),
    body,
  );
}

function encodeAmqpValue(value: unknown): Uint8Array {
  if (value === null || value === undefined) {
    return encodeNull();
  }

  if (typeof value === "boolean") {
    return encodeBoolean(value);
  }

  if (typeof value === "string") {
    return encodeString(value);
  }

  if (typeof value === "number") {
    if (Number.isInteger(value)) {
      if (value >= 0) {
        return encodeUInt(value);
      }

      return encodeInt32(value);
    }

    return encodeFloat64(value);
  }

  if (typeof value === "bigint") {
    if (value === 0n) {
      return new Uint8Array([0x44]);
    }

    if (value > 0n && value <= 0xffn) {
      return new Uint8Array([0x53, Number(value)]);
    }

    if (value > 0n) {
      const bytes = new Uint8Array(9);
      bytes[0] = 0x80;
      let remaining = value;

      for (let index = 8; index >= 1; index -= 1) {
        bytes[index] = Number(remaining & 0xffn);
        remaining >>= 8n;
      }

      return bytes;
    }

    const bytes = new Uint8Array(9);
    bytes[0] = 0x81;
    let remaining = value;
    if (value < 0n) {
      remaining = (1n << 64n) + value;
    }

    for (let index = 8; index >= 1; index -= 1) {
      bytes[index] = Number(remaining & 0xffn);
      remaining >>= 8n;
    }

    return bytes;
  }

  if (value instanceof Uint8Array) {
    return encodeBinary(value);
  }

  if (Array.isArray(value)) {
    return encodeList(value);
  }

  if (value instanceof Date) {
    return encodeTimestamp(value.getTime());
  }

  if (typeof value === "object") {
    return encodeMapEntries(Object.entries(value as Record<string, unknown>));
  }

  return encodeString(String(value));
}

function encodeMessageWithSections(message: {
  message_id?: unknown;
  correlation_id?: unknown;
  content_type?: unknown;
  absolute_expiry_time?: number;
  creation_time?: number;
  group_id?: string;
  body?: unknown;
  message_annotations?: Record<string, unknown>;
  application_properties?: Record<string, unknown>;
}): Uint8Array {
  const sections: Uint8Array[] = [];

  const encodeDescribedValue = (descriptorCode: number, value: Uint8Array): Uint8Array => {
    return concatBytes(new Uint8Array([0x00, 0x53, descriptorCode]), value);
  };

  if (message.message_annotations && Object.keys(message.message_annotations).length > 0) {
    sections.push(encodeDescribedValue(0x72, encodeMapEntries(Object.entries(message.message_annotations))));
  }

  const hasProperties =
    message.message_id !== undefined
    || message.correlation_id !== undefined
    || message.content_type !== undefined
    || message.absolute_expiry_time !== undefined
    || message.creation_time !== undefined
    || message.group_id !== undefined;

  if (hasProperties) {
    sections.push(encodeDescribedList(0x73, [
      encodeAmqpValue(message.message_id ?? null),        // [0] message-id
      encodeNull(),                                         // [1] user-id
      encodeNull(),                                         // [2] to
      encodeNull(),                                         // [3] subject
      encodeNull(),                                         // [4] reply-to
      encodeAmqpValue(message.correlation_id ?? null),      // [5] correlation-id
      encodeAmqpValue(message.content_type ?? null),        // [6] content-type
      encodeNull(),                                         // [7] content-encoding
      message.absolute_expiry_time !== undefined            // [8] absolute-expiry-time
        ? encodeTimestamp(message.absolute_expiry_time)
        : encodeNull(),
      message.creation_time !== undefined                   // [9] creation-time
        ? encodeTimestamp(message.creation_time)
        : encodeNull(),
      message.group_id !== undefined                        // [10] group-id
        ? encodeAmqpValue(message.group_id)
        : encodeNull(),
    ]));
  }

  if (message.application_properties && Object.keys(message.application_properties).length > 0) {
    sections.push(encodeDescribedValue(0x74, encodeMapEntries(Object.entries(message.application_properties))));
  }

  if (message.body !== undefined) {
    sections.push(encodeDescribedValue(0x77, encodeAmqpValue(message.body)));
  }

  return concatBytes(...sections);
}

export function encodeCbsResponseMessage(correlationId: unknown): Uint8Array {
  const cbsMessage = {
    correlation_id: correlationId,
    application_properties: {
      "status-code": 202,
      "status-description": "Accepted",
    },
    body: "cbs-ok",
  };

  return encodeMessageWithSections(cbsMessage);
}

export function encodeManagementResponseMessage(correlationId: unknown): Uint8Array {
  return encodeMessageWithSections({
    correlation_id: correlationId,
    application_properties: {
      "statusCode": 200,
      "status-code": 200,
      "statusDescription": "OK",
      "status-description": "OK",
    },
    body: null,
  });
}

export function encodeAmqpMessage(message: {
  messageId: string;
  body: unknown;
  contentType?: string;
  correlationId?: string;
  sessionId?: string;
  timeToLive?: number;
  messageAnnotations?: Record<string, unknown>;
  applicationProperties?: Record<string, unknown>;
}): Uint8Array {
  // Compute absolute-expiry-time and creation-time from TTL
  let absoluteExpiryTime: number | undefined;
  let creationTime: number | undefined;
  if (typeof message.timeToLive === "number" && message.timeToLive > 0) {
    creationTime = Date.now();
    absoluteExpiryTime = creationTime + message.timeToLive;
  }

  return encodeMessageWithSections({
    message_id: message.messageId,
    correlation_id: message.correlationId,
    content_type: message.contentType,
    group_id: message.sessionId,
    absolute_expiry_time: absoluteExpiryTime,
    creation_time: creationTime,
    body: message.body,
    message_annotations: message.messageAnnotations,
    application_properties: message.applicationProperties,
  });
}
