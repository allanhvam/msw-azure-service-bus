import type { AmqpFrame, AmqpPerformative, ParsedServiceBusAmqpRequest } from "./types/protocol.js";

export const AMQP_HEADER_PREFIX = new Uint8Array([0x41, 0x4d, 0x51, 0x50]);
export const AMQP_PROTOCOL_VERSION = { major: 1, minor: 0, revision: 0 };

export type AmqpProtocolHeader = {
  protocolId: number;
  major: number;
  minor: number;
  revision: number;
};

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function toHexPreview(bytes: Uint8Array, maxBytes = 24): string {
  return Array.from(bytes.slice(0, maxBytes), (value) => value.toString(16).padStart(2, "0")).join(" ");
}

export function isAmqpHeader(bytes: Uint8Array): boolean {
  if (bytes.length < 8) {
    return false;
  }

  return AMQP_HEADER_PREFIX.every((value, index) => bytes[index] === value);
}

export function parseAmqpProtocolHeader(bytes: Uint8Array): AmqpProtocolHeader | undefined {
  if (!isAmqpHeader(bytes)) {
    return undefined;
  }

  return {
    protocolId: bytes[4],
    major: bytes[5],
    minor: bytes[6],
    revision: bytes[7],
  };
}

export async function toBytes(data: unknown): Promise<Uint8Array | undefined> {
  if (typeof data === "string") {
    return textEncoder.encode(data);
  }

  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }

  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }

  if (data instanceof Blob) {
    const buffer = await data.arrayBuffer();
    return new Uint8Array(buffer);
  }

  return undefined;
}

function resolvePerformative(code: number | undefined): AmqpPerformative | undefined {
  switch (code) {
    case 0x40:
      return "sasl-mechanisms";
    case 0x41:
      return "sasl-init";
    case 0x42:
      return "sasl-challenge";
    case 0x43:
      return "sasl-response";
    case 0x44:
      return "sasl-outcome";
    case 0x10:
      return "open";
    case 0x11:
      return "begin";
    case 0x12:
      return "attach";
    case 0x13:
      return "flow";
    case 0x14:
      return "transfer";
    case 0x15:
      return "disposition";
    case 0x16:
      return "detach";
    case 0x17:
      return "end";
    case 0x18:
      return "close";
    default:
      return undefined;
  }
}

function findPerformativeCode(body: Uint8Array): number | undefined {
  if (body.length < 2 || body[0] !== 0x00) {
    return undefined;
  }

  // smallulong descriptor: 0x00 0x53 <code>
  if (body[1] === 0x53 && body.length >= 3) {
    return body[2];
  }

  // ulong descriptor: 0x00 0x80 <8 bytes>
  if (body[1] === 0x80 && body.length >= 10) {
    return body[9];
  }

  return undefined;
}

export function parseAmqpFrame(bytes: Uint8Array): AmqpFrame | undefined {
  if (bytes.length < 8) {
    return undefined;
  }

  const size = ((bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3]) >>> 0;
  const doff = bytes[4];
  const type = bytes[5];
  const channel = (bytes[6] << 8) | bytes[7];

  if (size < 8 || size > bytes.length || doff < 2) {
    return undefined;
  }

  const bodyStart = doff * 4;
  if (bodyStart > size) {
    return undefined;
  }

  const body = bytes.slice(bodyStart, size);
  const performativeCode = findPerformativeCode(body);

  return {
    size,
    doff,
    type,
    channel,
    body,
    performative: resolvePerformative(performativeCode),
  };
}

function decodePrintableText(bytes: Uint8Array): string | undefined {
  const decoded = textDecoder.decode(bytes);
  const segments = decoded
    // eslint-disable-next-line no-control-regex
    .split(/[\u0000-\u001f\u007f]+/)
    .map((segment) => segment.trim())
    .filter((segment) => segment.length > 0);

  if (segments.length === 0) {
    return undefined;
  }

  return segments[segments.length - 1];
}

function extractAmqpStrings(bytes: Uint8Array): string[] {
  const strings: string[] = [];
  let index = 0;

  while (index < bytes.length) {
    const formatCode = bytes[index];

    if (formatCode === 0xa1 || formatCode === 0xa3) {
      if (index + 1 >= bytes.length) {
        break;
      }

      const length = bytes[index + 1];
      const start = index + 2;
      const end = start + length;

      if (end > bytes.length) {
        break;
      }

      strings.push(textDecoder.decode(bytes.slice(start, end)));
      index = end;
      continue;
    }

    if (formatCode === 0xb1 || formatCode === 0xb3) {
      if (index + 4 >= bytes.length) {
        break;
      }

      const length =
        (bytes[index + 1] << 24) |
        (bytes[index + 2] << 16) |
        (bytes[index + 3] << 8) |
        bytes[index + 4];

      const start = index + 5;
      const end = start + length;

      if (length < 0 || end > bytes.length) {
        break;
      }

      strings.push(textDecoder.decode(bytes.slice(start, end)));
      index = end;
      continue;
    }

    index += 1;
  }

  return strings;
}

function extractBodyText(data: unknown, bytes: Uint8Array | undefined): string | undefined {
  if (typeof data === "string") {
    return data;
  }

  if (!bytes) {
    return undefined;
  }

  const amqpStrings = extractAmqpStrings(bytes)
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  if (amqpStrings.length > 0) {
    return amqpStrings[amqpStrings.length - 1];
  }

  return decodePrintableText(bytes);
}

export function parseServiceBusAmqpRequest(data: unknown, bytes: Uint8Array | undefined): ParsedServiceBusAmqpRequest {
  const isText = typeof data === "string";
  const bodyText = extractBodyText(data, bytes);
  const parsedFrame = bytes ? parseAmqpFrame(bytes) : undefined;

  if (!bytes) {
    return {
      messageType: isText ? "text" : "binary",
      byteLength: 0,
      hexPreview: "",
      textPreview: isText ? data : undefined,
      bodyText,
      parsedFrame,
      frame: parsedFrame
        ? {
            channel: parsedFrame.channel,
            size: parsedFrame.size,
            doff: parsedFrame.doff,
            type: parsedFrame.type,
            performative: parsedFrame.performative,
          }
        : undefined,
    };
  }

  const messageType: ParsedServiceBusAmqpRequest["messageType"] = isText
    ? "text"
    : isAmqpHeader(bytes)
      ? "protocol-header"
      : "amqp-frame";

  return {
    messageType,
    byteLength: bytes.byteLength,
    hexPreview: toHexPreview(bytes),
    textPreview: isText ? data : undefined,
    bodyText,
    parsedFrame,
    frame: parsedFrame
      ? {
          channel: parsedFrame.channel,
          size: parsedFrame.size,
          doff: parsedFrame.doff,
          type: parsedFrame.type,
          performative: parsedFrame.performative,
        }
      : undefined,
  };
}
