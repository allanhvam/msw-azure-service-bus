import { decodeAmqpValue } from "../protocol/decodeAmqpValue.js";
import { concatBytes } from "../protocol/primitives.js";

const textDecoder = new TextDecoder();
const MESSAGE_SECTION_DESCRIPTOR_CODES = new Set([0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78]);

type ParsedMessageSections = {
  message: Record<string, unknown>;
  bodySections: unknown[];
};

function stripTrailingNulls(values: unknown[]): unknown[] {
  let end = values.length;
  while (end > 0 && values[end - 1] === null) {
    end -= 1;
  }

  return values.slice(0, end);
}

function resolveDescriptorCode(payload: Uint8Array, offset: number): number | undefined {
  if (offset + 2 >= payload.length || payload[offset] !== 0x00) {
    return undefined;
  }

  // smallulong: 0x00 0x53 <byte>
  if (payload[offset + 1] === 0x53 && offset + 2 < payload.length) {
    return payload[offset + 2];
  }

  // ulong: 0x00 0x80 <8 bytes> — use only the low byte for known section codes
  if (payload[offset + 1] === 0x80 && offset + 9 < payload.length) {
    return payload[offset + 9];
  }

  return undefined;
}

function parseMessageSections(payload: Uint8Array): ParsedMessageSections | undefined {
  const message: Record<string, unknown> = {};
  const bodySections: unknown[] = [];

  let offset = 0;
  while (offset < payload.length) {
    const descriptorCode = resolveDescriptorCode(payload, offset);
    if (descriptorCode === undefined) {
      return undefined;
    }

    const section = decodeAmqpValue(payload, offset);
    if (!section) {
      return undefined;
    }

    const described = section.value;
    if (!described || typeof described !== "object") {
      return undefined;
    }

    const sectionValue = (described as { value?: unknown }).value;

    if (descriptorCode === 0x72) {
      message.message_annotations = sectionValue;
    } else if (descriptorCode === 0x73 && Array.isArray(sectionValue)) {
      const properties = stripTrailingNulls(sectionValue);
      if (properties.length > 0) {
        message.message_id = properties[0];
      }
      if (properties.length > 5) {
        message.correlation_id = properties[5];
      }
      if (properties.length > 6) {
        message.content_type = properties[6];
      }
      // [8] absolute-expiry-time (timestamp)
      if (properties.length > 8 && properties[8] != null) {
        message.absolute_expiry_time = properties[8];
      }
      // [9] creation-time (timestamp)
      if (properties.length > 9 && properties[9] != null) {
        message.creation_time = properties[9];
      }
      // [10] group-id (string) — maps to sessionId
      if (properties.length > 10 && properties[10] != null) {
        message.group_id = properties[10];
      }
      // [12] reply-to-group-id (string) — maps to partitionKey
      if (properties.length > 12 && properties[12] != null) {
        message.reply_to_group_id = properties[12];
      }
    } else if (descriptorCode === 0x74) {
      if (!sectionValue || typeof sectionValue !== "object" || Array.isArray(sectionValue)) {
        return undefined;
      }

      message.application_properties = sectionValue;
    } else if (descriptorCode === 0x75) {
      if (sectionValue instanceof Uint8Array) {
        bodySections.push(sectionValue);
      }
    } else if (descriptorCode === 0x76 || descriptorCode === 0x77) {
      bodySections.push(sectionValue);
    }

    offset = section.nextOffset;
  }

  return {
    message,
    bodySections,
  };
}

function buildDecodedMessage(payload: Uint8Array): Record<string, unknown> | undefined {
  const parsed = parseMessageSections(payload);
  if (!parsed) {
    return undefined;
  }

  const { message, bodySections } = parsed;

  if (bodySections.length === 1) {
    const singleBody = bodySections[0];
    if (singleBody instanceof Uint8Array) {
      const textBody = textDecoder.decode(singleBody);
      try {
        message.body = JSON.parse(textBody) as unknown;
      } catch {
        message.body = textBody;
      }
    } else {
      message.body = singleBody;
    }
  } else if (bodySections.length > 1) {
    const binarySections = bodySections.every((value) => value instanceof Uint8Array);
    if (binarySections) {
      const combined = concatBytes(...(bodySections as Uint8Array[]));
      const textBody = textDecoder.decode(combined);
      try {
        message.body = JSON.parse(textBody) as unknown;
      } catch {
        message.body = textBody;
      }
    } else {
      message.body = bodySections;
    }
  }

  return message;
}

function isEmbeddedAmqpMessagePayload(value: unknown): value is Uint8Array {
  if (!(value instanceof Uint8Array) || value.length < 3) {
    return false;
  }

  return value[0] === 0x00 && value[1] === 0x53 && MESSAGE_SECTION_DESCRIPTOR_CODES.has(value[2]);
}

export function decodeTransferMessage(payload: Uint8Array | undefined): Record<string, unknown> | undefined {
  if (!payload || payload.length === 0) {
    return undefined;
  }

  try {
    const decoded = buildDecodedMessage(payload);
    if (!decoded || typeof decoded !== "object") {
      return undefined;
    }

    return decoded as Record<string, unknown>;
  } catch {
    return undefined;
  }
}

export function decodeTransferMessages(payload: Uint8Array | undefined): Record<string, unknown>[] | undefined {
  if (!payload || payload.length === 0) {
    return undefined;
  }

  const parsed = parseMessageSections(payload);
  if (!parsed) {
    return undefined;
  }

  const { bodySections } = parsed;
  const looksLikeEmbeddedBatch =
    bodySections.length > 0
    && bodySections.every((value) => value instanceof Uint8Array)
    && bodySections.every((value) => isEmbeddedAmqpMessagePayload(value));

  if (looksLikeEmbeddedBatch) {
    const decodedBatch = (bodySections as Uint8Array[])
      .map((entry) => decodeTransferMessage(entry))
      .filter((entry): entry is Record<string, unknown> => entry !== undefined);

    return decodedBatch.length > 0 ? decodedBatch : undefined;
  }

  const decodedSingle = buildDecodedMessage(payload);
  return decodedSingle ? [decodedSingle] : undefined;
}
