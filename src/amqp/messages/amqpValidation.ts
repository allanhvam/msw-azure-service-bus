import { decodeAmqpValue } from "../protocol/decodeAmqpValue.js";
import { decodeTransferMessage, decodeTransferMessages } from "./amqpMessageDecode.js";
import { encodeAmqpMessage, encodeCbsResponseMessage, encodeManagementResponseMessage } from "./amqpMessageEncode.js";

type SectionValidationResult = {
  isValid: boolean;
  error?: string;
};

type AnnotationValidationResult = {
  isValid: boolean;
  error?: string;
};

const KNOWN_SECTION_CODES = new Set([0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78]);

export function validateBareMessageSections(payload: Uint8Array | undefined): SectionValidationResult {
  if (!payload || payload.length === 0) {
    return { isValid: true };
  }

  const sectionCodes: number[] = [];
  let offset = 0;

  while (offset < payload.length) {
    if (offset + 1 >= payload.length || payload[offset] !== 0x00) {
      return { isValid: false, error: "Malformed AMQP section descriptor" };
    }

    let descriptorCode: number;

    if (payload[offset + 1] === 0x53) {
      // smallulong descriptor: 0x00 0x53 <byte>
      if (offset + 2 >= payload.length) {
        return { isValid: false, error: "Malformed AMQP section descriptor" };
      }
      descriptorCode = payload[offset + 2];
    } else if (payload[offset + 1] === 0x80) {
      // ulong descriptor: 0x00 0x80 <8 bytes>
      if (offset + 9 >= payload.length) {
        return { isValid: false, error: "Malformed AMQP section descriptor" };
      }
      // AMQP message section codes fit in a single byte
      descriptorCode = payload[offset + 9];
    } else {
      return { isValid: false, error: "Malformed AMQP section descriptor" };
    }
    if (!KNOWN_SECTION_CODES.has(descriptorCode)) {
      return { isValid: false, error: `Unsupported AMQP section descriptor 0x${descriptorCode.toString(16)}` };
    }

    sectionCodes.push(descriptorCode);

    const parsed = decodeAmqpValue(payload, offset);
    if (parsed === undefined || parsed.nextOffset <= offset) {
      return { isValid: false, error: "Malformed AMQP section value" };
    }

    offset = parsed.nextOffset;
  }

  const sectionOrder = new Map<number, number>([
    [0x70, 0],
    [0x71, 1],
    [0x72, 2],
    [0x73, 3],
    [0x74, 4],
    [0x75, 5],
    [0x76, 5],
    [0x77, 5],
    [0x78, 6],
  ]);

  let previousRank = -1;
  let bodyType: number | undefined;

  for (const code of sectionCodes) {
    const rank = sectionOrder.get(code);
    if (rank === undefined) {
      return { isValid: false, error: `Unknown AMQP section descriptor 0x${code.toString(16)}` };
    }

    if (rank < previousRank) {
      return { isValid: false, error: "AMQP section order violation" };
    }

    if (code === 0x75 || code === 0x76 || code === 0x77) {
      if (bodyType === undefined) {
        bodyType = code;
      } else if (bodyType !== code) {
        return { isValid: false, error: "Mixed AMQP body section types are not allowed" };
      }
    }

    previousRank = rank;
  }

  return { isValid: true };
}

export { decodeTransferMessage, decodeTransferMessages, encodeAmqpMessage, encodeCbsResponseMessage, encodeManagementResponseMessage };

export function validateInboundMessageAnnotations(message: Record<string, unknown> | undefined): AnnotationValidationResult {
  if (!message) {
    return { isValid: true };
  }

  const annotations = message.message_annotations;
  if (annotations === undefined || annotations === null) {
    return { isValid: true };
  }

  if (typeof annotations !== "object" || Array.isArray(annotations)) {
    return { isValid: false, error: "message-annotations must be an AMQP map" };
  }

  const invalidKeys = Object.keys(annotations as Record<string, unknown>).filter((key) => !key.startsWith("x-opt-"));
  if (invalidKeys.length > 0) {
    return {
      isValid: false,
      error: `Unsupported message-annotations keys: ${invalidKeys.join(", ")}`,
    };
  }

  return { isValid: true };
}
