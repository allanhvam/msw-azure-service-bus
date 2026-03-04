import type { ParsedAmqpValue } from "../types/protocol.js";

const textDecoder = new TextDecoder();

export function readUInt8(bytes: Uint8Array, offset: number): number | undefined {
  if (offset >= bytes.length) {
    return undefined;
  }

  return bytes[offset];
}

function readUInt16(bytes: Uint8Array, offset: number): number | undefined {
  if (offset + 1 >= bytes.length) {
    return undefined;
  }

  return (bytes[offset] << 8) | bytes[offset + 1];
}

function readInt8(bytes: Uint8Array, offset: number): number | undefined {
  const value = readUInt8(bytes, offset);
  if (value === undefined) {
    return undefined;
  }

  return value > 0x7f ? value - 0x100 : value;
}

function readInt16(bytes: Uint8Array, offset: number): number | undefined {
  const value = readUInt16(bytes, offset);
  if (value === undefined) {
    return undefined;
  }

  return value > 0x7fff ? value - 0x10000 : value;
}

export function readUInt32(bytes: Uint8Array, offset: number): number {
  return ((bytes[offset] << 24) | (bytes[offset + 1] << 16) | (bytes[offset + 2] << 8) | bytes[offset + 3]) >>> 0;
}

function readInt32(bytes: Uint8Array, offset: number): number | undefined {
  if (offset + 3 >= bytes.length) {
    return undefined;
  }

  return (bytes[offset] << 24) | (bytes[offset + 1] << 16) | (bytes[offset + 2] << 8) | bytes[offset + 3];
}

function readUInt64(bytes: Uint8Array, offset: number): bigint | undefined {
  if (offset + 7 >= bytes.length) {
    return undefined;
  }

  let result = 0n;
  for (let index = 0; index < 8; index += 1) {
    result = (result << 8n) | BigInt(bytes[offset + index]);
  }

  return result;
}

function readInt64(bytes: Uint8Array, offset: number): bigint | undefined {
  const value = readUInt64(bytes, offset);
  if (value === undefined) {
    return undefined;
  }

  if ((value & (1n << 63n)) === 0n) {
    return value;
  }

  return value - (1n << 64n);
}

function bigIntToNumberIfSafe(value: bigint): number | bigint {
  const maxSafe = BigInt(Number.MAX_SAFE_INTEGER);
  const minSafe = BigInt(Number.MIN_SAFE_INTEGER);
  if (value <= maxSafe && value >= minSafe) {
    return Number(value);
  }

  return value;
}

function readLengthPrefixed(bytes: Uint8Array, offset: number, width: 1 | 4): { data: Uint8Array; nextOffset: number } | undefined {
  if (width === 1) {
    const length = readUInt8(bytes, offset + 1);
    if (length === undefined) {
      return undefined;
    }

    const start = offset + 2;
    const end = start + length;
    if (end > bytes.length) {
      return undefined;
    }

    return { data: bytes.slice(start, end), nextOffset: end };
  }

  if (offset + 4 >= bytes.length) {
    return undefined;
  }

  const length = readUInt32(bytes, offset + 1);
  const start = offset + 5;
  const end = start + length;
  if (end > bytes.length) {
    return undefined;
  }

  return { data: bytes.slice(start, end), nextOffset: end };
}

function readCompositeHeader(bytes: Uint8Array, offset: number, width: 1 | 4): { bodyOffset: number; bodySize: number; count: number } | undefined {
  if (width === 1) {
    const size = readUInt8(bytes, offset + 1);
    const count = readUInt8(bytes, offset + 2);
    if (size === undefined || count === undefined) {
      return undefined;
    }

    const bodyOffset = offset + 3;
    const bodySize = size - 1;
    if (bodySize < 0 || bodyOffset + bodySize > bytes.length) {
      return undefined;
    }

    return { bodyOffset, bodySize, count };
  }

  if (offset + 8 >= bytes.length) {
    return undefined;
  }

  const size = readUInt32(bytes, offset + 1);
  const count = readUInt32(bytes, offset + 5);
  const bodyOffset = offset + 9;
  const bodySize = size - 4;
  if (bodySize < 0 || bodyOffset + bodySize > bytes.length) {
    return undefined;
  }

  return { bodyOffset, bodySize, count };
}

function readFloat32(buffer: ArrayBufferLike, offset: number): number {
  return new DataView(buffer, offset, 4).getFloat32(0, false);
}

function readFloat64(buffer: ArrayBufferLike, offset: number): number {
  return new DataView(buffer, offset, 8).getFloat64(0, false);
}

/**
 * Decode a single element within an AMQP array, given the shared constructor type code.
 * Array elements don't carry individual type codes — the constructor is shared.
 */
function decodeArrayElement(bytes: Uint8Array, offset: number, constructorCode: number): ParsedAmqpValue | undefined {
  // Zero-width types
  if (constructorCode === 0x40) return { value: null, nextOffset: offset };
  if (constructorCode === 0x41) return { value: true, nextOffset: offset };
  if (constructorCode === 0x42) return { value: false, nextOffset: offset };
  if (constructorCode === 0x43) return { value: 0, nextOffset: offset };
  if (constructorCode === 0x44) return { value: BigInt(0), nextOffset: offset };

  // 1-byte unsigned (ubyte, smalluint, smallulong)
  if (constructorCode === 0x50 || constructorCode === 0x52 || constructorCode === 0x53) {
    const v = readUInt8(bytes, offset);
    return v !== undefined ? { value: v, nextOffset: offset + 1 } : undefined;
  }
  // 1-byte signed (byte, smallint, smalllong)
  if (constructorCode === 0x51 || constructorCode === 0x54 || constructorCode === 0x55) {
    const v = readInt8(bytes, offset);
    return v !== undefined ? { value: v, nextOffset: offset + 1 } : undefined;
  }
  // 1-byte boolean
  if (constructorCode === 0x56) {
    const v = readUInt8(bytes, offset);
    return v !== undefined ? { value: v !== 0, nextOffset: offset + 1 } : undefined;
  }

  // 2-byte types
  if (constructorCode === 0x60) {
    const v = readUInt16(bytes, offset);
    return v !== undefined ? { value: v, nextOffset: offset + 2 } : undefined;
  }
  if (constructorCode === 0x61) {
    const v = readInt16(bytes, offset);
    return v !== undefined ? { value: v, nextOffset: offset + 2 } : undefined;
  }

  // 4-byte types
  if (constructorCode === 0x70) {
    if (offset + 3 >= bytes.length) return undefined;
    return { value: readUInt32(bytes, offset), nextOffset: offset + 4 };
  }
  if (constructorCode === 0x71) {
    const v = readInt32(bytes, offset);
    return v !== undefined ? { value: v, nextOffset: offset + 4 } : undefined;
  }
  if (constructorCode === 0x72) {
    if (offset + 3 >= bytes.length) return undefined;
    return { value: readFloat32(bytes.buffer, bytes.byteOffset + offset), nextOffset: offset + 4 };
  }
  if (constructorCode === 0x73) {
    if (offset + 3 >= bytes.length) return undefined;
    return { value: String.fromCodePoint(readUInt32(bytes, offset)), nextOffset: offset + 4 };
  }

  // 8-byte types
  if (constructorCode === 0x80) {
    const v = readUInt64(bytes, offset);
    return v !== undefined ? { value: bigIntToNumberIfSafe(v), nextOffset: offset + 8 } : undefined;
  }
  if (constructorCode === 0x81) {
    const v = readInt64(bytes, offset);
    return v !== undefined ? { value: bigIntToNumberIfSafe(v), nextOffset: offset + 8 } : undefined;
  }
  if (constructorCode === 0x82) {
    if (offset + 7 >= bytes.length) return undefined;
    return { value: readFloat64(bytes.buffer, bytes.byteOffset + offset), nextOffset: offset + 8 };
  }
  if (constructorCode === 0x83) {
    const v = readInt64(bytes, offset);
    return v !== undefined ? { value: new Date(Number(v)), nextOffset: offset + 8 } : undefined;
  }
  if (constructorCode === 0x84) {
    if (offset + 7 >= bytes.length) return undefined;
    return { value: bytes.slice(offset, offset + 8), nextOffset: offset + 8 };
  }

  // 16-byte types
  if (constructorCode === 0x94) {
    if (offset + 15 >= bytes.length) return undefined;
    return { value: bytes.slice(offset, offset + 16), nextOffset: offset + 16 };
  }
  if (constructorCode === 0x98) {
    if (offset + 15 >= bytes.length) return undefined;
    return { value: bytes.slice(offset, offset + 16), nextOffset: offset + 16 };
  }

  // Variable-width 1-byte length prefix (vbin8, str8, sym8)
  if (constructorCode === 0xa0 || constructorCode === 0xa1 || constructorCode === 0xa3) {
    const len = readUInt8(bytes, offset);
    if (len === undefined || offset + 1 + len > bytes.length) return undefined;
    const data = bytes.slice(offset + 1, offset + 1 + len);
    const value = constructorCode === 0xa0 ? data : textDecoder.decode(data);
    return { value, nextOffset: offset + 1 + len };
  }

  // Variable-width 4-byte length prefix (vbin32, str32, sym32)
  if (constructorCode === 0xb0 || constructorCode === 0xb1 || constructorCode === 0xb3) {
    if (offset + 3 >= bytes.length) return undefined;
    const len = readUInt32(bytes, offset);
    const start = offset + 4;
    if (start + len > bytes.length) return undefined;
    const data = bytes.slice(start, start + len);
    const value = constructorCode === 0xb0 ? data : textDecoder.decode(data);
    return { value, nextOffset: start + len };
  }

  return undefined;
}

/**
 * Unified AMQP 1.0 type decoder.
 *
 * Decodes a single AMQP typed value starting at `offset` in `bytes` and returns
 * both the decoded value and the offset immediately after the consumed bytes.
 *
 * Covers all fixed-width primitives (null, boolean, ubyte, byte, ushort, short,
 * uint, int, ulong, long, float, double, decimal128, timestamp, UUID), variable-
 * width types (binary, string, symbol), compound types (list, map, array), and
 * described types.
 */
export function decodeAmqpValue(bytes: Uint8Array, offset: number): ParsedAmqpValue | undefined {
  if (offset >= bytes.length) {
    return undefined;
  }

  const typeCode = bytes[offset];

  // --- described type (constructor) ---
  if (typeCode === 0x00) {
    if (offset + 1 >= bytes.length) {
      return undefined;
    }

    const descriptor = decodeAmqpValue(bytes, offset + 1);
    if (!descriptor) {
      return undefined;
    }

    const described = decodeAmqpValue(bytes, descriptor.nextOffset);
    if (!described) {
      return undefined;
    }

    return {
      value: {
        descriptor: descriptor.value,
        value: described.value,
      },
      nextOffset: described.nextOffset,
    };
  }

  // --- null ---
  if (typeCode === 0x40) {
    return { value: null, nextOffset: offset + 1 };
  }

  // --- boolean ---
  if (typeCode === 0x41) {
    return { value: true, nextOffset: offset + 1 };
  }

  if (typeCode === 0x42) {
    return { value: false, nextOffset: offset + 1 };
  }

  if (typeCode === 0x56) {
    const value = readUInt8(bytes, offset + 1);
    if (value === undefined) {
      return undefined;
    }

    return { value: value !== 0, nextOffset: offset + 2 };
  }

  // --- zero-width numeric shortcuts ---
  if (typeCode === 0x43) {
    return { value: 0, nextOffset: offset + 1 };
  }

  if (typeCode === 0x44) {
    return { value: BigInt(0), nextOffset: offset + 1 };
  }

  // --- empty containers ---
  if (typeCode === 0x45) {
    return { value: [], nextOffset: offset + 1 };
  }

  // --- 1-byte body: ubyte (0x50), smalluint (0x52), smallulong (0x53), smallint (0x54) ---
  if (typeCode === 0x50 || typeCode === 0x52 || typeCode === 0x53 || typeCode === 0x54) {
    const value = readUInt8(bytes, offset + 1);
    if (value === undefined) {
      return undefined;
    }

    return { value, nextOffset: offset + 2 };
  }

  // --- byte (0x51) ---
  if (typeCode === 0x51) {
    const value = readInt8(bytes, offset + 1);
    if (value === undefined) {
      return undefined;
    }

    return { value, nextOffset: offset + 2 };
  }

  // --- smalllong (0x55) ---
  if (typeCode === 0x55) {
    const value = readInt8(bytes, offset + 1);
    if (value === undefined) {
      return undefined;
    }

    return { value, nextOffset: offset + 2 };
  }

  // --- ushort (0x60) ---
  if (typeCode === 0x60) {
    const value = readUInt16(bytes, offset + 1);
    if (value === undefined) {
      return undefined;
    }

    return { value, nextOffset: offset + 3 };
  }

  // --- short (0x61) ---
  if (typeCode === 0x61) {
    const value = readInt16(bytes, offset + 1);
    if (value === undefined) {
      return undefined;
    }

    return { value, nextOffset: offset + 3 };
  }

  // --- uint (0x70) ---
  if (typeCode === 0x70) {
    if (offset + 4 >= bytes.length) {
      return undefined;
    }

    return { value: readUInt32(bytes, offset + 1), nextOffset: offset + 5 };
  }

  // --- int (0x71) ---
  if (typeCode === 0x71) {
    const value = readInt32(bytes, offset + 1);
    if (value === undefined) {
      return undefined;
    }

    return { value, nextOffset: offset + 5 };
  }

  // --- float (0x72) ---
  if (typeCode === 0x72) {
    if (offset + 4 >= bytes.length) {
      return undefined;
    }

    return { value: readFloat32(bytes.buffer, bytes.byteOffset + offset + 1), nextOffset: offset + 5 };
  }

  // --- char (0x73) ---
  if (typeCode === 0x73) {
    if (offset + 4 >= bytes.length) {
      return undefined;
    }

    const codepoint = readUInt32(bytes, offset + 1);
    return { value: String.fromCodePoint(codepoint), nextOffset: offset + 5 };
  }

  // --- ulong (0x80) ---
  if (typeCode === 0x80) {
    const value = readUInt64(bytes, offset + 1);
    if (value === undefined) {
      return undefined;
    }

    return { value: bigIntToNumberIfSafe(value), nextOffset: offset + 9 };
  }

  // --- long (0x81) ---
  if (typeCode === 0x81) {
    const value = readInt64(bytes, offset + 1);
    if (value === undefined) {
      return undefined;
    }

    return { value: bigIntToNumberIfSafe(value), nextOffset: offset + 9 };
  }

  // --- double (0x82) ---
  if (typeCode === 0x82) {
    if (offset + 8 >= bytes.length) {
      return undefined;
    }

    return { value: readFloat64(bytes.buffer, bytes.byteOffset + offset + 1), nextOffset: offset + 9 };
  }

  // --- timestamp (0x83) ---
  if (typeCode === 0x83) {
    const value = readInt64(bytes, offset + 1);
    if (value === undefined) {
      return undefined;
    }

    return { value: new Date(Number(value)), nextOffset: offset + 9 };
  }

  // --- decimal64 (0x84) ---
  if (typeCode === 0x84) {
    if (offset + 8 >= bytes.length) {
      return undefined;
    }

    return { value: bytes.slice(offset + 1, offset + 9), nextOffset: offset + 9 };
  }

  // --- uuid (0x98) ---
  if (typeCode === 0x98) {
    if (offset + 16 >= bytes.length) {
      return undefined;
    }

    return { value: bytes.slice(offset + 1, offset + 17), nextOffset: offset + 17 };
  }

  // --- binary (vbin8 0xa0, vbin32 0xb0) ---
  if (typeCode === 0xa0 || typeCode === 0xb0) {
    const binary = readLengthPrefixed(bytes, offset, typeCode === 0xa0 ? 1 : 4);
    if (!binary) {
      return undefined;
    }

    return { value: binary.data, nextOffset: binary.nextOffset };
  }

  // --- string / symbol (str8 0xa1, str32 0xb1, sym8 0xa3, sym32 0xb3) ---
  if (typeCode === 0xa1 || typeCode === 0xb1 || typeCode === 0xa3 || typeCode === 0xb3) {
    const stringData = readLengthPrefixed(bytes, offset, typeCode === 0xa1 || typeCode === 0xa3 ? 1 : 4);
    if (!stringData) {
      return undefined;
    }

    return { value: textDecoder.decode(stringData.data), nextOffset: stringData.nextOffset };
  }

  // --- list (list8 0xc0, list32 0xd0) ---
  if (typeCode === 0xc0 || typeCode === 0xd0) {
    const header = readCompositeHeader(bytes, offset, typeCode === 0xc0 ? 1 : 4);
    if (!header) {
      return undefined;
    }

    let cursor = header.bodyOffset;
    const end = header.bodyOffset + header.bodySize;
    const list: unknown[] = [];

    for (let index = 0; index < header.count; index += 1) {
      const item = decodeAmqpValue(bytes, cursor);
      if (!item || item.nextOffset > end) {
        return undefined;
      }

      list.push(item.value);
      cursor = item.nextOffset;
    }

    return { value: list, nextOffset: end };
  }

  // --- map (map8 0xc1, map32 0xd1) ---
  if (typeCode === 0xc1 || typeCode === 0xd1) {
    const header = readCompositeHeader(bytes, offset, typeCode === 0xc1 ? 1 : 4);
    if (!header || header.count % 2 !== 0) {
      return undefined;
    }

    let cursor = header.bodyOffset;
    const end = header.bodyOffset + header.bodySize;
    const map: Record<string, unknown> = {};

    for (let index = 0; index < header.count; index += 2) {
      const key = decodeAmqpValue(bytes, cursor);
      if (!key || key.nextOffset > end) {
        return undefined;
      }

      const value = decodeAmqpValue(bytes, key.nextOffset);
      if (!value || value.nextOffset > end) {
        return undefined;
      }

      map[String(key.value)] = value.value;
      cursor = value.nextOffset;
    }

    return { value: map, nextOffset: end };
  }

  // --- array (array8 0xe0, array32 0xf0) ---
  if (typeCode === 0xe0 || typeCode === 0xf0) {
    const header = readCompositeHeader(bytes, offset, typeCode === 0xe0 ? 1 : 4);
    if (!header) {
      return undefined;
    }

    const end = header.bodyOffset + header.bodySize;

    if (header.count === 0) {
      return { value: [], nextOffset: end };
    }

    // Read the shared constructor type code
    const constructorCode = readUInt8(bytes, header.bodyOffset);
    if (constructorCode === undefined) {
      return undefined;
    }

    let cursor = header.bodyOffset + 1;

    const values: unknown[] = [];
    for (let index = 0; index < header.count; index += 1) {
      const item = decodeArrayElement(bytes, cursor, constructorCode);
      if (!item || item.nextOffset > end) {
        return undefined;
      }

      values.push(item.value);
      cursor = item.nextOffset;
    }

    return { value: values, nextOffset: end };
  }

  return undefined;
}
