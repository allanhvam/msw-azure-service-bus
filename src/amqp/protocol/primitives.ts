const textEncoder = new TextEncoder();

export function concatBytes(...parts: Uint8Array[]): Uint8Array {
  const totalLength = parts.reduce((sum, current) => sum + current.length, 0);
  const combined = new Uint8Array(totalLength);
  let offset = 0;

  for (const part of parts) {
    combined.set(part, offset);
    offset += part.length;
  }

  return combined;
}

export function encodeUInt(value: number): Uint8Array {
  if (value === 0) {
    return new Uint8Array([0x43]);
  }

  if (value > 0 && value <= 0xff) {
    return new Uint8Array([0x52, value]);
  }

  return new Uint8Array([0x70, (value >>> 24) & 0xff, (value >>> 16) & 0xff, (value >>> 8) & 0xff, value & 0xff]);
}

export function encodeString(value: string): Uint8Array {
  const valueBytes = textEncoder.encode(value);

  if (valueBytes.length <= 0xff) {
    return concatBytes(new Uint8Array([0xa1, valueBytes.length]), valueBytes);
  }

  return concatBytes(
    new Uint8Array([
      0xb1,
      (valueBytes.length >>> 24) & 0xff,
      (valueBytes.length >>> 16) & 0xff,
      (valueBytes.length >>> 8) & 0xff,
      valueBytes.length & 0xff,
    ]),
    valueBytes,
  );
}

export function encodeSymbol(value: string): Uint8Array {
  const valueBytes = textEncoder.encode(value);

  if (valueBytes.length <= 0xff) {
    return concatBytes(new Uint8Array([0xa3, valueBytes.length]), valueBytes);
  }

  return concatBytes(
    new Uint8Array([
      0xb3,
      (valueBytes.length >>> 24) & 0xff,
      (valueBytes.length >>> 16) & 0xff,
      (valueBytes.length >>> 8) & 0xff,
      valueBytes.length & 0xff,
    ]),
    valueBytes,
  );
}

export function encodeBoolean(value: boolean): Uint8Array {
  return new Uint8Array([value ? 0x41 : 0x42]);
}

export function encodeUByte(value: number): Uint8Array {
  return new Uint8Array([0x50, value & 0xff]);
}

export function encodeUShort(value: number): Uint8Array {
  return new Uint8Array([0x60, (value >>> 8) & 0xff, value & 0xff]);
}

export function encodeNull(): Uint8Array {
  return new Uint8Array([0x40]);
}

export function encodeSymbolArray(values: string[]): Uint8Array {
  // Determine constructor upfront to ensure all elements use the same encoding width
  const valueBytesList = values.map((value) => textEncoder.encode(value));
  const needsSym32 = valueBytesList.some((vb) => vb.length > 0xff);
  const constructor = needsSym32 ? 0xb3 : 0xa3;

  const encodedValues = valueBytesList.map((valueBytes) => {
    if (needsSym32) {
      // sym32: 4-byte length for all elements
      return concatBytes(
        new Uint8Array([
          (valueBytes.length >>> 24) & 0xff,
          (valueBytes.length >>> 16) & 0xff,
          (valueBytes.length >>> 8) & 0xff,
          valueBytes.length & 0xff,
        ]),
        valueBytes,
      );
    }

    // sym8: 1-byte length
    return concatBytes(new Uint8Array([valueBytes.length]), valueBytes);
  });

  const body = concatBytes(...encodedValues);

  // constructor byte + count header byte(s) contribute to size
  const constructorSize = 1;

  // array8: size (1 byte) + count (1 byte), max 255 each
  const smallSize = constructorSize + body.length;
  if (smallSize <= 0xff && values.length <= 0xff) {
    return concatBytes(new Uint8Array([0xe0, smallSize + 1, values.length, constructor]), body);
  }

  // array32: size (4 bytes) + count (4 bytes)
  const size = constructorSize + body.length + 4; // +4 for the count field inside the size
  return concatBytes(
    new Uint8Array([
      0xf0,
      (size >>> 24) & 0xff,
      (size >>> 16) & 0xff,
      (size >>> 8) & 0xff,
      size & 0xff,
      (values.length >>> 24) & 0xff,
      (values.length >>> 16) & 0xff,
      (values.length >>> 8) & 0xff,
      values.length & 0xff,
      constructor,
    ]),
    body,
  );
}

export function encodeBinary(value: Uint8Array): Uint8Array {
  if (value.length <= 0xff) {
    return concatBytes(new Uint8Array([0xa0, value.length]), value);
  }

  return concatBytes(
    new Uint8Array([
      0xb0,
      (value.length >>> 24) & 0xff,
      (value.length >>> 16) & 0xff,
      (value.length >>> 8) & 0xff,
      value.length & 0xff,
    ]),
    value,
  );
}

export function encodeDescribedList(descriptorCode: number, fields: Uint8Array[]): Uint8Array {
  const body = concatBytes(...fields);
  const descriptor = new Uint8Array([0x00, 0x53, descriptorCode]);

  if (body.length + 1 <= 0xff && fields.length <= 0xff) {
    const listPrefix = new Uint8Array([0xc0, body.length + 1, fields.length]);
    return concatBytes(descriptor, listPrefix, body);
  }

  const size = body.length + 4;
  const listPrefix = new Uint8Array([
    0xd0,
    (size >>> 24) & 0xff,
    (size >>> 16) & 0xff,
    (size >>> 8) & 0xff,
    size & 0xff,
    (fields.length >>> 24) & 0xff,
    (fields.length >>> 16) & 0xff,
    (fields.length >>> 8) & 0xff,
    fields.length & 0xff,
  ]);
  return concatBytes(descriptor, listPrefix, body);
}

export function encodeAmqpFrame(channel: number, performativeBody: Uint8Array, type = 0): Uint8Array {
  const frameSize = 8 + performativeBody.length;
  return concatBytes(
    new Uint8Array([
      (frameSize >>> 24) & 0xff,
      (frameSize >>> 16) & 0xff,
      (frameSize >>> 8) & 0xff,
      frameSize & 0xff,
      0x02,
      type,
      (channel >>> 8) & 0xff,
      channel & 0xff,
    ]),
    performativeBody,
  );
}

export function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return new Uint8Array(bytes).buffer;
}
