import type {
  ParsedDeliveryState,
  ParsedDescribedList,
  ParsedAttach,
  ParsedTransfer,
  ParsedFlow,
  ParsedDetach,
  ParsedDisposition,
} from "../types/protocol.js";
import { decodeAmqpValue } from "./decodeAmqpValue.js";

export { readUInt32 } from "./decodeAmqpValue.js";

function parseDescribedList(bytes: Uint8Array): ParsedDescribedList | undefined {
  if (bytes.length < 5 || bytes[0] !== 0x00 || bytes[1] !== 0x53) {
    return undefined;
  }

  const descriptorCode = bytes[2];
  const listParsed = decodeAmqpValue(bytes, 3);
  if (!listParsed || !Array.isArray(listParsed.value)) {
    return undefined;
  }

  return {
    descriptorCode,
    fields: listParsed.value,
    nextOffset: listParsed.nextOffset,
  };
}

function parseAddressFromTerminus(value: unknown): string | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }

  const terminus = value as { value?: unknown };
  if (!Array.isArray(terminus.value)) {
    return undefined;
  }

  return typeof terminus.value[0] === "string" ? terminus.value[0] : undefined;
}

export function parseAttachFrame(body: Uint8Array): ParsedAttach | undefined {
  const described = parseDescribedList(body);
  if (!described || described.descriptorCode !== 0x12) {
    return undefined;
  }

  const [name, handle, role, sndSettleMode, rcvSettleMode, source, target] = described.fields;

  const handleNumber = typeof handle === "number" ? handle : typeof handle === "bigint" ? Number(handle) : undefined;

  const linkName = typeof name === "string" ? name : undefined;
  const entityFromName = linkName?.split("-")[0];
  const normalizedEntity = entityFromName && entityFromName.length > 0 ? entityFromName : undefined;

  const sourceAddress = parseAddressFromTerminus(source) ?? ((role === true && normalizedEntity) ? normalizedEntity : undefined);
  const targetAddress = parseAddressFromTerminus(target) ?? ((role === false && normalizedEntity) ? normalizedEntity : undefined);

  return {
    name: linkName,
    handle: Number.isFinite(handleNumber) ? handleNumber : undefined,
    role: typeof role === "boolean" ? role : undefined,
    sndSettleMode: typeof sndSettleMode === "number" ? sndSettleMode : undefined,
    rcvSettleMode: typeof rcvSettleMode === "number" ? rcvSettleMode : undefined,
    sourceAddress,
    targetAddress,
  };
}

export function parseTransferFrame(body: Uint8Array): ParsedTransfer | undefined {
  const described = parseDescribedList(body);
  if (!described || described.descriptorCode !== 0x14) {
    return undefined;
  }

  const [
    handle,
    deliveryId,
    deliveryTag,
    messageFormat,
    settled,
    more,
    rcvSettleMode,
    state,
    resume,
    aborted,
  ] = described.fields;
  const payload = described.nextOffset < body.length ? body.slice(described.nextOffset) : undefined;

  const handleNumber = typeof handle === "number" ? handle : typeof handle === "bigint" ? Number(handle) : undefined;
  const deliveryIdNumber = typeof deliveryId === "number" ? deliveryId : typeof deliveryId === "bigint" ? Number(deliveryId) : undefined;
  const messageFormatNumber = typeof messageFormat === "number"
    ? messageFormat
    : typeof messageFormat === "bigint"
      ? Number(messageFormat)
      : undefined;
  const rcvSettleModeNumber = typeof rcvSettleMode === "number"
    ? rcvSettleMode
    : typeof rcvSettleMode === "bigint"
      ? Number(rcvSettleMode)
      : undefined;

  let parsedState: ParsedDeliveryState | undefined;
  if (state && typeof state === "object" && "descriptor" in state) {
    const descriptor = (state as { descriptor?: unknown }).descriptor;
    if (descriptor === 0x23) {
      parsedState = "received";
    } else if (descriptor === 0x24) {
      parsedState = "accepted";
    } else if (descriptor === 0x25) {
      parsedState = "rejected";
    } else if (descriptor === 0x26) {
      parsedState = "released";
    } else if (descriptor === 0x27) {
      parsedState = "modified";
    } else {
      parsedState = "other";
    }
  }

  return {
    handle: Number.isFinite(handleNumber) ? handleNumber : undefined,
    deliveryId: Number.isFinite(deliveryIdNumber) ? deliveryIdNumber : undefined,
    deliveryTag: deliveryTag instanceof Uint8Array ? deliveryTag : undefined,
    messageFormat: Number.isFinite(messageFormatNumber) ? messageFormatNumber : undefined,
    settled: typeof settled === "boolean" ? settled : undefined,
    more: typeof more === "boolean" ? more : undefined,
    rcvSettleMode: Number.isFinite(rcvSettleModeNumber) ? rcvSettleModeNumber : undefined,
    state: parsedState,
    resume: typeof resume === "boolean" ? resume : undefined,
    aborted: typeof aborted === "boolean" ? aborted : undefined,
    payload,
  };
}

export function parseFlowFrame(body: Uint8Array): ParsedFlow | undefined {
  const described = parseDescribedList(body);
  if (!described || described.descriptorCode !== 0x13) {
    return undefined;
  }

  const nextIncomingId = described.fields[0];
  const incomingWindow = described.fields[1];
  const nextOutgoingId = described.fields[2];
  const outgoingWindow = described.fields[3];
  const handle = described.fields[4];
  const deliveryCount = described.fields[5];
  const linkCredit = described.fields[6];
  const drain = described.fields[8];
  const echo = described.fields[9];

  const nextIncomingIdNumber = typeof nextIncomingId === "number" ? nextIncomingId : typeof nextIncomingId === "bigint" ? Number(nextIncomingId) : undefined;
  const incomingWindowNumber = typeof incomingWindow === "number" ? incomingWindow : typeof incomingWindow === "bigint" ? Number(incomingWindow) : undefined;
  const nextOutgoingIdNumber = typeof nextOutgoingId === "number" ? nextOutgoingId : typeof nextOutgoingId === "bigint" ? Number(nextOutgoingId) : undefined;
  const outgoingWindowNumber = typeof outgoingWindow === "number" ? outgoingWindow : typeof outgoingWindow === "bigint" ? Number(outgoingWindow) : undefined;
  const handleNumber = typeof handle === "number" ? handle : typeof handle === "bigint" ? Number(handle) : undefined;
  const deliveryCountNumber = typeof deliveryCount === "number" ? deliveryCount : typeof deliveryCount === "bigint" ? Number(deliveryCount) : undefined;
  const linkCreditNumber = typeof linkCredit === "number" ? linkCredit : typeof linkCredit === "bigint" ? Number(linkCredit) : undefined;

  return {
    nextIncomingId: Number.isFinite(nextIncomingIdNumber) ? nextIncomingIdNumber : undefined,
    incomingWindow: Number.isFinite(incomingWindowNumber) ? incomingWindowNumber : undefined,
    nextOutgoingId: Number.isFinite(nextOutgoingIdNumber) ? nextOutgoingIdNumber : undefined,
    outgoingWindow: Number.isFinite(outgoingWindowNumber) ? outgoingWindowNumber : undefined,
    handle: Number.isFinite(handleNumber) ? handleNumber : undefined,
    deliveryCount: Number.isFinite(deliveryCountNumber) ? deliveryCountNumber : undefined,
    linkCredit: Number.isFinite(linkCreditNumber) ? linkCreditNumber : undefined,
    drain: typeof drain === "boolean" ? drain : undefined,
    echo: typeof echo === "boolean" ? echo : undefined,
  };
}

export function parseDetachFrame(body: Uint8Array): ParsedDetach | undefined {
  const described = parseDescribedList(body);
  if (!described || described.descriptorCode !== 0x16) {
    return undefined;
  }

  const [handle, closed] = described.fields;
  const handleNumber = typeof handle === "number" ? handle : typeof handle === "bigint" ? Number(handle) : undefined;

  return {
    handle: Number.isFinite(handleNumber) ? handleNumber : undefined,
    closed: typeof closed === "boolean" ? closed : undefined,
  };
}

export function parseDispositionFrame(body: Uint8Array): ParsedDisposition | undefined {
  const described = parseDescribedList(body);
  if (!described || described.descriptorCode !== 0x15) {
    return undefined;
  }

  const [role, first, last, settled, state, batchable] = described.fields;

  const firstNumber = typeof first === "number" ? first : typeof first === "bigint" ? Number(first) : undefined;
  const lastNumber = typeof last === "number" ? last : typeof last === "bigint" ? Number(last) : undefined;

  let parsedState: ParsedDisposition["state"];
  if (state && typeof state === "object" && "descriptor" in state) {
    const descriptor = (state as { descriptor?: unknown }).descriptor;
    if (descriptor === 0x24) {
      parsedState = "accepted";
    } else if (descriptor === 0x25) {
      parsedState = "rejected";
    } else if (descriptor === 0x26) {
      parsedState = "released";
    } else if (descriptor === 0x27) {
      parsedState = "modified";
    } else if (descriptor === 0x23) {
      parsedState = "received";
    } else {
      parsedState = "other";
    }
  }

  return {
    role: typeof role === "boolean" ? role : undefined,
    first: Number.isFinite(firstNumber) ? firstNumber : undefined,
    last: Number.isFinite(lastNumber) ? lastNumber : undefined,
    settled: typeof settled === "boolean" ? settled : undefined,
    state: parsedState,
    batchable: typeof batchable === "boolean" ? batchable : undefined,
  };
}
