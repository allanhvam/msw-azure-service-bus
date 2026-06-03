import type { ServiceBusTier } from "../types/ServiceBusTier.js";

/** Max brokered message size (payload and properties) per tier, in bytes. */
export const SERVICE_BUS_TIER_MAX_MESSAGE_SIZE_BYTES: Record<ServiceBusTier, number> = {
  basic: 256 * 1024,
  standard: 256 * 1024,
  premium: 100 * 1024 * 1024,
};

export function getMaxMessageSizeBytes(tier: ServiceBusTier): number {
  return SERVICE_BUS_TIER_MAX_MESSAGE_SIZE_BYTES[tier];
}

/** Rough brokered message size (body + properties/annotations overhead). */
export function estimateBrokeredMessageSizeBytes(message: Record<string, unknown>): number {
  let size = 512;

  const body = message.body;
  if (typeof body === "string") {
    size += Buffer.byteLength(body, "utf8");
  } else if (body instanceof Uint8Array) {
    size += body.length;
  } else if (body !== undefined) {
    size += Buffer.byteLength(JSON.stringify(body), "utf8");
  }

  return size;
}

export function estimateBrokeredBatchSizeBytes(messages: Record<string, unknown>[]): number {
  return messages.reduce((total, message) => total + estimateBrokeredMessageSizeBytes(message), 0);
}