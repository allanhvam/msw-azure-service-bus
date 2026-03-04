export type AmqpPerformative =
  | "sasl-mechanisms"
  | "sasl-init"
  | "sasl-challenge"
  | "sasl-response"
  | "sasl-outcome"
  | "open"
  | "begin"
  | "attach"
  | "flow"
  | "transfer"
  | "disposition"
  | "detach"
  | "end"
  | "close";

export type AmqpFrame = {
  size: number;
  doff: number;
  type: number;
  channel: number;
  body: Uint8Array;
  performative?: AmqpPerformative;
};

export type AmqpFrameInfo = {
  channel: number;
  size: number;
  doff: number;
  type: number;
  performative?: AmqpPerformative;
};

export type ParsedAmqpValue = {
  value: unknown;
  nextOffset: number;
};

export type ParsedDescribedList = {
  descriptorCode: number;
  fields: unknown[];
  nextOffset: number;
};

export type ParsedAttach = {
  name?: string;
  handle?: number;
  role?: boolean;
  sndSettleMode?: number;
  rcvSettleMode?: number;
  sourceAddress?: string;
  targetAddress?: string;
};

export type ParsedTransfer = {
  handle?: number;
  deliveryId?: number;
  deliveryTag?: Uint8Array;
  messageFormat?: number;
  settled?: boolean;
  more?: boolean;
  rcvSettleMode?: number;
  state?: ParsedDeliveryState;
  resume?: boolean;
  aborted?: boolean;
  payload?: Uint8Array;
};

export type ParsedFlow = {
  nextIncomingId?: number;
  incomingWindow?: number;
  nextOutgoingId?: number;
  outgoingWindow?: number;
  handle?: number;
  deliveryCount?: number;
  linkCredit?: number;
  drain?: boolean;
  echo?: boolean;
};

export type ParsedDetach = {
  handle?: number;
  closed?: boolean;
};

export type ParsedDisposition = {
  role?: boolean;
  first?: number;
  last?: number;
  settled?: boolean;
  state?: ParsedDeliveryState;
  batchable?: boolean;
};

export type ParsedDeliveryState =
  | "received"
  | "accepted"
  | "released"
  | "modified"
  | "rejected"
  | "other";

export type ParsedServiceBusAmqpRequest = {
  messageType: "protocol-header" | "amqp-frame" | "text" | "binary";
  byteLength: number;
  hexPreview: string;
  textPreview?: string;
  bodyText?: string;
  frame?: AmqpFrameInfo;
  parsedFrame?: AmqpFrame;
};
