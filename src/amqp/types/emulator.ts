export type QueueMessage = {
  messageId: string;
  body: unknown;
  contentType?: string;
  correlationId?: string;
  sessionId?: string;
  partitionKey?: string;
  scheduledEnqueueTimeUtc?: Date;
  timeToLive?: number;
  applicationProperties?: Record<string, unknown>;
  deliveryCount: number;
  deadLetterReason?: string;
  deadLetterDescription?: string;
};

export type PendingDelivery = {
  queueName: string;
  message: QueueMessage;
  lockExpiresAt: number;
  lockTimer?: NodeJS.Timeout;
};

export type LinkState = {
  channel: number;
  handle: number;
  role: boolean;
  deliveryCount: number;
  availableCredit: number;
  receiveMode: "peekLock" | "receiveAndDelete";
  name?: string;
  sourceAddress?: string;
  targetAddress?: string;
};

export type OngoingTransfer = {
  deliveryId: number;
  deliveryTagKey: string;
  chunks: Uint8Array[];
  messageFormat?: number;
  settled?: boolean;
  state?: "received" | "accepted" | "released" | "modified" | "rejected" | "other";
  resume?: boolean;
};

export type UnsettledDelivery = {
  linkKey: string;
  deliveryTagKey: string;
  queueName?: string;
  state?: "received" | "accepted" | "released" | "modified" | "rejected" | "other";
  settled: boolean;
};

export type LinkRef = {
  channel: number;
  handle: number;
};

export type SessionFlowState = {
  initialOutgoingId: number;
  nextIncomingId: number;
  nextOutgoingId: number;
  incomingWindow: number;
  outgoingWindow: number;
  remoteIncomingWindow: number;
  remoteOutgoingWindow: number;
};

export type ConnectionTransportState =
  | "START"
  | "HDR_EXCH"
  | "SASL_MECH_SENT"
  | "SASL_DONE"
  | "OPENED"
  | "CLOSE_SENT"
  | "END";

export type EmulatorConnection = {
  id: string;
  transportState: ConnectionTransportState;
  protocolId?: number;
  nextDeliveryId: number;
  nextOutboundDeliveryIdByChannel: Map<number, number>;
  activeSessions: Set<number>;
  sessionFlowByChannel: Map<number, SessionFlowState>;
  linksByHandle: Map<string, LinkState>;
  pendingDeliveries: Map<string, PendingDelivery>;
  deliveryIdToTag: Map<string, string>;
  unsettledDeliveries: Map<string, UnsettledDelivery>;
  ongoingTransfersByLink: Map<string, OngoingTransfer>;
  cbsSender?: LinkRef;
  cbsReceiver?: LinkRef;
  managementLinksBySession: Map<number, { sender?: LinkRef; receiver?: LinkRef }>;
};
