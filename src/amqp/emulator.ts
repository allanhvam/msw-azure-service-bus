import type {
  EmulatorConnection,
  LinkState,
  OngoingTransfer,
  PendingDelivery,
  QueueMessage,
  SessionFlowState,
} from "./types/emulator.js";
import {
  concatBytes,
  decodeTransferMessage,
  decodeTransferMessages,
  encodeAmqpFrame,
  encodeAmqpMessage,
  encodeBinary,
  encodeBoolean,
  encodeDescribedList,
  encodeNull,
  encodeUInt,
  parseAttachFrame,
  parseDetachFrame,
  parseDispositionFrame,
  parseFlowFrame,
  parseTransferFrame,
  validateBareMessageSections,
  validateInboundMessageAnnotations,
} from "./codec.js";
import type { ClientConnection } from "./emulator/client.js";
import {
  sendAttach,
  sendAttachResponse,
  sendBegin,
  sendBinary,
  sendCbsResponseTransfer,
  sendClose,
  sendDetach,
  sendDisposition,
  sendDispositionAck,
  sendDispositionReleased,
  sendEnd,
  sendEndWithError,
  sendFlow,
  sendLinkFlow,
  sendManagementResponseTransfer,
  sendOpen,
  sendSaslMechanisms,
  sendSaslOutcome,
} from "./emulator/responders.js";
import { AMQP_PROTOCOL_VERSION, isAmqpHeader, parseAmqpProtocolHeader } from "./frame.js";
import type { AmqpFrame, ParsedDetach, ParsedDisposition, ParsedFlow, ParsedTransfer } from "./types/protocol.js";

const textEncoder = new TextEncoder();

type EmulatorOptions = {
  debugEnabled?: boolean;
  lockDurationInMs?: number;
  maxDeliveryCount?: number;
};

export class AmqpProtocolEmulator {
  private readonly connections = new Map<string, EmulatorConnection>();
  private readonly queues: Map<string, QueueMessage[]>;
  private options: Required<EmulatorOptions> = {
    debugEnabled: false,
    lockDurationInMs: 2_000,
    maxDeliveryCount: 10,
  };
  private readonly defaultSessionWindow = 5_000;

  constructor(queues: Map<string, QueueMessage[]>) {
    this.queues = queues;
  }

  setOptions(options: EmulatorOptions): void {
    if (typeof options.debugEnabled === "boolean") {
      this.options.debugEnabled = options.debugEnabled;
    }

    if (typeof options.lockDurationInMs === "number" && Number.isFinite(options.lockDurationInMs)) {
      this.options.lockDurationInMs = Math.max(250, options.lockDurationInMs);
    }

    if (typeof options.maxDeliveryCount === "number" && Number.isFinite(options.maxDeliveryCount)) {
      this.options.maxDeliveryCount = Math.max(1, Math.floor(options.maxDeliveryCount));
    }
  }

  private logDebug(message: string, payload?: unknown): void {
    if (!this.options.debugEnabled) {
      return;
    }

    if (payload === undefined) {
      console.log(message);
      return;
    }

    console.log(message, payload);
  }

  private getLinkKey(channel: number, handle: number): string {
    return `${channel}:${handle}`;
  }

  private allocateOutboundDeliveryId(connection: EmulatorConnection, channel: number): number {
    const next = connection.nextOutboundDeliveryIdByChannel.get(channel) ?? 0;
    connection.nextOutboundDeliveryIdByChannel.set(channel, next + 1);
    return next;
  }

  private canSendFrames(connection: EmulatorConnection | undefined): boolean {
    return !!connection && connection.transportState !== "CLOSE_SENT" && connection.transportState !== "END";
  }

  private transitionToClose(
    connection: EmulatorConnection | undefined,
    client: ClientConnection,
    channel: number,
    condition: string,
    description: string,
  ): void {
    if (!connection || !this.canSendFrames(connection)) {
      return;
    }

    sendClose(client, channel, { condition, description });
    connection.transportState = "CLOSE_SENT";
    this.clearAllPendingDeliveries(connection);
    connection.activeSessions.clear();
    connection.sessionFlowByChannel.clear();
    connection.linksByHandle.clear();
    connection.cbsReceiver = undefined;
    connection.cbsSender = undefined;
    connection.managementLinksBySession.clear();
  }

  private sendNegotiationRejection(client: ClientConnection, protocolId: number): void {
    const acceptedProtocolId = protocolId === 0x03 ? 0x03 : 0x00;
    const responseHeader = new Uint8Array([
      0x41,
      0x4d,
      0x51,
      0x50,
      acceptedProtocolId,
      AMQP_PROTOCOL_VERSION.major,
      AMQP_PROTOCOL_VERSION.minor,
      AMQP_PROTOCOL_VERSION.revision,
    ]);

    sendBinary(client, responseHeader);
    client.close?.();
  }

  private requiresOpen(connection: EmulatorConnection, client: ClientConnection, channel: number, performative?: string): boolean {
    const isOpenReady = connection.transportState === "HDR_EXCH" || connection.transportState === "SASL_DONE" || connection.transportState === "OPENED";
    if (isOpenReady) {
      return true;
    }

    this.transitionToClose(
      connection,
      client,
      channel,
      "amqp:illegal-state",
      `Received ${performative ?? "unknown"} before connection open`,
    );
    return false;
  }

  private requireActiveSession(connection: EmulatorConnection, client: ClientConnection, channel: number, performative: string): boolean {
    if (connection.activeSessions.has(channel)) {
      return true;
    }

    this.transitionToClose(
      connection,
      client,
      channel,
      "amqp:illegal-state",
      `Received ${performative} on channel ${channel} before begin`,
    );
    return false;
  }

  private createSessionFlowState(): SessionFlowState {
    return {
      initialOutgoingId: 1,
      nextIncomingId: 1,
      nextOutgoingId: 1,
      incomingWindow: this.defaultSessionWindow,
      outgoingWindow: this.defaultSessionWindow,
      remoteIncomingWindow: this.defaultSessionWindow,
      remoteOutgoingWindow: this.defaultSessionWindow,
    };
  }

  private getOrCreateSessionFlowState(connection: EmulatorConnection, channel: number): SessionFlowState {
    const existing = connection.sessionFlowByChannel.get(channel);
    if (existing) {
      return existing;
    }

    const created = this.createSessionFlowState();
    connection.sessionFlowByChannel.set(channel, created);
    return created;
  }

  private updateSessionFlowFromFlowFrame(session: SessionFlowState, flow: ParsedFlow | undefined): void {
    if (!flow) {
      return;
    }

    if (typeof flow.nextIncomingId === "number") {
      const remoteIncomingWindow = typeof flow.incomingWindow === "number" ? flow.incomingWindow : session.remoteIncomingWindow;
      session.remoteIncomingWindow = Math.max(0, (flow.nextIncomingId + remoteIncomingWindow) - session.nextOutgoingId);
    }

    if (typeof flow.nextOutgoingId === "number") {
      const remoteOutgoingWindow = typeof flow.outgoingWindow === "number" ? flow.outgoingWindow : session.remoteOutgoingWindow;
      session.remoteOutgoingWindow = Math.max(0, (flow.nextOutgoingId + remoteOutgoingWindow) - session.nextIncomingId);
    }
  }

  private updateSessionFlowAfterInboundTransfer(session: SessionFlowState): void {
    session.nextIncomingId += 1;
    session.incomingWindow = Math.max(0, session.incomingWindow - 1);
    session.remoteOutgoingWindow = Math.max(0, session.remoteOutgoingWindow - 1);
  }

  private updateSessionFlowAfterOutboundTransfer(session: SessionFlowState): void {
    session.nextOutgoingId += 1;
    session.outgoingWindow = Math.max(0, session.outgoingWindow - 1);
    session.remoteIncomingWindow = Math.max(0, session.remoteIncomingWindow - 1);
  }

  private endSessionWithError(
    connection: EmulatorConnection,
    client: ClientConnection,
    channel: number,
    condition: string,
    description: string,
  ): void {
    if (!this.canSendFrames(connection)) {
      return;
    }

    sendEndWithError(client, channel, condition, description);
    this.clearPendingDeliveriesForChannel(connection, channel);
    connection.activeSessions.delete(channel);
    connection.sessionFlowByChannel.delete(channel);
    connection.managementLinksBySession.delete(channel);

    for (const [linkKey, link] of connection.linksByHandle.entries()) {
      if (link.channel === channel) {
        connection.linksByHandle.delete(linkKey);
      }
    }
  }

  private normalizeAddress(address: string | undefined): string | undefined {
    if (!address) {
      return undefined;
    }

    const trimmed = address.trim();
    if (!trimmed) {
      return undefined;
    }

    const withoutPrefix = trimmed.replace(/^\/+/, "");
    const lower = withoutPrefix.toLowerCase();

    if (lower.startsWith("queues/")) {
      return withoutPrefix.slice("queues/".length);
    }

    return withoutPrefix;
  }

  private getQueue(address: string | undefined): QueueMessage[] | undefined {
    const normalized = this.normalizeAddress(address);
    if (!normalized) {
      return undefined;
    }

    const queue = this.queues.get(normalized) ?? [];
    this.queues.set(normalized, queue);
    return queue;
  }

  private getDeadLetterQueueName(queueName: string): string {
    return `${queueName}/$DeadLetterQueue`;
  }

  private moveToDeadLetter(queueName: string, message: QueueMessage, reason: string): void {
    const deadLetterQueueName = this.getDeadLetterQueueName(queueName);
    const deadLetterQueue = this.getQueue(deadLetterQueueName);
    if (!deadLetterQueue) {
      return;
    }

    deadLetterQueue.push({
      ...message,
      deadLetterReason: reason,
    });
  }

  private requeueOrDeadLetter(queueName: string, message: QueueMessage, reasonOnDeadLetter: string): void {
    const queue = this.getQueue(queueName);
    if (!queue) {
      return;
    }

    const nextDeliveryCount = message.deliveryCount + 1;
    if (nextDeliveryCount >= this.options.maxDeliveryCount) {
      this.moveToDeadLetter(queueName, {
        ...message,
        deliveryCount: nextDeliveryCount,
      }, reasonOnDeadLetter);
      return;
    }

    queue.unshift({
      ...message,
      deliveryCount: nextDeliveryCount,
    });
  }

  private enqueueFromTransfer(
    link: LinkState | undefined,
    transfer: ParsedTransfer,
    decodedMessage?: Record<string, unknown>,
  ): void {
    if (!link) {
      return;
    }

    const queue = this.getQueue(link.targetAddress ?? link.sourceAddress);
    if (!queue) {
      return;
    }

    const decoded = decodedMessage ?? decodeTransferMessage(transfer.payload);
    if (!decoded) {
      return;
    }

    const messageId = typeof decoded.message_id === "string"
      ? decoded.message_id
      : `mock-${Date.now()}-${Math.random().toString(16).slice(2)}`;

    const correlationId = typeof decoded.correlation_id === "string" ? decoded.correlation_id : undefined;
    const sessionId = typeof decoded.group_id === "string" ? decoded.group_id : undefined;
    const partitionKey = typeof decoded.reply_to_group_id === "string" ? decoded.reply_to_group_id : undefined;

    const annotations = decoded.message_annotations && typeof decoded.message_annotations === "object"
      ? decoded.message_annotations as Record<string, unknown>
      : undefined;
    const scheduledEnqueueRaw = annotations?.["x-opt-scheduled-enqueue-time"];
    const scheduledEnqueueTimeUtc = scheduledEnqueueRaw instanceof Date
      ? scheduledEnqueueRaw
      : (typeof scheduledEnqueueRaw === "number" ? new Date(scheduledEnqueueRaw) : undefined);

    const appProps = decoded.application_properties && typeof decoded.application_properties === "object" && !Array.isArray(decoded.application_properties)
      ? decoded.application_properties as Record<string, unknown>
      : undefined;

    // TTL: AMQP header section field or absolute_expiry_time − creation_time
    let timeToLive: number | undefined;
    const expiryRaw = decoded.absolute_expiry_time;
    const creationRaw = decoded.creation_time;
    if (typeof expiryRaw === "number" && typeof creationRaw === "number" && expiryRaw > creationRaw) {
      timeToLive = expiryRaw - creationRaw;
    } else if (expiryRaw instanceof Date && creationRaw instanceof Date && expiryRaw.getTime() > creationRaw.getTime()) {
      timeToLive = expiryRaw.getTime() - creationRaw.getTime();
    }

    queue.push({
      messageId,
      body: decoded.body,
      contentType: typeof decoded.content_type === "string" ? decoded.content_type : undefined,
      correlationId,
      sessionId,
      partitionKey,
      scheduledEnqueueTimeUtc,
      timeToLive,
      applicationProperties: appProps,
      deliveryCount: 0,
    });
  }

  private encodeTagKey(tag: Uint8Array | undefined, deliveryId: number | undefined): string | undefined {
    if (tag && tag.length > 0) {
      return `tag:${Array.from(tag, (value) => value.toString(16).padStart(2, "0")).join("")}`;
    }

    if (typeof deliveryId === "number") {
      return `delivery:${deliveryId}`;
    }

    return undefined;
  }

  private getDeliveryMapKey(channel: number, deliveryId: number): string {
    return `${channel}:${deliveryId}`;
  }

  private combinePayload(chunks: Uint8Array[]): Uint8Array {
    if (chunks.length === 0) {
      return new Uint8Array();
    }

    return concatBytes(...chunks);
  }

  private resolveTransferPayload(
    connection: EmulatorConnection,
    client: ClientConnection,
    channel: number,
    link: LinkState,
    transfer: ParsedTransfer,
  ): { payload?: Uint8Array; aborted: boolean; ignored: boolean } {
    const linkKey = this.getLinkKey(channel, link.handle);
    const deliveryId = transfer.deliveryId ?? connection.nextDeliveryId;
    const deliveryTagKey = this.encodeTagKey(transfer.deliveryTag, deliveryId);

    if (!deliveryTagKey) {
      return { payload: transfer.payload, aborted: !!transfer.aborted, ignored: false };
    }

    if (transfer.resume === true && !connection.unsettledDeliveries.has(deliveryTagKey)) {
      return { aborted: false, ignored: true };
    }

    const ongoing = connection.ongoingTransfersByLink.get(linkKey);
    if (ongoing && ongoing.deliveryTagKey !== deliveryTagKey) {
      this.transitionToClose(
        connection,
        client,
        channel,
        "amqp:not-allowed",
        "Interleaved multi-transfer deliveries are not permitted on the same link",
      );
      return { aborted: false, ignored: true };
    }

    const chunks = ongoing?.chunks ?? [];
    if (transfer.payload && transfer.payload.length > 0) {
      chunks.push(transfer.payload);
    }

    const resolvedOngoing: OngoingTransfer = {
      deliveryId,
      deliveryTagKey,
      chunks,
      messageFormat: transfer.messageFormat,
      settled: transfer.settled,
      state: transfer.state,
      resume: transfer.resume,
    };

    if (transfer.aborted) {
      connection.ongoingTransfersByLink.delete(linkKey);
      connection.deliveryIdToTag.set(this.getDeliveryMapKey(channel, deliveryId), deliveryTagKey);
      return { aborted: true, ignored: false };
    }

    if (transfer.more) {
      connection.ongoingTransfersByLink.set(linkKey, resolvedOngoing);
      connection.deliveryIdToTag.set(this.getDeliveryMapKey(channel, deliveryId), deliveryTagKey);
      connection.unsettledDeliveries.set(deliveryTagKey, {
        linkKey,
        deliveryTagKey,
        queueName: this.normalizeAddress(link.targetAddress),
        state: transfer.state ?? "received",
        settled: transfer.settled === true,
      });
      return { ignored: false, aborted: false };
    }

    connection.ongoingTransfersByLink.delete(linkKey);
    connection.deliveryIdToTag.set(this.getDeliveryMapKey(channel, deliveryId), deliveryTagKey);

    const payload = this.combinePayload(resolvedOngoing.chunks);
    if (transfer.settled === true) {
      connection.unsettledDeliveries.delete(deliveryTagKey);
    } else {
      connection.unsettledDeliveries.set(deliveryTagKey, {
        linkKey,
        deliveryTagKey,
        queueName: this.normalizeAddress(link.targetAddress),
        state: transfer.state,
        settled: false,
      });
    }

    return { payload, aborted: false, ignored: false };
  }

  private getPendingKey(channel: number, deliveryId: number): string {
    return `${channel}:${deliveryId}`;
  }

  private clearPendingDelivery(connection: EmulatorConnection, pendingKey: string): PendingDelivery | undefined {
    const pending = connection.pendingDeliveries.get(pendingKey);
    if (!pending) {
      return undefined;
    }

    if (pending.lockTimer) {
      clearTimeout(pending.lockTimer);
    }

    connection.pendingDeliveries.delete(pendingKey);
    return pending;
  }

  private requeuePendingDelivery(pending: PendingDelivery): void {
    const queue = this.getQueue(pending.queueName);
    if (!queue) {
      return;
    }

    this.requeueOrDeadLetter(pending.queueName, pending.message, "MaxDeliveryCountExceeded");
  }

  private clearAllPendingDeliveries(connection: EmulatorConnection): void {
    for (const pendingKey of Array.from(connection.pendingDeliveries.keys())) {
      const cleared = this.clearPendingDelivery(connection, pendingKey);
      if (cleared) {
        this.requeuePendingDelivery(cleared);
      }
    }
  }

  private clearPendingDeliveriesForChannel(connection: EmulatorConnection, channel: number): void {
    const keyPrefix = `${channel}:`;

    for (const pendingKey of Array.from(connection.pendingDeliveries.keys())) {
      if (!pendingKey.startsWith(keyPrefix)) {
        continue;
      }

      const cleared = this.clearPendingDelivery(connection, pendingKey);
      if (cleared) {
        this.requeuePendingDelivery(cleared);
      }
    }
  }

  private expireDueLocks(connection: EmulatorConnection, client: ClientConnection): void {
    if (!this.canSendFrames(connection)) {
      return;
    }

    const now = Date.now();

    for (const [pendingKey, pending] of connection.pendingDeliveries.entries()) {
      if (pending.lockExpiresAt > now) {
        continue;
      }

      const [channelPart, deliveryPart] = pendingKey.split(":");
      const channel = Number(channelPart);
      const deliveryId = Number(deliveryPart);
      if (Number.isFinite(channel) && Number.isFinite(deliveryId)) {
        sendDispositionReleased(client, channel, deliveryId);
      }

      const expired = this.clearPendingDelivery(connection, pendingKey);
      if (!expired) {
        continue;
      }

      const queue = this.getQueue(expired.queueName);
      if (!queue) {
        continue;
      }

      this.requeueOrDeadLetter(expired.queueName, expired.message, "MaxDeliveryCountExceeded");
    }
  }

  private scheduleLockExpiry(
    connection: EmulatorConnection,
    client: ClientConnection,
    channel: number,
    deliveryId: number,
    queueName: string,
    message: QueueMessage,
  ): void {
    const pendingKey = this.getPendingKey(channel, deliveryId);
    const lockExpiresAt = Date.now() + this.options.lockDurationInMs;

    const lockTimer = setTimeout(() => {
      const currentConnection = this.connections.get(connection.id);
      if (!currentConnection) {
        return;
      }

      if (!this.canSendFrames(currentConnection)) {
        return;
      }

      sendDispositionReleased(client, channel, deliveryId);

      const expired = this.clearPendingDelivery(currentConnection, pendingKey);
      if (!expired) {
        return;
      }

      const queue = this.getQueue(expired.queueName);
      if (!queue) {
        return;
      }

      this.requeueOrDeadLetter(expired.queueName, expired.message, "MaxDeliveryCountExceeded");
    }, this.options.lockDurationInMs);
    lockTimer.unref?.();

    connection.pendingDeliveries.set(pendingKey, {
      queueName,
      message,
      lockExpiresAt,
      lockTimer,
    });
  }

  private sendQueueTransfer(
    connection: EmulatorConnection,
    client: ClientConnection,
    channel: number,
    link: LinkState,
    deliveryId: number,
    message: QueueMessage,
  ): void {
    const isPeekLock = link.receiveMode === "peekLock";
    const encodedMessage = encodeAmqpMessage({
      ...message,
      messageAnnotations: {
        "x-opt-delivery-count": message.deliveryCount,
        ...(isPeekLock ? { "x-opt-locked-until": Date.now() + this.options.lockDurationInMs } : {}),
        ...(message.scheduledEnqueueTimeUtc ? { "x-opt-scheduled-enqueue-time": message.scheduledEnqueueTimeUtc.getTime() } : {}),
        ...(message.partitionKey ? { "x-opt-partition-key": message.partitionKey } : {}),
      },
      applicationProperties: {
        ...(message.applicationProperties ?? {}),
        ...(message.deadLetterReason ? { DeadLetterReason: message.deadLetterReason } : {}),
        ...(message.deadLetterDescription ? { DeadLetterErrorDescription: message.deadLetterDescription } : {}),
      },
    });
    const deliveryTag = textEncoder.encode(`msg-${deliveryId}`);

    const performative = encodeDescribedList(0x14, [
      encodeUInt(link.handle),
      encodeUInt(deliveryId),
      encodeBinary(deliveryTag),
      encodeUInt(0),
      encodeBoolean(!isPeekLock),
      encodeBoolean(false),
      encodeNull(),
      encodeNull(),
      encodeBoolean(false),
      encodeBoolean(false),
      encodeBoolean(false),
    ]);

    sendBinary(client, encodeAmqpFrame(channel, concatBytes(performative, encodedMessage)));

    const session = connection.sessionFlowByChannel.get(channel);
    if (session) {
      this.updateSessionFlowAfterOutboundTransfer(session);
      connection.sessionFlowByChannel.set(channel, session);
    }
  }

  private flushQueueToReceiver(
    connection: EmulatorConnection,
    client: ClientConnection,
    channel: number,
    link: LinkState,
    requestedCredit: number | undefined,
  ): void {
    if (typeof requestedCredit === "number") {
      link.availableCredit = Math.max(0, requestedCredit);
    }

    const queue = this.getQueue(link.sourceAddress ?? link.targetAddress);
    if (!queue || queue.length === 0) {
      return;
    }

    if (link.availableCredit <= 0) {
      return;
    }

    const maxCount = link.availableCredit;

    for (let index = 0; index < maxCount; index += 1) {
      const next = queue.shift();
      if (!next) {
        return;
      }

      const queueName = this.normalizeAddress(link.sourceAddress ?? link.targetAddress);
      if (!queueName) {
        return;
      }

      const deliveryId = this.allocateOutboundDeliveryId(connection, channel);
      this.sendQueueTransfer(connection, client, channel, link, deliveryId, next);
      if (link.receiveMode === "peekLock") {
        this.scheduleLockExpiry(connection, client, channel, deliveryId, queueName, next);
      }

      link.deliveryCount += 1;
      link.availableCredit -= 1;
    }
  }

  private flushEligibleReceivers(connection: EmulatorConnection, client: ClientConnection): void {
    for (const link of connection.linksByHandle.values()) {
      if (!link.role) {
        continue;
      }

      if (link.availableCredit <= 0) {
        continue;
      }

      this.flushQueueToReceiver(connection, client, link.channel, link, undefined);
    }
  }

  createConnection(connectionId: string): void {
    this.connections.set(connectionId, {
      id: connectionId,
      transportState: "START",
      nextDeliveryId: 0,
      nextOutboundDeliveryIdByChannel: new Map<number, number>(),
      activeSessions: new Set<number>(),
      sessionFlowByChannel: new Map<number, SessionFlowState>(),
      linksByHandle: new Map<string, LinkState>(),
      pendingDeliveries: new Map<string, PendingDelivery>(),
      deliveryIdToTag: new Map<string, string>(),
      unsettledDeliveries: new Map(),
      ongoingTransfersByLink: new Map(),
      managementLinksBySession: new Map(),
    });
  }

  removeConnection(connectionId: string): void {
    const connection = this.connections.get(connectionId);
    if (connection) {
      this.clearAllPendingDeliveries(connection);
    }

    this.connections.delete(connectionId);
  }

  reset(): void {
    for (const connectionId of Array.from(this.connections.keys())) {
      this.removeConnection(connectionId);
    }

    this.connections.clear();
  }

  // --- Performative Handlers ---

  private handleProtocolHeader(connection: EmulatorConnection, client: ClientConnection, bytes: Uint8Array): void {
    const protocolHeader = parseAmqpProtocolHeader(bytes);
    if (!protocolHeader) {
      this.sendNegotiationRejection(client, 0x00);
      connection.transportState = "END";
      return;
    }

    const hasSupportedVersion =
      protocolHeader.major === AMQP_PROTOCOL_VERSION.major
      && protocolHeader.minor === AMQP_PROTOCOL_VERSION.minor
      && protocolHeader.revision === AMQP_PROTOCOL_VERSION.revision;
    const hasSupportedProtocolId = protocolHeader.protocolId === 0x00 || protocolHeader.protocolId === 0x03;

    if (!hasSupportedVersion || !hasSupportedProtocolId) {
      this.sendNegotiationRejection(client, protocolHeader.protocolId);
      connection.transportState = "END";
      return;
    }

    connection.protocolId = protocolHeader.protocolId;
    sendBinary(client, bytes.slice(0, 8));
    if (protocolHeader.protocolId === 0x03) {
      sendSaslMechanisms(client);
      connection.transportState = "SASL_MECH_SENT";
    } else {
      connection.transportState = "HDR_EXCH";
    }
  }

  private handleSaslInit(connection: EmulatorConnection, client: ClientConnection, channel: number): void {
    if (connection.protocolId !== 0x03 || connection.transportState !== "SASL_MECH_SENT") {
      this.transitionToClose(connection, client, channel, "amqp:illegal-state", "Unexpected sasl-init frame");
      return;
    }

    sendSaslOutcome(client);
    connection.transportState = "SASL_DONE";
  }

  private handleOpen(connection: EmulatorConnection, client: ClientConnection, channel: number): void {
    if (channel !== 0) {
      this.transitionToClose(connection, client, channel, "amqp:connection:framing-error", "open must be sent on channel 0");
      return;
    }

    if (!this.requiresOpen(connection, client, channel, "open")) {
      return;
    }

    sendOpen(client, channel);
    connection.transportState = "OPENED";
  }

  private handleBegin(connection: EmulatorConnection, client: ClientConnection, channel: number): void {
    if (connection.transportState !== "OPENED") {
      this.transitionToClose(connection, client, channel, "amqp:illegal-state", "Received begin before open");
      return;
    }

    connection.activeSessions.add(channel);
    connection.sessionFlowByChannel.set(channel, this.createSessionFlowState());
    sendBegin(client, channel);
  }

  private handleAttach(connection: EmulatorConnection, client: ClientConnection, channel: number, frame: AmqpFrame): void {
    if (!this.requireActiveSession(connection, client, channel, "attach")) {
      return;
    }

    const attach = parseAttachFrame(frame.body);
    this.logDebug("[MSW AMQP] parsed attach", attach);

    if (attach && typeof attach.handle === "number") {
      const linkKey = this.getLinkKey(channel, attach.handle);
      if (connection.linksByHandle.has(linkKey)) {
        this.endSessionWithError(
          connection,
          client,
          channel,
          "amqp:session:handle-in-use",
          `Handle ${attach.handle} is already attached on channel ${channel}`,
        );
        return;
      }

      const receiveMode: LinkState["receiveMode"] = (attach.rcvSettleMode === 0 || attach.rcvSettleMode === undefined)
        ? "receiveAndDelete"
        : "peekLock";

      connection.linksByHandle.set(linkKey, {
        channel,
        handle: attach.handle,
        role: attach.role ?? false,
        deliveryCount: 0,
        availableCredit: 0,
        receiveMode,
        name: attach.name,
        sourceAddress: attach.sourceAddress,
        targetAddress: attach.targetAddress,
      });

      const isCbsAddress = attach.sourceAddress === "$cbs" || attach.targetAddress === "$cbs";
      if (attach.role === false && isCbsAddress) {
        connection.cbsSender = { channel, handle: attach.handle };
        this.logDebug("[MSW AMQP] cbsSenderHandle", connection.cbsSender);
      }

      if (attach.role === true && isCbsAddress) {
        connection.cbsReceiver = { channel, handle: attach.handle };
        this.logDebug("[MSW AMQP] cbsReceiverHandle", connection.cbsReceiver);
      }

      const isManagementAddress = (addr: string | undefined) => !!addr && addr.endsWith("/$management");
      if (isManagementAddress(attach.targetAddress) || isManagementAddress(attach.sourceAddress)) {
        const mgmt = connection.managementLinksBySession.get(channel) ?? {};
        if (attach.role === false) {
          mgmt.sender = { channel, handle: attach.handle };
        } else {
          mgmt.receiver = { channel, handle: attach.handle };
        }
        connection.managementLinksBySession.set(channel, mgmt);
        this.logDebug("[MSW AMQP] managementLink", { channel, role: attach.role, handle: attach.handle });
      }

      sendAttachResponse(client, channel, attach);

      if ((attach.role ?? false) === false) {
        sendLinkFlow(client, channel, attach.handle, 0, 1000);
      }

    } else {
      sendAttach(client, channel);
    }

    sendFlow(client, channel);
  }

  private handleFlow(connection: EmulatorConnection, client: ClientConnection, channel: number, frame: AmqpFrame): void {
    if (!this.requireActiveSession(connection, client, channel, "flow")) {
      return;
    }

    this.expireDueLocks(connection, client);

    const flow: ParsedFlow | undefined = parseFlowFrame(frame.body);
    const session = this.getOrCreateSessionFlowState(connection, channel);
    this.updateSessionFlowFromFlowFrame(session, flow);
    connection.sessionFlowByChannel.set(channel, session);

    if (typeof flow?.handle === "number") {
      const link = connection.linksByHandle.get(this.getLinkKey(channel, flow.handle));
      if (!link) {
        this.endSessionWithError(
          connection,
          client,
          channel,
          "amqp:session:unattached-handle",
          `Flow referenced unattached handle ${flow.handle} on channel ${channel}`,
        );
        return;
      }

      if (link && link.role) {
        this.flushQueueToReceiver(connection, client, channel, link, flow.linkCredit);

        if (flow.drain === true && link.availableCredit > 0) {
          link.deliveryCount += link.availableCredit;
          link.availableCredit = 0;
          sendLinkFlow(client, channel, link.handle, link.deliveryCount, 0);
        } else if (flow.echo === true) {
          sendLinkFlow(client, channel, link.handle, link.deliveryCount, link.availableCredit);
        }
      }

      if (link && !link.role) {
        sendLinkFlow(client, channel, link.handle, link.deliveryCount, 1000);
      }
    }

    sendFlow(client, channel);
  }

  private handleTransfer(connection: EmulatorConnection, client: ClientConnection, channel: number, frame: AmqpFrame): void {
    if (!this.requireActiveSession(connection, client, channel, "transfer")) {
      return;
    }

    const transfer: ParsedTransfer | undefined = parseTransferFrame(frame.body);
    const session = this.getOrCreateSessionFlowState(connection, channel);

    if (session.remoteOutgoingWindow <= 0) {
      this.endSessionWithError(
        connection,
        client,
        channel,
        "amqp:session:window-violation",
        `Transfer exceeded remote outgoing window on channel ${channel}`,
      );
      return;
    }

    const deliveryId = transfer?.deliveryId ?? connection.nextDeliveryId;

    const transferHandle = transfer?.handle;
    const link = typeof transferHandle === "number" ? connection.linksByHandle.get(this.getLinkKey(channel, transferHandle)) : undefined;
    if (typeof transferHandle === "number" && !link) {
      this.endSessionWithError(
        connection,
        client,
        channel,
        "amqp:session:unattached-handle",
        `Transfer referenced unattached handle ${transferHandle} on channel ${channel}`,
      );
      return;
    }

    if (link?.role === true) {
      this.endSessionWithError(
        connection,
        client,
        channel,
        "amqp:not-allowed",
        `Transfer is not allowed on receiver link handle ${link.handle} on channel ${channel}`,
      );
      return;
    }

    const requestMessage = decodeTransferMessage(transfer?.payload);
    const operation = requestMessage?.application_properties && typeof requestMessage.application_properties === "object"
      ? (requestMessage.application_properties as Record<string, unknown>).operation
      : undefined;
    const isPutTokenRequest = operation === "put-token";

    if (isPutTokenRequest && typeof transferHandle === "number") {
      connection.cbsSender = { channel, handle: transferHandle };
    }

    const isCbsRequestLink =
      !!link && (link.targetAddress === "$cbs" || link.sourceAddress === "$cbs" || isPutTokenRequest);
    const isManagementLink = !!link && link.role === false && !!link.targetAddress && link.targetAddress.endsWith("/$management");
    const isQueueSendLink = !!link && link.role === false && this.normalizeAddress(link.targetAddress) !== undefined && link.targetAddress !== "$cbs" && !isManagementLink;

    if (!isQueueSendLink) {
      sendDisposition(client, channel, deliveryId, true);
    }

    if (isCbsRequestLink && connection.cbsReceiver) {
      const correlationId = requestMessage?.message_id ?? requestMessage?.correlation_id ?? `${deliveryId}`;

      this.logDebug("[MSW AMQP] sending cbs response", {
        channel,
        transferHandle,
        cbsSenderHandle: connection.cbsSender,
        cbsReceiverHandle: connection.cbsReceiver,
        correlationId,
      });

      const cbsResponseDeliveryId = this.allocateOutboundDeliveryId(connection, connection.cbsReceiver.channel);
      sendCbsResponseTransfer(
        client,
        connection.cbsReceiver.channel,
        connection.cbsReceiver.handle,
        cbsResponseDeliveryId,
        correlationId,
      );

      const cbsSession = connection.sessionFlowByChannel.get(connection.cbsReceiver.channel);
      if (cbsSession) {
        this.updateSessionFlowAfterOutboundTransfer(cbsSession);
        connection.sessionFlowByChannel.set(connection.cbsReceiver.channel, cbsSession);
      }
    }

    if (isManagementLink) {
      const mgmt = connection.managementLinksBySession.get(channel);
      if (mgmt?.receiver) {
        const correlationId: unknown = requestMessage?.message_id ?? requestMessage?.correlation_id ?? `${deliveryId}`;

        this.logDebug("[MSW AMQP] sending management response", {
          channel,
          correlationId,
        });

        const mgmtResponseDeliveryId = this.allocateOutboundDeliveryId(connection, mgmt.receiver.channel);
        sendManagementResponseTransfer(
          client,
          mgmt.receiver.channel,
          mgmt.receiver.handle,
          mgmtResponseDeliveryId,
          correlationId,
        );

        const mgmtSession = connection.sessionFlowByChannel.get(mgmt.receiver.channel);
        if (mgmtSession) {
          this.updateSessionFlowAfterOutboundTransfer(mgmtSession);
          connection.sessionFlowByChannel.set(mgmt.receiver.channel, mgmtSession);
        }
      }
    }

    if (isQueueSendLink && transfer && link) {
      const transferResolution = this.resolveTransferPayload(connection, client, channel, link, transfer);
      if (!transferResolution.ignored && !transferResolution.aborted && transferResolution.payload) {
        const sectionValidation = validateBareMessageSections(transferResolution.payload);
        if (!sectionValidation.isValid) {
          this.endSessionWithError(
            connection,
            client,
            channel,
            "amqp:decode-error",
            sectionValidation.error ?? "Malformed AMQP bare message",
          );
          return;
        }

        const decodedTransferMessages = decodeTransferMessages(transferResolution.payload);
        if (!decodedTransferMessages || decodedTransferMessages.length === 0) {
          this.endSessionWithError(
            connection,
            client,
            channel,
            "amqp:decode-error",
            "Malformed AMQP message metadata",
          );
          return;
        }

        for (const decodedTransferMessage of decodedTransferMessages) {
          const annotationValidation = validateInboundMessageAnnotations(decodedTransferMessage);
          if (!annotationValidation.isValid) {
            this.endSessionWithError(
              connection,
              client,
              channel,
              "amqp:not-allowed",
              annotationValidation.error ?? "Unsupported message-annotations",
            );
            return;
          }

          this.enqueueFromTransfer(link, {
            ...transfer,
            payload: transferResolution.payload,
          }, decodedTransferMessage);
        }

        sendDisposition(client, channel, deliveryId, true);
      } else if (!transferResolution.ignored) {
        sendDisposition(client, channel, deliveryId, true);
      }

      this.flushEligibleReceivers(connection, client);
    }

    this.updateSessionFlowAfterInboundTransfer(session);
    connection.sessionFlowByChannel.set(channel, session);
    connection.nextDeliveryId += 1;
  }

  private handleClose(connection: EmulatorConnection, client: ClientConnection, channel: number): void {
    connection.transportState = "CLOSE_SENT";
    this.clearAllPendingDeliveries(connection);
    connection.activeSessions.clear();
    connection.sessionFlowByChannel.clear();
    connection.linksByHandle.clear();
    connection.cbsSender = undefined;
    connection.cbsReceiver = undefined;
    connection.managementLinksBySession.clear();
    sendClose(client, channel);
    connection.transportState = "END";
  }

  private handleDisposition(connection: EmulatorConnection, client: ClientConnection, channel: number, frame: AmqpFrame): void {
    if (!this.requireActiveSession(connection, client, channel, "disposition")) {
      return;
    }

    const disposition: ParsedDisposition | undefined = parseDispositionFrame(frame.body);

    if (disposition?.role === true && typeof disposition.first === "number") {
      const last = typeof disposition.last === "number" ? disposition.last : disposition.first;

      for (let deliveryId = disposition.first; deliveryId <= last; deliveryId += 1) {
        const pendingKey = `${channel}:${deliveryId}`;
        const pending = connection.pendingDeliveries.get(pendingKey);
        const isTerminal = disposition.settled === true
          || disposition.state === "accepted"
          || disposition.state === "released"
          || disposition.state === "modified"
          || disposition.state === "rejected";

        if (!isTerminal && disposition.state === "received") {
          const tagKey = connection.deliveryIdToTag.get(pendingKey);
          if (tagKey) {
            const existing = connection.unsettledDeliveries.get(tagKey);
            connection.unsettledDeliveries.set(tagKey, {
              linkKey: existing?.linkKey ?? "",
              deliveryTagKey: tagKey,
              queueName: existing?.queueName,
              state: "received",
              settled: false,
            });
          }

          continue;
        }

        const cleared = this.clearPendingDelivery(connection, pendingKey);
        if (!pending) {
          const tagKey = connection.deliveryIdToTag.get(pendingKey);
          if (tagKey && disposition.settled) {
            connection.unsettledDeliveries.delete(tagKey);
          } else if (tagKey) {
            const existing = connection.unsettledDeliveries.get(tagKey);
            if (existing) {
              existing.state = disposition.state;
              existing.settled = disposition.settled ?? existing.settled;
              connection.unsettledDeliveries.set(tagKey, existing);
            }
          }
          continue;
        }

        const tagKey = connection.deliveryIdToTag.get(pendingKey);

        if (cleared && (disposition.state === "released" || disposition.state === "modified")) {
          this.requeueOrDeadLetter(pending.queueName, pending.message, "MaxDeliveryCountExceeded");
        }

        if (cleared && disposition.state === "rejected") {
          this.moveToDeadLetter(
            pending.queueName,
            {
              ...pending.message,
              deliveryCount: pending.message.deliveryCount + 1,
            },
            "DeadLettered",
          );
        }

        if (tagKey) {
          if (disposition.settled === true || disposition.state === "accepted") {
            connection.unsettledDeliveries.delete(tagKey);
          } else if (disposition.state === "received") {
            const existing = connection.unsettledDeliveries.get(tagKey);
            connection.unsettledDeliveries.set(tagKey, {
              linkKey: existing?.linkKey ?? "",
              deliveryTagKey: tagKey,
              queueName: existing?.queueName,
              state: "received",
              settled: false,
            });
          } else {
            const existing = connection.unsettledDeliveries.get(tagKey);
            if (existing) {
              existing.state = disposition.state;
              existing.settled = disposition.settled ?? existing.settled;
              connection.unsettledDeliveries.set(tagKey, existing);
            }
          }
        }
      }

      sendDispositionAck(client, channel, disposition.first, last);
    }
  }

  private handleDetach(connection: EmulatorConnection, client: ClientConnection, channel: number, frame: AmqpFrame): void {
    if (!this.requireActiveSession(connection, client, channel, "detach")) {
      return;
    }

    const detach: ParsedDetach | undefined = parseDetachFrame(frame.body);
    const handle = detach?.handle;

    if (typeof handle === "number") {
      const linkKey = this.getLinkKey(channel, handle);
      if (!connection.linksByHandle.has(linkKey)) {
        this.endSessionWithError(
          connection,
          client,
          channel,
          "amqp:session:unattached-handle",
          `Detach referenced unattached handle ${handle} on channel ${channel}`,
        );
        return;
      }

      connection.linksByHandle.delete(linkKey);

      if (connection.cbsSender && connection.cbsSender.channel === channel && connection.cbsSender.handle === handle) {
        connection.cbsSender = undefined;
      }

      if (connection.cbsReceiver && connection.cbsReceiver.channel === channel && connection.cbsReceiver.handle === handle) {
        connection.cbsReceiver = undefined;
      }

      sendDetach(client, channel, handle, detach?.closed ?? true);
    }
  }

  private handleEnd(connection: EmulatorConnection, client: ClientConnection, channel: number): void {
    if (!this.requireActiveSession(connection, client, channel, "end")) {
      return;
    }

    this.clearPendingDeliveriesForChannel(connection, channel);
    connection.activeSessions.delete(channel);
    connection.sessionFlowByChannel.delete(channel);
    connection.managementLinksBySession.delete(channel);

    for (const [linkKey, link] of connection.linksByHandle.entries()) {
      if (link.channel === channel) {
        connection.linksByHandle.delete(linkKey);
      }
    }

    sendEnd(client, channel);
  }

  // --- Main Dispatch ---

  handleMessage(connectionId: string, client: ClientConnection, bytes: Uint8Array | undefined, frame: AmqpFrame | undefined): void {
    const connection = this.connections.get(connectionId);

    if (!connection) {
      return;
    }

    if (connection.transportState === "CLOSE_SENT" || connection.transportState === "END") {
      return;
    }

    if (bytes && isAmqpHeader(bytes)) {
      this.handleProtocolHeader(connection, client, bytes);
      return;
    }

    if (!frame) {
      return;
    }

    const { performative, channel } = frame;

    if (!performative) {
      this.transitionToClose(connection, client, channel, "amqp:decode-error", "Unable to resolve frame performative");
      return;
    }

    if (performative !== "sasl-init" && connection.transportState === "SASL_MECH_SENT") {
      this.transitionToClose(connection, client, channel, "amqp:illegal-state", "Expected sasl-init during SASL negotiation");
      return;
    }

    switch (performative) {
      case "sasl-init":   this.handleSaslInit(connection, client, channel); break;
      case "open":        this.handleOpen(connection, client, channel); break;
      case "begin":       this.handleBegin(connection, client, channel); break;
      case "attach":      this.handleAttach(connection, client, channel, frame); break;
      case "flow":        this.handleFlow(connection, client, channel, frame); break;
      case "transfer":    this.handleTransfer(connection, client, channel, frame); break;
      case "close":       this.handleClose(connection, client, channel); break;
      case "disposition":  this.handleDisposition(connection, client, channel, frame); break;
      case "detach":      this.handleDetach(connection, client, channel, frame); break;
      case "end":         this.handleEnd(connection, client, channel); break;
      default:
        this.transitionToClose(connection, client, channel, "amqp:not-implemented", `Performative not implemented: ${performative}`);
    }
  }
}
