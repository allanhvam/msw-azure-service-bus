# msw-azure-service-bus

TypeScript Azure Service Bus WebSocket AMQP emulator for local testing with MSW

> Disclaimer: Primarily coded by Codex and Opus, the emulator is not feature complete, eg there is no support for topics/subscriptions.

## Usage

1. Install `msw` (https://mswjs.io/docs/getting-started)
2. Install `msw-azure-service-bus`
3. Import `handlers` from `msw-azure-service-bus` and pass the handlers to your MSW setup

### Setup

```ts
import { setupServer } from "msw/node";
import { handlers } from "msw-azure-service-bus";

export const server = setupServer(
	...handlers({
		options: {
			verbose: false,
			lockDurationInMs: 60_000,
			maxDeliveryCount: 10,
		},
	}),
);
```

### `@azure/service-bus` WebSocket requirement

`msw-azure-service-bus` emulates the Service Bus WebSocket AMQP endpoint (`wss://.../$servicebus/websocket`).
When creating `ServiceBusClient`, configure `webSocketOptions` so the SDK uses WebSockets.

```ts
import { ServiceBusClient } from "@azure/service-bus";

const connectionString =
	"Endpoint=sb://mock.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mock-key";

const client = new ServiceBusClient(connectionString, {
	webSocketOptions: {
		webSocket: WebSocket,
	},
});

const queueName = "orders";
const sender = client.createSender(queueName);
await sender.sendMessages({ messageId: "1", body: "hello" });
await sender.close();

const receiver = client.createReceiver(queueName, { receiveMode: "receiveAndDelete" });
const messages = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10_000 });
await receiver.close();

console.log(messages[0]?.body);
await client.close();
```

## Options

The `handlers` function accepts an `options` object:

- `verbose` (`boolean`): Enables emulator request logging (default: `false`)
- `lockDurationInMs` (`number`): Lock duration for emulated queue messages (default: `60000`)
- `maxDeliveryCount` (`number`): Max delivery attempts before dead-letter behavior (default: `10`)
