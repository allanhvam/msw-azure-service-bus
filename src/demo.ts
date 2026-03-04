import { ServiceBusClient } from "@azure/service-bus";
import { setupServer } from "msw/node";
import { handlers } from "./handlers.js";

const server = setupServer(...handlers({
	options: {
		verbose: false,
	},
}));

server.listen({ onUnhandledRequest: "bypass" });

const connectionString =
	"Endpoint=sb://mock.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mock-key";

const client = new ServiceBusClient(connectionString, {
	webSocketOptions: {
		webSocket: WebSocket,
	},
});

try {
	const queueName = "orders";
	const sender = client.createSender(queueName);
	await sender.sendMessages({ messageId: "1", body: "hello" });
	await sender.close();

	const receiver = client.createReceiver(queueName, { receiveMode: "receiveAndDelete" });
	const messages = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10_000 });
	await receiver.close();

	console.log(messages[0]?.body);
} finally {
	await client.close();
	server.close();
}