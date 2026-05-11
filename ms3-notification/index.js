"use strict";

const path = require("path");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const { Kafka } = require("kafkajs");

const PROTO_PATH = path.join(__dirname, "proto", "notification.proto");
const GRPC_PORT = "0.0.0.0:50053";
const KAFKA_BROKERS = [process.env.KAFKA_BROKER || "kafka:9092"];
const CONSUMER_GROUP = "notif-group";
const INCIDENTS_TOPIC = "incidents-detectes";

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
	keepCase: true,
	longs: String,
	enums: String,
	defaults: true,
	oneofs: true,
});
const notificationProto = grpc.loadPackageDefinition(packageDefinition).notification;

const rapportSchema = {
	version: 0,
	primaryKey: "id",
	type: "object",
	properties: {
		id: { type: "string" },
		incident_id: { type: "string" },
		message: { type: "string" },
		timestamp: { type: "string" },
	},
	required: ["id", "incident_id", "message", "timestamp"],
};

const kafka = new Kafka({ clientId: "ms3-notification", brokers: KAFKA_BROKERS });
const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });

async function initDatabase() {
	// Simple in-memory store to avoid external DB/storage dependencies.
	return { rapports: [] };
}

async function creerRapport(rapports, incidentId, message) {
	const id = `${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
	const timestamp = new Date().toISOString();

	const doc = {
		id,
		incident_id: incidentId,
		message,
		timestamp,
	};

	rapports.push(doc);
	return doc;
}

function buildNotificationMessage(payload) {
	const { incident_id, niveau_risque, recommandation } = payload;
	return `Incident ${incident_id} | Risque: ${niveau_risque} | Action: ${recommandation}`;
}

async function envoyerNotification(rapports, call, callback) {
	try {
		const { incident_id, message } = call.request;
		const rapport = await creerRapport(rapports, incident_id, message);

		console.log(`[NOTIF] Rapport cree: ${rapport.id} pour incident ${incident_id}`);

		callback(null, { succes: true, rapport_id: rapport.id });
	} catch (error) {
		callback(null, { succes: false, rapport_id: "" });
	}
}

async function getRapports(rapports, call, callback) {
	try {
		callback(null, { rapports });
	} catch (error) {
		callback({ code: grpc.status.INTERNAL, message: `Erreur GetRapports: ${error.message}` });
	}
}

async function startKafkaConsumer(rapports) {
	await consumer.connect();
	await consumer.subscribe({ topic: INCIDENTS_TOPIC, fromBeginning: false });

	await consumer.run({
		eachMessage: async ({ message }) => {
			try {
				const payload = JSON.parse(message.value.toString());
				const notifMessage = buildNotificationMessage(payload);
				const rapport = await creerRapport(rapports, payload.incident_id, notifMessage);

				console.log(
					`[NOTIF] Incident ${payload.incident_id} traite | Rapport ${rapport.id} | ${notifMessage}`
				);
			} catch (error) {
				console.error("Erreur Kafka traitement:", error.message);
			}
		},
	});
}

async function main() {
	const { rapports } = await initDatabase();

	const server = new grpc.Server();
	server.addService(notificationProto.NotificationService.service, {
		EnvoyerNotification: envoyerNotification.bind(null, rapports),
		GetRapports: getRapports.bind(null, rapports),
	});

	server.bindAsync(GRPC_PORT, grpc.ServerCredentials.createInsecure(), (error) => {
		if (error) {
			console.error("Erreur de demarrage gRPC:", error.message);
			return;
		}
		console.log(`ms3-notification gRPC ecoute sur ${GRPC_PORT}`);
		server.start();
	});

	await startKafkaConsumer(rapports);
}

main().catch((error) => {
	console.error("Erreur fatale:", error.message);
	process.exit(1);
});
