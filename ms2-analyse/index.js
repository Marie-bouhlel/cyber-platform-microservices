"use strict";

const path = require("path");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const sqlite3 = require("sqlite3").verbose();
const { Kafka } = require("kafkajs");

const PROTO_PATH = path.join(__dirname, "proto", "analyse.proto");
const DB_PATH = path.join(__dirname, "ms2.db");
const GRPC_PORT = "0.0.0.0:50052";
const KAFKA_BROKERS = [process.env.KAFKA_BROKER || "kafka:9092"];
const CONSUMER_GROUP = "analyse-group";
const ALERTES_TOPIC = "nouvelles-alertes";
const INCIDENTS_TOPIC = "incidents-detectes";

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
	keepCase: true,
	longs: String,
	enums: String,
	defaults: true,
	oneofs: true,
});
const analyseProto = grpc.loadPackageDefinition(packageDefinition).analyse;

const db = new sqlite3.Database(DB_PATH);
db.run(
	"CREATE TABLE IF NOT EXISTS incidents (" +
		"id TEXT PRIMARY KEY, " +
		"alerte_id TEXT NOT NULL, " +
		"niveau_risque TEXT NOT NULL, " +
		"recommandation TEXT NOT NULL, " +
		"timestamp TEXT NOT NULL" +
	")",
	(error) => {
		if (error) {
			console.error("Erreur creation table incidents:", error.message);
		}
	}
);

const kafka = new Kafka({ clientId: "ms2-analyse", brokers: KAFKA_BROKERS });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });

function analyserSeverite(severite) {
	const value = (severite || "").toLowerCase();
	if (value === "critique") {
		return {
			niveau_risque: "ELEVEE",
			recommandation: "Bloquer IP immediatement",
		};
	}
	if (value === "haute") {
		return {
			niveau_risque: "MOYEN",
			recommandation: "Surveiller et investiguer",
		};
	}
	return {
		niveau_risque: "FAIBLE",
		recommandation: "Logger et continuer",
	};
}

function enregistrerIncidentAsync(alerteId, severite) {
	return new Promise((resolve, reject) => {
		const { niveau_risque, recommandation } = analyserSeverite(severite);
		const timestamp = new Date().toISOString();
		const id = `${Date.now()}-${Math.floor(Math.random() * 1e6)}`;

		db.run(
			"INSERT INTO incidents (id, alerte_id, niveau_risque, recommandation, timestamp) VALUES (?, ?, ?, ?, ?)",
			[id, alerteId, niveau_risque, recommandation, timestamp],
			(error) => {
				if (error) {
					reject(error);
					return;
				}
				resolve({ id, niveau_risque, recommandation, timestamp });
			}
		);
	});
}

async function analyserIncident(call, callback) {
	try {
		const { alerte_id, severite } = call.request;
		const incident = await enregistrerIncidentAsync(alerte_id, severite);

		callback(null, {
			incident_id: incident.id,
			niveau_risque: incident.niveau_risque,
			recommandation: incident.recommandation,
			succes: true,
		});
	} catch (error) {
		callback(null, {
			incident_id: "",
			niveau_risque: "",
			recommandation: "",
			succes: false,
		});
	}
}

function getIncidents(call, callback) {
	const { niveau_risque } = call.request;
	const sql = niveau_risque
		? "SELECT id, alerte_id, niveau_risque, recommandation, timestamp FROM incidents WHERE niveau_risque = ?"
		: "SELECT id, alerte_id, niveau_risque, recommandation, timestamp FROM incidents";
	const params = niveau_risque ? [niveau_risque] : [];

	db.all(sql, params, (error, rows) => {
		if (error) {
			callback({ code: grpc.status.INTERNAL, message: `Erreur GetIncidents: ${error.message}` });
			return;
		}
		callback(null, { incidents: rows || [] });
	});
}

async function startKafkaConsumer() {
	await consumer.connect();
	await consumer.subscribe({ topic: ALERTES_TOPIC, fromBeginning: false });

	await consumer.run({
		eachMessage: async ({ message }) => {
			try {
				const payload = JSON.parse(message.value.toString());
				const { alerte_id, severite } = payload;

				const incident = await enregistrerIncidentAsync(alerte_id, severite);

				await producer.send({
					topic: INCIDENTS_TOPIC,
					messages: [
						{
							value: JSON.stringify({
								incident_id: incident.id,
								alerte_id,
								niveau_risque: incident.niveau_risque,
								recommandation: incident.recommandation,
								timestamp: incident.timestamp,
							}),
						},
					],
				});
			} catch (error) {
				console.error("Erreur Kafka traitement:", error.message);
			}
		},
	});
}

async function main() {
	await producer.connect();

	const server = new grpc.Server();
	server.addService(analyseProto.AnalyseService.service, {
		AnalyserIncident: analyserIncident,
		GetIncidents: getIncidents,
	});

	server.bindAsync(GRPC_PORT, grpc.ServerCredentials.createInsecure(), (error) => {
		if (error) {
			console.error("Erreur de demarrage gRPC:", error.message);
			return;
		}
		console.log(`ms2-analyse gRPC ecoute sur ${GRPC_PORT}`);
		server.start();
	});

	await startKafkaConsumer();
}

main().catch((error) => {
	console.error("Erreur fatale:", error.message);
	process.exit(1);
});
