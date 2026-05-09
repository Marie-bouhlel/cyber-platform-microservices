"use strict";

const path = require("path");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const sqlite3 = require("sqlite3").verbose();
const { Kafka } = require("kafkajs");

const PROTO_PATH = path.join(__dirname, "proto", "collecte.proto");
const DB_PATH = path.join(__dirname, "ms1.db");
const GRPC_PORT = "0.0.0.0:50051";
const KAFKA_BROKERS = [process.env.KAFKA_BROKER || "kafka:9092"];
const KAFKA_TOPIC = "nouvelles-alertes";

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
	keepCase: true,
	longs: String,
	enums: String,
	defaults: true,
	oneofs: true,
});
const collecteProto = grpc.loadPackageDefinition(packageDefinition).collecte;

const db = new sqlite3.Database(DB_PATH);
db.run(
	"CREATE TABLE IF NOT EXISTS alertes (" +
		"id TEXT PRIMARY KEY, " +
		"source_ip TEXT NOT NULL, " +
		"type_attaque TEXT NOT NULL, " +
		"severite TEXT NOT NULL, " +
		"description TEXT NOT NULL, " +
		"timestamp TEXT NOT NULL" +
	")",
	(error) => {
		if (error) {
			console.error("Erreur creation table alertes:", error.message);
		}
	}
);

const kafka = new Kafka({ clientId: "ms1-collecte", brokers: KAFKA_BROKERS });
const producer = kafka.producer();
let producerReady = false;
let producerConnecting = null;

async function connectProducerWithRetry(maxAttempts = 30, delayMs = 2000) {
	for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
		try {
			await producer.connect();
			producerReady = true;
			console.log("Kafka producer connecte");
			return;
		} catch (error) {
			console.error(
				`Echec connexion Kafka (tentative ${attempt}/${maxAttempts}): ${error.message}`
			);
			if (attempt === maxAttempts) {
				throw error;
			}
			await new Promise((resolve) => setTimeout(resolve, delayMs));
		}
	}
}

async function ensureProducerConnected() {
	if (producerReady) {
		return;
	}
	if (!producerConnecting) {
		producerConnecting = connectProducerWithRetry();
	}
	await producerConnecting;
}

function envoyerAlerte(call, callback) {
	const { source_ip, type_attaque, severite, description } = call.request;
	const timestamp = new Date().toISOString();
	const id = `${Date.now()}-${Math.floor(Math.random() * 1e6)}`;

	db.run(
		"INSERT INTO alertes (id, source_ip, type_attaque, severite, description, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
		[id, source_ip, type_attaque, severite, description, timestamp],
		async (error) => {
			if (error) {
				callback(null, {
					id: "",
					succes: false,
					message: `Erreur EnvoyerAlerte: ${error.message}`,
				});
				return;
			}

			try {
				await ensureProducerConnected();
				await producer.send({
					topic: KAFKA_TOPIC,
					messages: [
						{
							value: JSON.stringify({
								alerte_id: id,
								type_attaque,
								severite,
								description,
							}),
						},
					],
				});

				callback(null, { id, succes: true, message: "Alerte enregistree" });
			} catch (sendError) {
				callback(null, {
					id: "",
					succes: false,
					message: `Erreur EnvoyerAlerte: ${sendError.message}`,
				});
			}
		}
	);
}

function listerAlertes(call, callback) {
	const { severite } = call.request;
	const sql = severite
		? "SELECT id, source_ip, type_attaque, severite, description, timestamp FROM alertes WHERE severite = ?"
		: "SELECT id, source_ip, type_attaque, severite, description, timestamp FROM alertes";
	const params = severite ? [severite] : [];

	db.all(sql, params, (error, rows) => {
		if (error) {
			callback({ code: grpc.status.INTERNAL, message: `Erreur ListerAlertes: ${error.message}` });
			return;
		}
		callback(null, { alertes: rows || [] });
	});
}

function supprimerAlerte(call, callback) {
	const { id } = call.request;
	db.run("DELETE FROM alertes WHERE id = ?", [id], function (error) {
		if (error) {
			callback(null, {
				succes: false,
				message: `Erreur SupprimerAlerte: ${error.message}`,
			});
			return;
		}
		if (this.changes === 0) {
			callback(null, { succes: false, message: "Alerte introuvable" });
			return;
		}
		callback(null, { succes: true, message: "Alerte supprimee" });
	});
}

async function main() {
	const server = new grpc.Server();
	server.addService(collecteProto.CollecteService.service, {
		EnvoyerAlerte: envoyerAlerte,
		ListerAlertes: listerAlertes,
		SupprimerAlerte: supprimerAlerte,
	});

	server.bindAsync(GRPC_PORT, grpc.ServerCredentials.createInsecure(), (error) => {
		if (error) {
			console.error("Erreur de demarrage gRPC:", error.message);
			return;
		}
		console.log(`ms1-collecte gRPC ecoute sur ${GRPC_PORT}`);
		server.start();
	});

	connectProducerWithRetry().catch((error) => {
		console.error("Kafka indisponible apres plusieurs tentatives:", error.message);
	});
}

main().catch((error) => {
	console.error("Erreur fatale:", error.message);
	process.exit(1);
});
