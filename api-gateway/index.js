"use strict";

const express = require("express");
const { ApolloServer } = require("@apollo/server");
const { startStandaloneServer } = require("@apollo/server/standalone");
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const PORT = 3000;
const APOLLO_PORT = 4000;
const PROTO_OPTIONS = {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
};

const collecteDefinition = protoLoader.loadSync(
  path.join(__dirname, 'proto', 'collecte.proto'), PROTO_OPTIONS
);
const analyseDefinition = protoLoader.loadSync(
  path.join(__dirname, 'proto', 'analyse.proto'), PROTO_OPTIONS
);
const notificationDefinition = protoLoader.loadSync(
  path.join(__dirname, 'proto', 'notification.proto'), PROTO_OPTIONS
);

const collecteProto = grpc.loadPackageDefinition(collecteDefinition).collecte;
const analyseProto = grpc.loadPackageDefinition(analyseDefinition).analyse;
const notificationProto = grpc.loadPackageDefinition(notificationDefinition).notification;

const ms1Client = new collecteProto.CollecteService(
  process.env.MS1_HOST || 'ms1-collecte:50051',
  grpc.credentials.createInsecure()
);
const ms2Client = new analyseProto.AnalyseService(
  process.env.MS2_HOST || 'ms2-analyse:50052',
  grpc.credentials.createInsecure()
);
const ms3Client = new notificationProto.NotificationService(
  process.env.MS3_HOST || 'ms3-notification:50053',
  grpc.credentials.createInsecure()
);

function grpcCall(client, method, payload) {
	return new Promise((resolve, reject) => {
		client[method](payload, (error, response) => {
			if (error) {
				reject(error);
				return;
			}
			resolve(response);
		});
	});
}

async function main() {
	const app = express();
	app.use(express.json());

	app.post("/api/alertes", async (req, res, next) => {
		try {
			const response = await grpcCall(ms1Client, "EnvoyerAlerte", req.body);
			res.json(response);
		} catch (error) {
			next(error);
		}
	});

	app.get("/api/alertes", async (req, res, next) => {
		try {
			const severite = req.query.severite || "";
			const response = await grpcCall(ms1Client, "ListerAlertes", { severite });
			res.json(response);
		} catch (error) {
			next(error);
		}
	});

	app.delete("/api/alertes/:id", async (req, res, next) => {
		try {
			const response = await grpcCall(ms1Client, "SupprimerAlerte", { id: req.params.id });
			res.json(response);
		} catch (error) {
			next(error);
		}
	});

	app.get("/api/incidents", async (req, res, next) => {
		try {
			const niveau_risque = req.query.niveau_risque || "";
			const response = await grpcCall(ms2Client, "GetIncidents", { niveau_risque });
			res.json(response);
		} catch (error) {
			next(error);
		}
	});

	app.get("/api/rapports", async (req, res, next) => {
		try {
			const response = await grpcCall(ms3Client, "GetRapports", {});
			res.json(response);
		} catch (error) {
			next(error);
		}
	});

	const typeDefs = `#graphql
		type Query {
			alertes(severite: String): [Alerte]
			incidents(niveau_risque: String): [Incident]
			rapports: [Rapport]
		}
		type Mutation {
			envoyerAlerte(source_ip: String!, type_attaque: String!, severite: String!, description: String!): AlerteResponse
		}
		type Alerte { id: String, source_ip: String, type_attaque: String, severite: String, description: String, timestamp: String }
		type Incident { id: String, alerte_id: String, niveau_risque: String, recommandation: String, timestamp: String }
		type Rapport { id: String, incident_id: String, message: String, timestamp: String }
		type AlerteResponse { id: String, succes: Boolean, message: String }
	`;

	const resolvers = {
		Query: {
			alertes: async (_, args) => {
				const response = await grpcCall(ms1Client, "ListerAlertes", {
					severite: args.severite || "",
				});
				return response.alertes || [];
			},
			incidents: async (_, args) => {
				const response = await grpcCall(ms2Client, "GetIncidents", {
					niveau_risque: args.niveau_risque || "",
				});
				return response.incidents || [];
			},
			rapports: async () => {
				const response = await grpcCall(ms3Client, "GetRapports", {});
				return response.rapports || [];
			},
		},
		Mutation: {
			envoyerAlerte: async (_, args) => {
				const response = await grpcCall(ms1Client, "EnvoyerAlerte", args);
				return response;
			},
		},
	};

	const apolloServer = new ApolloServer({ typeDefs, resolvers });

	app.use((err, req, res, next) => {
		console.error("Erreur API:", err.message);
		res.status(500).json({ error: "Erreur interne du serveur" });
	});

	await Promise.all([
		startStandaloneServer(apolloServer, {
			listen: { port: APOLLO_PORT },
		}).then(() => {
			console.log(`apollo-server ecoute sur http://localhost:${APOLLO_PORT}`);
		}),
		new Promise((resolve) => {
			app.listen(PORT, () => {
				console.log(`api-gateway ecoute sur http://localhost:${PORT}`);
				resolve();
			});
		}),
	]);
}

main().catch((error) => {
	console.error("Erreur fatale:", error.message);
	process.exit(1);
});
