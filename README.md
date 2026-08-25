# Cyber Platform Microservices

Plateforme de detection d'incidents cyber construite en architecture microservices avec gRPC, Kafka, REST et GraphQL.

![Cyber Platform Cover Placeholder](docs/images/cyber-platform-cover-placeholder.png)

## Vue d'ensemble

Le flux metier suit trois services:

1. MS1 collecte les alertes
2. MS2 analyse la gravite et detecte des incidents
3. MS3 produit des rapports de notification

Un API Gateway central expose REST et GraphQL.

## Architecture

![Cyber Platform Architecture Placeholder](docs/images/cyber-platform-architecture-placeholder.png)

```text
Client
  -> API Gateway (REST + GraphQL)
    -> MS1 Collecte (gRPC)
      -> Kafka: nouvelles-alertes
        -> MS2 Analyse (gRPC)
          -> Kafka: incidents-detectes
            -> MS3 Notification (gRPC)
```

## Services et ports

- API Gateway REST: `3000`
- API Gateway GraphQL: `4000`
- MS1 gRPC: `50051`
- MS2 gRPC: `50052`
- MS3 gRPC: `50053`
- Kafka: `9092`
- Zookeeper: `2181`

## Demarrage avec Docker

```bash
docker-compose up --build
```

## Endpoints REST

- `POST /api/alertes`
- `GET /api/alertes`
- `DELETE /api/alertes/:id`
- `GET /api/incidents`
- `GET /api/rapports`

Base URL: `http://localhost:3000`

## GraphQL

Endpoint: `http://localhost:4000/graphql`

- Query: `alertes`, `incidents`, `rapports`
- Mutation: `envoyerAlerte(...)`

## Captures (placeholders)

![Cyber Platform API Placeholder](docs/images/cyber-platform-api-placeholder.png)
![Cyber Platform Monitoring Placeholder](docs/images/cyber-platform-monitoring-placeholder.png)

## Evolutions possibles

- Authentification centralisee sur le gateway
- Observabilite (metrics, traces, logs)
- Circuit breaking et retries inter-services
