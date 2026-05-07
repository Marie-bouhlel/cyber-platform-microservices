# cyber-platform

Plateforme de detection d'incidents de cybersecurite basee sur une architecture microservices avec Node.js, gRPC, Kafka, REST et GraphQL.

## Description

Le systeme collecte des alertes (MS1), analyse leur gravite (MS2), puis cree des rapports de notification (MS3). Un API Gateway expose REST et GraphQL pour interagir avec l'ensemble.

## Architecture

Client REST/GraphQL
  -> API Gateway (Express + Apollo Server)
    -> MS1 Collecte (gRPC, SQLite)
      -> Kafka topic: nouvelles-alertes
        -> MS2 Analyse (gRPC, SQLite)
          -> Kafka topic: incidents-detectes
            -> MS3 Notification (gRPC, RxDB)

## Services et ports

- api-gateway: REST sur 3000, GraphQL sur 4000
- ms1-collecte: gRPC 50051
- ms2-analyse: gRPC 50052
- ms3-notification: gRPC 50053
- kafka: 9092, zookeeper: 2181

## Endpoints REST

Base URL: http://localhost:3000

### POST /api/alertes
Envoie une alerte a MS1.

```bash
curl -X POST http://localhost:3000/api/alertes \
  -H "Content-Type: application/json" \
  -d '{
    "source_ip": "10.0.0.5",
    "type_attaque": "scan",
    "severite": "haute",
    "description": "Scan de ports detecte"
  }'
```

### GET /api/alertes
Liste les alertes (filtre optionnel: severite).

```bash
curl "http://localhost:3000/api/alertes?severite=haute"
```

### DELETE /api/alertes/:id
Supprime une alerte par id.

```bash
curl -X DELETE http://localhost:3000/api/alertes/ALERTE_ID
```

### GET /api/incidents
Liste les incidents (filtre optionnel: niveau_risque).

```bash
curl "http://localhost:3000/api/incidents?niveau_risque=MOYEN"
```

### GET /api/rapports
Liste les rapports de notification.

```bash
curl http://localhost:3000/api/rapports
```

## GraphQL

Endpoint: http://localhost:4000/graphql

### Schema

```graphql
type Query {
  alertes(severite: String): [Alerte]
  incidents(niveau_risque: String): [Incident]
  rapports: [Rapport]
}

type Mutation {
  envoyerAlerte(
    source_ip: String!
    type_attaque: String!
    severite: String!
    description: String!
  ): AlerteResponse
}

type Alerte { id: String, source_ip: String, type_attaque: String, severite: String, description: String, timestamp: String }
type Incident { id: String, alerte_id: String, niveau_risque: String, recommandation: String, timestamp: String }
type Rapport { id: String, incident_id: String, message: String, timestamp: String }
type AlerteResponse { id: String, succes: Boolean, message: String }
```

### Exemples (PowerShell)

```powershell
$body = '{"query":"{ alertes { id source_ip severite } incidents { id niveau_risque } rapports { id message } }"}'
Invoke-RestMethod -Method Post -Uri http://localhost:4000/graphql -ContentType 'application/json' -Body $body
```

```powershell
$body = '{"query":"mutation { envoyerAlerte(source_ip: \"10.0.0.1\", type_attaque: \"XSS\", severite: \"moyenne\", description: \"Script inject\") { id succes message } }"}'
Invoke-RestMethod -Method Post -Uri http://localhost:4000/graphql -ContentType 'application/json' -Body $body
```

## Kafka

- Topic: nouvelles-alertes
  - Producer: ms1-collecte
  - Consumer: ms2-analyse
- Topic: incidents-detectes
  - Producer: ms2-analyse
  - Consumer: ms3-notification

## Bases de donnees

- ms1-collecte: SQLite3 (ms1.db)
- ms2-analyse: SQLite3 (ms2.db)
- ms3-notification: stockage en memoire (liste en RAM)

## Installation

### Avec Docker (recommande)

```bash
docker-compose up --build
```

## Bonus Docker - preuve de conteneurisation

Redemarrage complet valide avec `docker-compose down -v` puis `docker-compose up --build`, confirmant que tout repart proprement depuis zero.

### Conteneurs en cours d'execution

```
docker-compose ps
NAME                                IMAGE                             COMMAND                  SERVICE            CREATED          STATUS                    PORTS
cyber-platform-api-gateway-1        cyber-platform-api-gateway        "docker-entrypoint.s…"   api-gateway        4 minutes ago    Up 4 minutes              0.0.0.0:3000->3000/tcp, [::]:3000->3000/tcp, 0.0.0.0:4000->4000/tcp, [::]:4000->4000/tcp
cyber-platform-kafka-1              confluentinc/cp-kafka:7.4.0       "/etc/confluent/dock…"   kafka              33 minutes ago   Up 33 minutes (healthy)   0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp
cyber-platform-ms1-collecte-1       cyber-platform-ms1-collecte       "docker-entrypoint.s…"   ms1-collecte       4 minutes ago    Up 4 minutes              0.0.0.0:50051->50051/tcp, [::]:50051->50051/tcp
cyber-platform-ms2-analyse-1        cyber-platform-ms2-analyse        "docker-entrypoint.s…"   ms2-analyse        4 minutes ago    Up 4 minutes              0.0.0.0:50052->50052/tcp, [::]:50052->50052/tcp
cyber-platform-ms3-notification-1   cyber-platform-ms3-notification   "docker-entrypoint.s…"   ms3-notification   4 minutes ago    Up 4 minutes              0.0.0.0:50053->50053/tcp, [::]:50053->50053/tcp
cyber-platform-zookeeper-1          confluentinc/cp-zookeeper:7.4.0   "/etc/confluent/dock…"   zookeeper          33 minutes ago   Up 33 minutes             0.0.0.0:2181->2181/tcp, [::]:2181->2181/tcp
```

### Images builtees

```
docker images | grep cyber
cyber-platform-api-gateway:latest        9e69b3ddab3e        278MB           60MB   U
cyber-platform-ms1-collecte:latest       8333c7ee4183        289MB           69MB   U
cyber-platform-ms2-analyse:latest        d4cbfa49a81b        289MB           69MB   U
cyber-platform-ms3-notification:latest   8a2bf68bfad4        285MB           57.5MB U
```

### Volumes Docker (persistance)

```
docker volume ls
DRIVER    VOLUME NAME
local     6ca56f9bc8f8f7c6f455a784a5d7417b8ecf2266c3bcc82d70144222793add9c
local     13f7279445edafb5997d3929e3e083fd7d92c5151a0082020dd8d411d21826f1
local     27fd4c39e355f5beb85374187546c729fbb32f30667aee8809e36126da496ecc
local     32b57d6072e95667e3b98b8edbb7ca520355ae7184c2f24599bdcdcab9535bb9
local     68b2def5fba73d08b89d57d6089bfbb9da52bbbea2b36b44244f45920e45d684
local     091f6c36c9fb052177be953d5ebec8444c3c5f13e6a87835e483f6191f057b2b
local     429deb8d5c16f69f9125a8a992f69f5355e2a144984f7f5ba911c91e3cf94a40
local     885ceedf73e2153b01f0d095900f2d9ac018372de011747b3a8b9c50410889fb
local     6905bc46f6e8d709d0817e94046dd88787b1dfa159ab4123f8a8b14784098f0a
local     a1d464ef40e33c1724bf7fe26fea92e989423158732f76d015b1f7339101f0bc
local     b97b477234ce8a0f2e1b7f4668f14de7fdfa40b96b9d06051f85151932c06b51
local     bb5cf5745add20a102b01f5f890379fc7a2e9bd872a96407d418040c4d356960
local     bbde70d2453e93f702b442844d1ec04c13fef5f74238ccc0330706494d8a364d
local     e74bba9356fd02de3a5f33badc10468865f76de3660b73072b8fad85a50a698f
local     eeb8bee4b8b472e577c1af9ff0e81033ce54e9edf9c30f32c28a5d350a596da4
local     fc8b6f4ebdad460b5d05b5eeb0f490e3c0fbb8bc86aa2bde4816270cb5efc55f
local     ff4ab3e3daa73a8a4a37606db67b26217e5ba674286844bc41c6961665e26d5a
```

### Sans Docker

Pre-requis: Node.js 18+, Kafka + Zookeeper local sur localhost:9092.

```bash
cd api-gateway && npm install
cd ../ms1-collecte && npm install
cd ../ms2-analyse && npm install
cd ../ms3-notification && npm install
```

```bash
cd ms1-collecte && node index.js
cd ms2-analyse && node index.js
cd ms3-notification && node index.js
cd api-gateway && node index.js
```

## Configuration (env)

- KAFKA_BROKER: broker Kafka (defaut: kafka:9092)
- MS1_HOST: adresse gRPC ms1 (defaut: ms1-collecte:50051)
- MS2_HOST: adresse gRPC ms2 (defaut: ms2-analyse:50052)
- MS3_HOST: adresse gRPC ms3 (defaut: ms3-notification:50053)

