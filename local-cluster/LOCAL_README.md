# Local Kafka + Flink + Backend + Frontend Setup

This setup runs everything in Docker: Kafka, Flink, MinIO, Backend API, and Frontend.

## Architecture

- **Kafka**: Message broker (KRaft mode, no Zookeeper)
- **Flink**: Stream processing (JobManager + TaskManager)
- **MinIO**: S3-compatible storage for Iceberg
- **Backend**: FastAPI service that produces to `themepark_raw` and consumes from `attraction_avg_waittime`
- **Frontend**: React + Vite dashboard

## Quick Start

### 1. Start all services
```bash
cd local-cluster
docker compose up -d
```

### 2. Create Kafka topics
```bash
docker exec -it kafka kafka-topics --create --topic themepark_raw --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic attraction_avg_waittime --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
```

### 3. Submit Flink SQL job
```bash
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/sql/avg_waittime_job.sql
```

Or interactively:
```bash
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh
```

Then paste the SQL from `sql/avg_waittime_job.sql`

### 4. Access services

- **Frontend**: http://localhost:5173
- **Backend API**: http://localhost:8002
- **Flink UI**: http://localhost:8081
- **MinIO Console**: http://localhost:9999 (minioadmin/minioadmin)

## Verify Data Flow

### Check Kafka topics
```bash
# Raw data from producer
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic themepark_raw --from-beginning

# Aggregated data from Flink
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic attraction_avg_waittime --from-beginning
```

### Check backend logs
```bash
docker logs -f themepark-backend
```

### Check Flink logs
```bash
docker logs -f flink-jobmanager
docker logs -f flink-taskmanager
```

## Stop services
```bash
docker compose down
```

## Clean up (remove volumes)
```bash
docker compose down -v
```

## Differences from Confluent Cloud Setup

1. **No Schema Registry**: Using plain JSON instead of Avro/JSON Schema
2. **Local Kafka**: Single broker in KRaft mode
3. **Simplified Config**: No SASL/SSL authentication
4. **All in Docker**: Including frontend with npm

## Troubleshooting

### Backend can't connect to Kafka
- Check if Kafka is running: `docker ps | grep kafka`
- Check Kafka logs: `docker logs kafka`
- Verify network: `docker network inspect local-cluster_themepark-network`

### Flink job not processing
- Check if topics exist: `docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9092`
- Check Flink UI: http://localhost:8081
- Verify job is running in Flink UI

### Frontend not loading
- Check if backend is running: `curl http://localhost:8002/api/health`
- Check frontend logs: `docker logs themepark-frontend`
- Verify npm install completed: `docker exec -it themepark-frontend npm list`
