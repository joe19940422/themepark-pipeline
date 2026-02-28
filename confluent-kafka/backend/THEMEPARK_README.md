# 🎢 Theme Park Wait Time Analytics

Real-time theme park attraction wait time monitoring using Apache Kafka, Confluent Cloud, Apache Flink SQL, and React.

## 📊 What You'll Build

- **Real-time data ingestion** from Theme Parks API every 5 minutes
- **Stream processing** with Apache Flink SQL to calculate average wait times
- **Live dashboard** displaying attraction analytics
- **Server-Sent Events** for real-time updates

## 🏗️ Architecture

```
Theme Parks API → Python Producer → Kafka (themepark_raw) 
                                         ↓
                                    Flink SQL (AVG)
                                         ↓
                                Kafka (attraction_avg_waittime)
                                         ↓
                                  Python Consumer
                                         ↓
                                   FastAPI + SSE
                                         ↓
                                   React Dashboard
```

## 📋 Prerequisites

- Python 3.11+
- Node.js 18+
- Confluent Cloud account (free tier)

## 🚀 Quick Start

### 1. Confluent Cloud Setup

#### Create Environment & Cluster
1. Go to [Confluent Cloud](https://confluent.cloud)
2. Create environment: `themepark-analytics-env`
3. Create cluster: Basic, AWS us-east-2

#### Create API Keys
1. **Kafka API Key**: Cluster → API Keys → Create Key
2. **Schema Registry API Key**: Environment → Schema Registry → API Keys

#### Create Topics
1. Navigate to Topics
2. Create: `themepark_raw` (raw attraction data)
3. Create: `attraction_avg_waittime` (Flink output)

### 2. Configure Application

Update `themepark_config.yaml`:
```yaml
kafka:
  bootstrap.servers: 'pkc-xxxxx.us-east-1.aws.confluent.cloud:9092'
  security.protocol: "SASL_SSL"
  sasl.mechanism: "PLAIN"
  sasl.username: 'YOUR_API_KEY'
  sasl.password: 'YOUR_API_SECRET'
  schema_registry_url: 'https://psrc-xxxxx.us-east-2.aws.confluent.cloud'
  schema_registry_api_key: 'YOUR_SR_KEY'
  schema_registry_secret: 'YOUR_SR_SECRET'
  topics:
    raw_data: "themepark_raw"
    avg_waittime: "attraction_avg_waittime"
  consumer_group: "themepark-consumer"
```

### 3. Install Dependencies

```bash
pip install confluent-kafka requests pyyaml fastapi uvicorn
```

### 4. Create Flink SQL Analytics

#### Open Flink Workspace
1. Navigate to Flink in Confluent Cloud
2. Create Compute Pool (AWS us-east-2)
3. Open SQL Workspace

#### Create Output Table
```sql
CREATE TABLE attraction_avg_waittime (
  entityId STRING,
  name STRING,
  avg_waittime DOUBLE,
  PRIMARY KEY (entityId) NOT ENFORCED
) WITH (
  'changelog.mode' = 'upsert',
  'value.format' = 'avro-registry'
);
```

#### Calculate Real-Time Average Wait Times
```sql
INSERT INTO attraction_avg_waittime
SELECT
  entityId,
  REGEXP_REPLACE(name, '“|”|’', '') AS name,
  AVG(CAST(waitTime AS DOUBLE)) AS avg_waittime
FROM themepark_raw
WHERE status = 'OPERATING'
GROUP BY entityId, REGEXP_REPLACE(name, '“|”|’', '');

```

This query:
- Filters only OPERATING attractions and remove “ ” ’
- Groups by attraction (entityId + name)
- Calculates rolling average wait time
- Updates continuously as new data arrives

### 5. Run Application

```bash
python themepark_main.py
```

The application will:
- ✅ Start FastAPI server on port 8002
- ✅ Begin fetching theme park data every 5 minutes
- ✅ Produce data to Kafka
- ✅ Consume Flink analytics
- ✅ Serve real-time updates via SSE

### 6. Test Endpoints

```bash
# Health check
curl http://localhost:8002/api/health

# Get current attractions
curl http://localhost:8002/api/attractions

# Stream real-time analytics (SSE)
curl http://localhost:8002/api/analytics/stream
```

## 📊 Data Flow

### Input Data (themepark_raw)
```json
{
  "entityId": "attraction-123",
  "timestamp_ms": 1704067200000,
  "status": "OPERATING",
  "name": "Space Mountain",
  "waitTime": 45,
  "entityType": "ATTRACTION"
}
```

### Output Data (attraction_avg_waittime)
```json
{
  "entityId": "attraction-123",
  "name": "Space Mountain",
  "avg_waittime": 42.5
}
```

## 🔍 Flink SQL Queries

### View Raw Data
```sql
SELECT * FROM themepark_raw LIMIT 10;
```

### Top 5 Longest Waits
```sql
SELECT name, waitTime
FROM themepark_raw
WHERE status = 'OPERATING'
ORDER BY waitTime DESC
LIMIT 5;
```

### Attractions by Status
```sql
SELECT status, COUNT(*) as count
FROM themepark_raw
GROUP BY status;
```

### Average Wait Time Over Time Window
```sql
SELECT
  name,
  TUMBLE_START(timestamp_ms, INTERVAL '10' MINUTES) as window_start,
  AVG(waitTime) as avg_wait
FROM themepark_raw
WHERE status = 'OPERATING'
GROUP BY name, TUMBLE(timestamp_ms, INTERVAL '10' MINUTES);
```

## 📁 Project Files

```
themepark_producer.py      # Fetches API data, produces to Kafka
themepark_consumer.py      # Consumes Flink analytics
themepark_config.py        # Configuration management
themepark_config.yaml      # Confluent Cloud credentials
themepark_main.py          # FastAPI server with SSE
```

## 🎯 Key Features

### Producer
- Polls Theme Parks API every 5 minutes
- Filters ATTRACTION entities
- Uses JSON Schema Registry
- Handles API errors gracefully

### Flink SQL
- Real-time aggregation
- Upsert mode for latest values
- Filters by status
- Groups by attraction

### Consumer
- Subscribes to analytics topic
- Deserializes JSON data
- Maintains in-memory state
- Triggers SSE callbacks

### API
- FastAPI with async support
- Server-Sent Events for real-time updates
- CORS enabled for frontend
- Health check endpoint

## 🧹 Cleanup

```bash
# Stop application: Ctrl+C

# Delete Confluent Cloud resources:
# - Delete topics
# - Delete cluster
# - Delete environment
```

## 🔗 API Reference

### Theme Parks API
- **Endpoint**: `https://api.themeparks.wiki/v1/entity/{parkId}/live`
- **Documentation**: https://api.themeparks.wiki/docs/
- **Park ID**: `6e1464ca-1e9b-49c3-8937-c5c6f6675057` (Magic Kingdom)

### Response Structure
```json
{
  "liveData": [
    {
      "id": "attraction-id",
      "name": "Attraction Name",
      "entityType": "ATTRACTION",
      "status": "OPERATING",
      "queue": {
        "STANDBY": {
          "waitTime": 30
        }
      }
    }
  ]
}
```

## 📚 Resources

- [Confluent Cloud Docs](https://docs.confluent.io/cloud/current/)
- [Apache Flink SQL](https://docs.confluent.io/cloud/current/flink/)
- [Theme Parks API](https://api.themeparks.wiki/docs/)
- [FastAPI Docs](https://fastapi.tiangolo.com/)

## 🎉 Next Steps

1. **Add Frontend**: Build React dashboard to visualize data
2. **Add Alerts**: Notify when wait times exceed threshold
3. **Historical Analysis**: Store data in database for trends
4. **Multiple Parks**: Extend to monitor multiple theme parks
5. **Predictions**: Use ML to predict wait times

---

**Built with ❤️ using Confluent Cloud, Apache Flink, and FastAPI**
