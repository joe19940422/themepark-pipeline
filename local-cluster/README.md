docker exec -it kafka /usr/bin/kafka-console-consumer --bootstrap-server kafka:29092 --topic themepark-raw --from-beginning

docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh

docker compose up -d