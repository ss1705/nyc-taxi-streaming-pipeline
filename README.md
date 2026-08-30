Kafka: 
- message broker
- holds messages in a queue -> topic until PySpark is ready to consume them
- called decoupling: producer + consumer don't need to know about each other or run at same speed

Pipeline: 
NYC Taxi API → Kafka → PySpark → Redshift

Zookeeper:
- Kafka's coordinator
- handles: which Kafka broker is the leader, tracks which consumers have read which messages, cluster membership
- needs to be running for Kafka to work (behind-the-scenes manager)

KRaft mode:
- Kafka runs without Zookeeper entirely
- one less container, simpler setup
- Kafka 3.x removed the Zookeeper dependency by handling cluster coordination internally

Docker commands:
docker compose up -d: 
- starts all containers defined in docker-compose.yml
- "-d" means detached - runs them in background so terminal stays free, otherwise logs would stream in terminal

docker compose down:
- stops + removes all containers
- run when done working / before changing config

docker compose ps:
- lists status fo containers - running/stopped/errored
- same idea as ps in Unix

up = turning on your servers
down = turning them off
ps = checking which servers are on right now

Other:
docker compose logs kafka — see what Kafka is printing internally, useful for debugging
docker compose restart kafka — restart just one container without touching the others

producer.py
Kafka setup
- Kafka producer defined
- serializing values: convert every Python dict into JSON bytes (Kafka only speaks bytes)

Fetching data
- fetch NYC TLC open dataset via Socrata API (pulling 500 most recent taxi trips, ordered by pickup time)
- remember: always check field names from raw API response returns before writing the script -- we found a mismatch in the pickup_datetime param

Streaming
- set time.sleep(0.1) to simulate real-time conditions: one trip every 100ms, i.e., 10 trips per second (behaves likes a live stream)

Flush:
- force to send anything sitting in kafka's buffer before the script exits
- without this we could lose last few messages

Creating Kafka topic:
docker exec -it data-stack-project1-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic taxi-trips \
  --partitions 1 \
  --replication-factor 1

Spark:
- distributed data processing engine
- splits work across many machines in a cluster and processes them in parallel
- horizontally scalable (add more machines to the cluster > buying single bigger machine)
- PySpark: Python API for Spark
- two modes: batch mode, structured streaming
- Batch mode: processes everything, then stops
- Structured streaming: treats incoming data stream as unbounded table that keeps growing, looks at new messages - processes them - appends results to output, keeps running
