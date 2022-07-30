# Tokio-To-kafka

A simple UDP async server based on [Tokio framework](https://https://tokio.rs) that forwards packets to [Apache Kafka](https://kafka.apache.org/).

Each UDP datagram is forwarded to kafka with an header containing :
- Source IP and PORT of udp packet
- Optionally, the KAFKA PARTITION where the packet will be stored  __'\*'__

__'\*'__: only available when _KAFKA_PARTITION_STRATEGY_ is set to _RANDOM, ROUND_ROBIN or STICKY_ROUND_ROBIN_ 

## Partition strategy
- **NONE** (default): let the broker to decide
- **RANDOM**: assign a random partition for each packet
- **ROUND_ROBIN**: packets are distributed over partitions using a round robin schema
- **STICKY_ROUND_ROBIN**: packets coming from the same peer are guaranted to be sent on the same partition. Partitions are assigned to peers using a round robin schema

## Environment Variables Required
### Server Behaviour:
  - **SERVER_IP** [default = 127.0.0.1]
  - **SERVER_PORT**  [default = 8888]
  - **BUFFER_SIZE**: buffer size for the socket recv output [default = 1024]
  - **STATS_INTERVAL**: Refresh interval for the statistics [default = 10]
  - **WORKER_THREADS** Number of worker threads for the Tokio runtime [default: #CPU core]
  - **KAFKA_BROKERS**: brokers list (comma separated)

### Kafka side (see [Rdkafka configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md))
  - **KAFKA_TOPIC**: output topic for the packets
  - **KAFKA_PARTITION_STRATEGY** [default = NONE]
  - **KAFKA_BATCH_NUM_MESSAGES** [default = 10000]
  - **KAFKA_QUEUE_BUFFERING_MAX_MS** [default = 5]
  - **KAFKA_COMPRESSION_CODEC** [default = lz4]
  - **KAFKA_REQUEST_REQUIRED_ACKS** [default = 0]


## Example
Check the complete example involving a client, the broker and a kafka consumer [here]()