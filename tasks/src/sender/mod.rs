use std::{time::{Instant, Duration}, mem, net::SocketAddr};

use async_trait::async_trait;
use rdkafka::{producer::{FutureProducer, FutureRecord}, util::Timeout};
use tokio::{sync::{mpsc:: UnboundedSender}};

use crate::{Task, statistics::StatisticIncoming, DataPacketPlusPartition};

type DataPacketKafka = (String,Vec<u8>,Option<i32>);

pub struct KafkaSenderTask {
    stats_tx: UnboundedSender<StatisticIncoming>,
    payload: Option<Vec<u8>>,
    addr: SocketAddr,
    recv_time: Instant,
    partition: Option<i32>,
    kafka_producer: FutureProducer,
    output_topic: String
}

impl KafkaSenderTask {
    pub fn new(stats_tx: UnboundedSender<StatisticIncoming>, packet: DataPacketPlusPartition, kafka_producer: FutureProducer,
        output_topic: String) -> Self {
            let ((payload, addr, recv_time), partition) = packet;
            let payload = Some(payload);
            Self { stats_tx, payload, addr,recv_time, partition, kafka_producer, output_topic}
    }

    async fn send_stat(&self, len: usize) {
        let stat = StatisticIncoming::new(
            self.addr, 
            self.recv_time, 
            Instant::now(), 
            len);

            let _ = self.stats_tx.send(stat);
    }

    fn build_kafka_packet(&mut self) -> DataPacketKafka {
        let mut addr =self.addr.ip().to_string() + "|" + &self.addr.port().to_string() + "|";
        
        match self.partition {
            Some(part) => addr += &part.to_string(),
            None => addr += "auto",
        }

        (addr,mem::take(&mut self.payload).unwrap(),self.partition)
    }

    async fn send_to_kafka(&self, packet: DataPacketKafka) -> Result<(),String> {
        let (key, payload, partition) = packet;
        
        let mut record =  FutureRecord::to(&self.output_topic)
        .key(&key)
        .payload(&payload);

        if let Some(partition) = partition {
            record = record.partition(partition);
        }
        // self.send_stat(payload.len()).await;
        // Ok(())

        match self.kafka_producer.send(record, Timeout::After(Duration::new(0, 0))).await {
            Ok(_) => {
                self.send_stat(payload.len()).await;
                Ok(())
            }
            Err((e, _)) => Err(e.to_string()),
        }
    }
}

#[async_trait]
impl Task for KafkaSenderTask {
    async fn run(&mut self) -> Result<(),String> {
        let kafka_packet = self.build_kafka_packet();
        self.send_to_kafka(kafka_packet).await
    }
}