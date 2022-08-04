use std::time::Instant;

use async_trait::async_trait;
use rdkafka::{producer::{FutureProducer, FutureRecord}, util::Timeout};
use tokio::{sync::{mpsc:: UnboundedSender}};

use crate::{Task, statistics::StatisticIncoming, DataPacketPlusPartition};

pub struct KafkaSenderTask {
    stats_tx: UnboundedSender<StatisticIncoming>,
    kafka_producer: FutureProducer,
    output_topic: String,
    packet: DataPacketPlusPartition,
    
}

impl KafkaSenderTask {
    pub fn new(stats_tx: UnboundedSender<StatisticIncoming>, packet: DataPacketPlusPartition, kafka_producer: FutureProducer,
        output_topic: String) -> Self {
          
            Self { stats_tx, packet, kafka_producer, output_topic}
    }

    async fn send_stat(&self) {
        let ((payload, addr, recv_time), _) = &self.packet;
        let stat = StatisticIncoming::new(
            *addr, 
            *recv_time, 
            Instant::now(), 
            payload.len());

            let _ = self.stats_tx.send(stat);
    }


    async fn send_to_kafka(&self) -> Result<(),String> {
        let ((payload, addr, _), partition) = &self.packet;
        let mut record =  FutureRecord::to(&self.output_topic).payload(payload);
        let mut key = addr.to_string() + "|";
        match partition {
            Some(partition) => {
                key += &partition.to_string();
                record = record.partition(*partition);
            }
            None => key += "auto",
        }

        record = record.key(&key);
        
        match self.kafka_producer.send(record, Timeout::Never).await {
            Ok(_) => {
                self.send_stat().await;
                Ok(())
            }
            Err((e, _)) => Err(e.to_string()),
        }
    }
}

#[async_trait]
impl Task for KafkaSenderTask {
    async fn run(&mut self) -> Result<(),String> {
        self.send_to_kafka().await
    }
}