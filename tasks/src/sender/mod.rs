use std::{time::{Instant, Duration}, net::SocketAddr};

use rdkafka::{producer::{FutureProducer, FutureRecord}, util::Timeout};
use tokio::{sync::{mpsc:: UnboundedSender}};
use utilities::logger::error;

use crate::{statistics::StatisticIncoming, DataPacket};


pub async fn send_to_kafka_exp(packet: DataPacket, partition: Option<i32>, kafka_producer: FutureProducer,
    stats_tx: UnboundedSender<StatisticIncoming>, output_topic: String) -> Result<(),String>{
        let (payload, addr, recv_time) = packet;
        let mut record =  FutureRecord::to(&output_topic).payload(&payload);
        let mut key = addr.to_string() + "|";
        match partition {
            Some(partition) => {
                key += &partition.to_string();
                record = record.partition(partition);
            }
            None => key += "auto",
        }

        record = record.key(&key);

        match kafka_producer.send(record, Timeout::After(Duration::ZERO)).await {
            Ok(_) => {
                send_stat_exp(stats_tx,payload.len(),addr,recv_time).await;
                Ok(())
            }
            Err((e, _)) => {
                error!("Not sended");
                Err(e.to_string())
            }
        }
    }

async fn send_stat_exp(stats_tx: UnboundedSender<StatisticIncoming>,len: usize, addr: SocketAddr, recv_time: Instant) {
    let stat = StatisticIncoming::new(
        addr, 
        recv_time, 
        Instant::now(), 
        len);

        let _ = stats_tx.send(stat);
}