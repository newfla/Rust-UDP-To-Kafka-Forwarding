use std::{time::Instant, net::SocketAddr};

use rdkafka::{producer::{FutureProducer, FutureRecord}, util::Timeout};
use tokio::{sync::{mpsc:: UnboundedSender}};

use crate::{statistics::{StatisticIncoming::{*, self}, StatisticData}, DataPacket};

#[inline(always)]
pub async fn send_to_kafka(packet: DataPacket, partition: Option<i32>, key: &'static str, kafka_producer: &'static FutureProducer,
    stats_tx: UnboundedSender<StatisticIncoming>, output_topic: &'static str) -> Result<(),String>{
        let (payload, addr, recv_time) = packet;
        let mut record = FutureRecord::to(output_topic).payload(&payload).key(key);
        record.partition=partition;

        match kafka_producer.send(record, Timeout::Never).await {
            Ok(_) => {
                send_stat(stats_tx,payload.len(),addr,recv_time).await;
                Ok(())
            }
            Err((e, _)) => {
                let _ = stats_tx.send(DataLoss);
                Err(e.to_string())
            }
        }
    }

#[inline(always)]
async fn send_stat(stats_tx: UnboundedSender<StatisticIncoming>,len: usize, addr: SocketAddr, recv_time: Instant) {
    let stat = StatisticData::new(
        addr, 
        recv_time, 
        Instant::now(), 
        len);

        let _ = stats_tx.send(DataTransmitted(stat));
}