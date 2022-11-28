use std::{time::Instant, net::SocketAddr};

use kanal::AsyncSender;
use rdkafka::{producer::{FutureProducer, FutureRecord}, util::Timeout};
use utilities::logger::debug;

use crate::{statistics::{StatisticIncoming::{*, self}, StatisticData}, DataPacket};

#[inline(always)]
pub async fn send_to_kafka(packet: DataPacket, partition: Option<i32>, key: &'static str, kafka_producer: &'static FutureProducer,
    stats_tx: AsyncSender<StatisticIncoming>, output_topic: &'static str) -> Result<(),String>{
        let (payload, addr, recv_time) = packet;
        let mut record = FutureRecord::to(output_topic).payload(&payload).key(key);
        record.partition=partition;

        debug!("Send {} bytes with key {}",payload.len(), key);
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
async fn send_stat(stats_tx: AsyncSender<StatisticIncoming>,len: usize, addr: SocketAddr, recv_time: Instant) {
    let stat = StatisticData::new(
        addr, 
        recv_time, 
        Instant::now(), 
        len);

        let _ = stats_tx.send(DataTransmitted(stat)).await;
}