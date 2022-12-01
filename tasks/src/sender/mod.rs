use std::{time::Instant, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use kanal::AsyncSender;
use rdkafka::{producer::{FutureProducer, FutureRecord}, util::Timeout};
use rustc_hash::FxHashMap;
use tokio::{spawn, sync::Notify};
use utilities::logger::debug;

use crate::{statistics::{StatisticIncoming::{*, self}, StatisticData}, DataPacket};

#[async_trait]
pub trait PacketsOrderStrategy {

    #[inline(always)]
    async fn send_stat(stats_tx: AsyncSender<StatisticIncoming>,len: usize, addr: SocketAddr, recv_time: Instant) {
        let stat = StatisticData::new(
            addr, 
            recv_time, 
            Instant::now(), 
            len);

            let _ = stats_tx.send(DataTransmitted(stat)).await;
    }

    async fn send_to_kafka(
        &mut self,
        packet: DataPacket, 
        partition: Option<i32>,
        key: &'static str,
        kafka_producer:
        &'static FutureProducer,
        stats_tx: AsyncSender<StatisticIncoming>,
        output_topic: &'static str);
}
#[derive(Default)]
pub struct PacketsNotSortedStrategy {

}

#[async_trait]
impl PacketsOrderStrategy for PacketsNotSortedStrategy {
    #[inline(always)]
    async fn send_to_kafka(
        &mut self,
        packet: DataPacket, 
        partition: Option<i32>,
        key: &'static str,
        kafka_producer:
        &'static FutureProducer,
        stats_tx: AsyncSender<StatisticIncoming>,
        output_topic: &'static str) {
            spawn(async move {
                let (payload, addr, recv_time) = packet;
                let mut record = FutureRecord::to(output_topic).payload(&payload).key(key);
                record.partition=partition;

                debug!("Send {} bytes with key {}",payload.len(), key);
                match kafka_producer.send(record, Timeout::Never).await {
                    Ok(_) => {
                        Self::send_stat(stats_tx,payload.len(),addr,recv_time).await;
                    }
                    Err((_, _)) => {
                        let _ = stats_tx.send(DataLoss);
                    }
                }
            });
    }
}

#[derive(Default)]
pub struct PacketsSortedByAddressStrategy {
    sender_tasks_map: FxHashMap<SocketAddr,Arc<Notify>>
}

#[async_trait]
impl PacketsOrderStrategy for PacketsSortedByAddressStrategy {
    #[inline(always)]
    async fn send_to_kafka(
        &mut self,
        packet: DataPacket, 
        partition: Option<i32>,
        key: &'static str,
        kafka_producer:
        &'static FutureProducer,
        stats_tx: AsyncSender<StatisticIncoming>,
        output_topic: &'static str) {
            if self.sender_tasks_map.get(&packet.1).is_none() {
                //Notify from fake previous task
                let fake_notify = Arc::new(Notify::new());
                let _ = self.sender_tasks_map.insert(packet.1, fake_notify.clone());
                fake_notify.notify_one();
            };

            //Notify for the next task
            let notify_next = Arc::new(Notify::new());
            let notify_prev = self.sender_tasks_map.insert(packet.1, notify_next.clone()).unwrap();

            spawn(async move {
                let (payload, addr, recv_time) = packet;
                let mut record = FutureRecord::to(output_topic).payload(&payload).key(key);
                record.partition=partition;

                debug!("Send {} bytes with key {}",payload.len(), key);
                notify_prev.notified().await;
                notify_next.notify_one();
                match kafka_producer.send(record, Timeout::Never).await {
                    Ok(_) => {
                        Self::send_stat(stats_tx,payload.len(),addr,recv_time).await;
                    }
                    Err((_, _)) => {
                        let _ = stats_tx.send(DataLoss);
                    }
                }
            });

    }
}

pub enum PacketsOrderStrategies {
    NotSorted(PacketsNotSortedStrategy),
    SortedByAddress(PacketsSortedByAddressStrategy)
}

#[async_trait]
impl PacketsOrderStrategy for PacketsOrderStrategies {
    #[inline(always)]
    async fn send_to_kafka(
        &mut self,
        packet: DataPacket, 
        partition: Option<i32>,
        key: &'static str,
        kafka_producer:
        &'static FutureProducer,
        stats_tx: AsyncSender<StatisticIncoming>,
        output_topic: &'static str) {
            match self {
                PacketsOrderStrategies::NotSorted(strategy) => strategy.send_to_kafka(packet, partition, key, kafka_producer, stats_tx, output_topic).await,
                PacketsOrderStrategies::SortedByAddress(strategy) => strategy.send_to_kafka(packet, partition, key, kafka_producer, stats_tx, output_topic).await,
            }
    }
}
