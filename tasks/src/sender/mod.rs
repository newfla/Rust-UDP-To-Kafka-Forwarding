use std::{time::Instant, sync::Arc};

use async_trait::async_trait;
use kanal::AsyncSender;
use nohash_hasher::IntMap;
use rdkafka::{producer::{FutureProducer, FutureRecord}, util::Timeout};
use tokio::{spawn, sync::Notify};
use utilities::logger::debug;

use crate::{statistics::{StatisticIncoming::{*, self}, StatisticData}, DataPacket, PartitionDetails};

#[async_trait]
pub trait PacketsOrderStrategy {

    #[inline(always)]
    async fn send_stat(stats_tx: AsyncSender<StatisticIncoming>,len: usize, recv_time: Instant) {
        let stat = StatisticData::new(
            recv_time, 
            Instant::now(), 
            len);

            let _ = stats_tx.send(DataTransmitted(stat)).await;
    }
    
    #[inline(always)]
    async fn send_data_loss(stats_tx: AsyncSender<StatisticIncoming>) {
        let _ = stats_tx.send(DataLoss).await;
    }

    fn send_to_kafka(
        &mut self,
        packet: DataPacket,
        partition_detail: PartitionDetails,
        kafka_producer: &'static FutureProducer,
        stats_tx: AsyncSender<StatisticIncoming>,
        output_topic: &'static str);
}
#[derive(Default)]
pub struct PacketsNotSortedStrategy {

}

impl PacketsOrderStrategy for PacketsNotSortedStrategy {
    #[inline(always)]
    fn send_to_kafka(
        &mut self,
        packet: DataPacket,
        partition_detail: PartitionDetails,
        kafka_producer: &'static FutureProducer,
        stats_tx: AsyncSender<StatisticIncoming>,
        output_topic: &'static str) {
            spawn(async move {
                let (payload, _, recv_time) = packet;
                let (partition, key) = partition_detail;
                let mut record = FutureRecord::to(output_topic).payload(&payload).key(key.as_str());
                record.partition=partition;

                debug!("Send {} bytes with key {}",payload.len(), key);
                match kafka_producer.send(record, Timeout::Never).await {
                    Ok(_) => {
                        Self::send_stat(stats_tx,payload.len(),recv_time).await;
                    }
                    Err(_) => {
                        Self::send_data_loss(stats_tx).await;
                    }
                }
            });
    }
}

#[derive(Default)]
pub struct PacketsSortedByAddressStrategy {
    sender_tasks_map: IntMap<u64,Arc<Notify>>
}

impl PacketsOrderStrategy for PacketsSortedByAddressStrategy {
    #[inline(always)]
    fn send_to_kafka(
        &mut self,
        packet: DataPacket,
        partition_detail: PartitionDetails,
        kafka_producer: &'static FutureProducer,
        stats_tx: AsyncSender<StatisticIncoming>,
        output_topic: &'static str) {
            let (payload, _, recv_time) = packet;
            let (partition, key) = partition_detail;
            let key_hash = key.precomputed_hash();

            if self.sender_tasks_map.get(&key_hash).is_none() {
                //Notify from fake previous task
                let fake_notify = Arc::new(Notify::new());
                let _ = self.sender_tasks_map.insert(key_hash, fake_notify.clone());
                fake_notify.notify_one();
            };

            //Notify for the next task
            let notify_next = Arc::new(Notify::new());
            let notify_prev = self.sender_tasks_map.insert(key_hash, notify_next.clone()).unwrap();

            spawn(async move {
                let mut record = FutureRecord::to(output_topic).payload(&payload).key(key.as_str());
                record.partition=partition;

                debug!("Send {} bytes with key {}",payload.len(), key);
                notify_prev.notified().await;
                notify_next.notify_one();
                match kafka_producer.send(record, Timeout::Never).await {
                    Ok(_) => {
                        Self::send_stat(stats_tx,payload.len(),recv_time).await;
                    }
                    Err(_) => {
                        Self::send_data_loss(stats_tx).await;
                    }
                }
            });
    }
}

pub enum PacketsOrderStrategies {
    NotSorted(PacketsNotSortedStrategy),
    SortedByAddress(PacketsSortedByAddressStrategy)
}

impl PacketsOrderStrategy for PacketsOrderStrategies {
    #[inline(always)]
    fn send_to_kafka(
        &mut self,
        packet: DataPacket,
        partition_detail: PartitionDetails,
        kafka_producer: &'static FutureProducer,
        stats_tx: AsyncSender<StatisticIncoming>,
        output_topic: &'static str) {
            match self {
                PacketsOrderStrategies::NotSorted(strategy) => strategy.send_to_kafka(packet, partition_detail, kafka_producer, stats_tx, output_topic),
                PacketsOrderStrategies::SortedByAddress(strategy) => strategy.send_to_kafka(packet, partition_detail, kafka_producer, stats_tx, output_topic),
            }
    }
}
