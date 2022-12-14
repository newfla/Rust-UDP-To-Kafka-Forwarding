use std::{time::Instant, net::SocketAddr};

use kanal::AsyncSender;
use nohash_hasher::IntMap;
use once_cell::sync::OnceCell;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::spawn;
use ustr::Ustr;
use utilities::logger::debug;
use prost::Message;

use crate::{DataTransmitted, DataPacket, PartitionDetails,sender::proto::KafkaMessage, Ticket, statistics::StatisticData};

static ONCE_PRODUCER: OnceCell<FutureProducer> = OnceCell::new();

// Include the `items` module, which is generated from items.proto.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/sender.proto.rs"));
}

pub struct KafkaPacketSender {
    producer: &'static FutureProducer,
    output_topic: &'static str,
    use_proto: bool,
    sender_tasks_map: IntMap<u64,Ticket>,
    stats_tx: AsyncSender<DataTransmitted>
}

impl KafkaPacketSender{

    pub fn new (kafka_producer: FutureProducer, output_topic: Ustr,use_proto: bool, stats_tx: AsyncSender<DataTransmitted>) -> Self{
        let producer = ONCE_PRODUCER.get_or_init(|| {kafka_producer});
        let output_topic = output_topic.as_str();
        Self {
            producer,
            output_topic,
            use_proto,
            sender_tasks_map: IntMap::default(),
            stats_tx
        }
    }

    #[inline(always)]
    async fn send_stat(stats_tx: AsyncSender<DataTransmitted>,len: usize, recv_time: Instant, key: u64) {
        let stat = StatisticData::new(
            recv_time, 
            Instant::now(), 
            len,
        key);

            let _ = stats_tx.send(Some(stat)).await;
    }
    
    #[inline(always)]
    async fn send_data_loss(stats_tx: AsyncSender<DataTransmitted>) {
        let _ = stats_tx.send(None).await;
    }

    #[inline(always)]
    fn build_message(addr: &SocketAddr, payload: Vec<u8>, partition: &Option<i32>) -> KafkaMessage {
        KafkaMessage { data: payload, address: addr.to_string(), partition: partition.unwrap_or(-1) }
    }
    
    #[inline(always)]
    pub fn send_to_kafka(
        &mut self,
        packet: DataPacket,
        partition_detail: PartitionDetails) {
            let (mut payload, addr, recv_time) = packet;
            let (partition, key, key_hash) = partition_detail;
            let key_hash = key_hash.precomputed_hash();

            if self.sender_tasks_map.get(&key_hash).is_none() {
                //Notify from fake previous task
                let fake_notify = Ticket::default();
                let _ = self.sender_tasks_map.insert(key_hash, fake_notify.clone());
                fake_notify.notify_one();
            };

            //Notify for the next task
            let notify_next = Ticket::default();
            let notify_prev = self.sender_tasks_map.insert(key_hash, notify_next.clone()).unwrap();

            let producer = self.producer;
            let output_topic = self.output_topic;
            let use_proto = self.use_proto;
            let stats_tx = self.stats_tx.clone();

            spawn(async move {
                if use_proto {
                    payload = Self::build_message(&addr,payload, &partition_detail.0).encode_to_vec();
                }
                let mut record = FutureRecord::to(output_topic).payload(&payload).key(key.as_str());
                record.partition=partition;

                debug!("{} bytes with key {} ready to be sent",payload.len(), key);
                notify_prev.notified().await;

                'send_loop: loop {
                    match producer.send_result(record) {
                        Ok(enqueuing_ok) => {
                            notify_next.notify_one();
                            match enqueuing_ok.await {
                                Ok(_) => {
                                    Self::send_stat(stats_tx,payload.len(),recv_time, key_hash).await;
                                }
                                Err(_) => {
                                    Self::send_data_loss(stats_tx).await;
                                }
                            }
                            break 'send_loop;
                        }
                        Err((_,rec)) => {
                            record = rec;
                        }
                    }
                }
            });
    }
}