use async_trait::async_trait;
use kanal::{AsyncReceiver, AsyncSender};
use rdkafka::producer::FutureProducer;
use tokio::{sync::broadcast, select, spawn};
use utilities::logger::*;

use crate::{Task, DataPacket, CheckpointStrategy, PartitionStrategy, statistics::StatisticIncoming, sender::send_to_kafka, PartitionStrategies, CheckpointStrategies};

pub struct DispatcherTask {
    stats_tx: AsyncSender<StatisticIncoming>,
    shutdown_receiver: broadcast::Receiver<()>,
    dispatcher_receiver: AsyncReceiver<DataPacket>,
    checkpoint_strategy: CheckpointStrategies,
    partition_strategy: PartitionStrategies,
    kafka_producer: FutureProducer,
    output_topic: String
}

impl DispatcherTask {
    pub fn new(shutdown_receiver: broadcast::Receiver<()>, dispatcher_receiver: AsyncReceiver<DataPacket>, stats_tx: AsyncSender<StatisticIncoming>, checkpoint_strategy: CheckpointStrategies, partition_strategy: PartitionStrategies, kafka_producer: FutureProducer, output_topic: String)-> Self {
        Self { shutdown_receiver, dispatcher_receiver, stats_tx, checkpoint_strategy, partition_strategy, output_topic, kafka_producer}
    }

    #[inline(always)]
    async fn dispatch_packet(&mut self, packet: DataPacket, topic: &'static str, producer: &'static FutureProducer) {
        let partition = self.partition_strategy.partition(&packet.1);
        if !self.checkpoint_strategy.check((&packet,&partition.0)) {
            return; 
        }
        let stats_tx = self.stats_tx.clone();
        debug!("Dispatching packet {}",partition.1);
        spawn(async move {
            send_to_kafka(packet, partition.0, partition.1,producer, stats_tx, topic).await
        });
    }
}

#[async_trait]
impl Task for DispatcherTask {
    async fn run(&mut self) -> Result<(),String> {
        let topic = Box::leak(self.output_topic.clone().into_boxed_str());
        let producer = Box::leak(Box::new(self.kafka_producer.clone()));
        loop {
           select! {
                _ = self.shutdown_receiver.recv() => { 
                    info!("Shutting down dispatcher task");
                    break;
                }

                data = self.dispatcher_receiver.recv() => {
                    if let Ok(pkt) = data  {
                        DispatcherTask::dispatch_packet(self, pkt,topic,producer).await;
                    }
                }
           }
        }
        Ok(())
    }
}
