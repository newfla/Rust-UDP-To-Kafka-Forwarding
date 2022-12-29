use async_trait::async_trait;
use derive_new::new;
use kanal::{AsyncReceiver, AsyncSender};
use rdkafka::producer::FutureProducer;
use tokio::select;
use tokio_util::sync::CancellationToken;
use utilities::logger::*;

use crate::{Task, DataPacket, CheckpointStrategy, PartitionStrategy, statistics::StatisticIncoming, PartitionStrategies, CheckpointStrategies, sender::{PacketsOrderStrategies, PacketsOrderStrategy}};

#[derive(new)]
pub struct DispatcherTask {
    shutdown_token: CancellationToken,
    dispatcher_receiver: AsyncReceiver<DataPacket>,
    stats_tx: AsyncSender<StatisticIncoming>, 
    checkpoint_strategy: CheckpointStrategies,
    partition_strategy: PartitionStrategies,
    order_strategy: PacketsOrderStrategies,
    kafka_producer: FutureProducer,
    output_topic: String
}

impl DispatcherTask {

    #[inline(always)]
    async fn dispatch_packet(&mut self, packet: DataPacket, topic: &'static str, producer: &'static FutureProducer) {
        let partition = self.partition_strategy.partition(&packet.1);
        if !self.checkpoint_strategy.check((&packet,&partition.0)) {
            return; 
        }
        self.order_strategy.send_to_kafka(packet, partition, producer, self.stats_tx.clone(), topic);
    }
}

#[async_trait]
impl Task for DispatcherTask {
    async fn run(&mut self) {
        let topic = Box::leak(self.output_topic.clone().into_boxed_str());
        let producer = Box::leak(Box::new(self.kafka_producer.clone()));
        loop {
           select! {
                _ = self.shutdown_token.cancelled() => { 
                    info!("Shutting down dispatcher task");
                    break;
                }

                data = self.dispatcher_receiver.recv() => {
                    if let Ok(pkt) = data  {
                        DispatcherTask::dispatch_packet(self, pkt, topic, producer).await;
                    }
                }
           }
        }
    }
}
