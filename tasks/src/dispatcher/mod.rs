use async_trait::async_trait;
use rdkafka::producer::FutureProducer;
use tokio::{sync::{broadcast, mpsc::{UnboundedSender, Receiver}}, select, spawn};
use utilities::logger::*;

use crate::{Task, DataPacket, ShouldGoOn, PartitionStrategy, statistics::StatisticIncoming, sender::send_to_kafka};

pub struct DispatcherTask {
    stats_tx: UnboundedSender<StatisticIncoming>,
    shutdown_receiver: broadcast::Receiver<()>,
    dispatcher_receiver: Receiver<DataPacket>,
    should_go_on: Box<dyn ShouldGoOn + Send>,
    partition_strategy: Box<dyn PartitionStrategy + Send>,
    kafka_producer: FutureProducer,
    output_topic: String
}

impl DispatcherTask {
    pub fn new(shutdown_receiver: broadcast::Receiver<()>, dispatcher_receiver: Receiver<DataPacket>, stats_tx: UnboundedSender<StatisticIncoming>, should_go_on: Box<dyn ShouldGoOn + Send>, partition_strategy: Box<dyn PartitionStrategy + Send>, kafka_producer: FutureProducer, output_topic: String)-> Self {
        Self { shutdown_receiver, dispatcher_receiver, stats_tx, should_go_on, partition_strategy, output_topic, kafka_producer}
    }

    #[inline(always)]
    async fn dispatch_packet(&mut self, packet: DataPacket, topic: &'static str, producer: &'static FutureProducer) {
        let partition = self.partition_strategy.partition(&packet.1);
        if !self.should_go_on.should_go_on((&packet,&partition.0)) {
            return; 
        }
        let stats_tx = self.stats_tx.clone();
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
                    if let Some(pkt) = data  {
                        DispatcherTask::dispatch_packet(self, pkt,topic,producer).await;
                    }
                }
           }
        }
        Ok(())
    }
}
