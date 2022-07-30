use async_trait::async_trait;
use rdkafka::producer::FutureProducer;
use tokio::{sync::{broadcast, mpsc::{UnboundedSender, Receiver}}, select, spawn};
use utilities::logger::*;

use crate::{Task, DataPacket, ShouldGoOn, PartitionStrategy, statistics::StatisticIncoming, sender::KafkaSenderTask};

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

    async fn dispatch_packet(&mut self, packet: DataPacket) {
        let partition = self.partition_strategy.partition(&packet.1);
        if !self.should_go_on.should_go_on((&packet,&partition)) {
            return; 
        }
        let packet = (packet,partition);
        let stats_tx = self.stats_tx.clone();
        let producer = self.kafka_producer.clone();
        let topic = self.output_topic.clone();
        spawn(async move {
            if let Err(err) = KafkaSenderTask::new(stats_tx, packet,producer,topic).run().await {
                error!("Send failed {}",err);
            } 
        });
    }
}

#[async_trait]
impl Task for DispatcherTask {
    async fn run(&mut self) -> Result<(),String> {
        loop {
           select! {
                _ = self.shutdown_receiver.recv() => { 
                    info!("Shutting down dispatcher task");
                    break;
                }

                data = self.dispatcher_receiver.recv() => {
                    if let Some(pkt) = data  {
                        DispatcherTask::dispatch_packet(self, pkt).await;
                    }
                }
           }
        }
        Ok(())
    }
}
