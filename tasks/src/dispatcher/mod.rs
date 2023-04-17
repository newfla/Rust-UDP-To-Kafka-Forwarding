use std::{future::{IntoFuture, Future}, pin::Pin};

use kanal::AsyncReceiver;
use tokio::select;
use tokio_util::sync::CancellationToken;
use utilities::logger::*;

use crate::{DataPacket, CheckpointStrategy, PartitionStrategy, PartitionStrategies, CheckpointStrategies, Strategies, sender::KafkaPacketSender};

pub struct DispatcherTask {
    shutdown_token: CancellationToken,
    dispatcher_receiver: AsyncReceiver<DataPacket>,
    checkpoint_strategy: CheckpointStrategies,
    partition_strategy: PartitionStrategies,
    kafka_sender: KafkaPacketSender
}


impl DispatcherTask {

    pub fn new (
        shutdown_token: CancellationToken,
        dispatcher_receiver: AsyncReceiver<DataPacket>,
        strategies: Strategies,
        kafka_sender: KafkaPacketSender) -> Self {

            let (checkpoint_strategy, partition_strategy) = strategies;

            Self { shutdown_token, dispatcher_receiver, checkpoint_strategy, partition_strategy, kafka_sender}
    }

    #[inline(always)]
    async fn dispatch_packet(&mut self, packet: DataPacket) {
        let partition = self.partition_strategy.partition(&packet.1.1);
        if !self.checkpoint_strategy.check((&packet,&partition.0)) {
            return; 
        }
        self.kafka_sender.send_to_kafka(packet, partition);
    }

    async fn run(mut self) {
        loop {
           select! {
                _ = self.shutdown_token.cancelled() => { 
                    info!("Shutting down dispatcher task");
                    break;
                }

                data = self.dispatcher_receiver.recv() => {
                    if let Ok(pkt) = data  {
                        DispatcherTask::dispatch_packet(&mut self, pkt).await;
                    }
                }
           }
        }
    }
}

impl IntoFuture for DispatcherTask {
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.run())
    }
}
