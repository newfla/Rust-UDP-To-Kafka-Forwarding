use crate::{PartitionStrategies::{*, self}, CheckpointStrategies, OpenDoorsStrategy, ClosedDoorsStrategy, sender::{PacketsOrderStrategies, PacketsNotSortedStrategy, PacketsSortedByAddressStrategy}, FlipCoinStrategy};

use std::{mem, time::Duration};

use async_trait::async_trait;

use kanal::{unbounded_async, bounded_async};
use rdkafka::{producer::FutureProducer, ClientConfig, error::KafkaError, consumer::{BaseConsumer, Consumer}};
use tokio::{runtime::Builder, sync::broadcast, select, signal, task::JoinSet};
use utilities::{env_var::{EnvVars, self}, logger, logger::info};

use crate::{Task, statistics::StatisticsTask, receiver::{ReceiverTask, build_socket_from_env}, dispatcher::{DispatcherTask}, NonePartitionStrategy, RandomPartitionStrategy, RoundRobinPartitionStrategy, StickyRoundRobinPartitionStrategy};

#[derive(Default)]
pub struct ServerManagerTask {
    vars: Option<EnvVars>,
    producer: Option<FutureProducer>,
    kafka_num_partitions: i32
}

impl ServerManagerTask {

    pub fn init(&mut self) -> Result<(),String> {

        //Init logger
        logger!();

        //Load env variables
        self.vars = utilities::env_var::load_env_var();
        if self.vars.is_none() {
            return Err("error initializatin env vars".to_string());
        }

        //Build rdkafka config
        let kafka_config = self.build_kafka_config();
        let producer = Self::build_kafka_producer(&kafka_config);
        let partitions_count = self.find_partition_number(&kafka_config);

        if let Err(err) = producer {
            return Err(err.to_string());
        }
        
        self.kafka_num_partitions = partitions_count? as i32;
        
        info!("Founded {} partitions for the topic '{}'!",self.kafka_num_partitions,  self.vars.as_ref().unwrap().kafka_topic.as_str());
        self.producer = producer.ok();
        
        Ok(())
    }
    
    pub fn start(&mut self) ->Result<(),String> {
        let worker_threads = self.vars.as_ref().unwrap().worker_threads;
        let mut rt_builder = Builder::new_multi_thread();
        if worker_threads > 0 {
            rt_builder.worker_threads(worker_threads);
        }
        let rt = rt_builder.enable_all().build();

        match rt {
            Ok(rt) =>  rt.block_on(self.run()),
            Err(_) =>  Err("Failed to initialize Tokio runtime".to_string())
        }
    }

    fn build_kafka_config(&self) -> ClientConfig {
        let vars = self.vars.as_ref().unwrap();
        ClientConfig::new()
            .set("bootstrap.servers", vars.kafka_brokers.to_owned())
            .set("batch.num.messages", vars.kafka_batch_num_messages.to_string())
            .set("queue.buffering.max.ms", vars.kafka_queue_buffering_max_ms.to_string())
            .set("queue.buffering.max.messages", vars.kafka_queue_buffering_max_messages.to_string())
            .set("queue.buffering.max.kbytes", vars.kafka_queue_buffering_max_kbytes.to_string())
            .set("compression.codec", vars.kafka_compression_codec.to_string())
            .set("request.required.acks", vars.kafka_request_required_acks.to_string())
            .set("retries", vars.kafka_retries.to_string())
            .to_owned()
    }

    fn build_kafka_producer(config: &ClientConfig) -> Result<FutureProducer,KafkaError> {
        config.create()
    }

    fn find_partition_number(&self, config: &ClientConfig) -> Result<usize,String> {
        let consumer: Result<BaseConsumer,KafkaError> = config.create();
        let topic_name = self.vars.as_ref().unwrap().kafka_topic.as_str();
        let timeout = Duration::from_secs(30);
        match consumer {
            Err(_) => Err("Failed to initialize metadata consumer".to_string()),
            Ok(consumer) => {
                match consumer.fetch_metadata(Some(topic_name), timeout) {
                    Err(_) => Err("Failed to retrieve topic metadata".to_string()),
                    Ok(metadata) => {
                        match metadata.topics().first() {
                            None => Err("Topic".to_string() + topic_name +  "not found"),
                            Some(data) => {
                                if data.partitions().is_empty() {
                                    Err("Topic has 0 partitions".to_string())
                                }else{
                                    Ok(data.partitions().len())}
                                },
                        }
                    },
                }
            },
        }
    }

    fn build_checkpoint_strategy(&self) -> CheckpointStrategies {
        let vars = self.vars.as_ref().unwrap();
        info!("Selected Checkpoint Strategy: {}",vars.checkpoint_strategy());
        
        match vars.checkpoint_strategy() {
            env_var::CheckpointStrategy::OpenDoors =>  CheckpointStrategies::OpenDoors(OpenDoorsStrategy::default()),
            env_var::CheckpointStrategy::ClosedDoors =>  CheckpointStrategies::ClosedDoors(ClosedDoorsStrategy::default()),
            env_var::CheckpointStrategy::FlipCoin =>  CheckpointStrategies::FlipCoin(FlipCoinStrategy::default()),
        }
    }

    fn build_partition_strategy(&self) -> PartitionStrategies {
        let vars = self.vars.as_ref().unwrap();
        info!("Selected Partion Strategy: {}",vars.kafka_partition_strategy());
        
        match vars.kafka_partition_strategy() {
            env_var::PartitionStrategy::None =>  NonePartition(NonePartitionStrategy::default()),
            env_var::PartitionStrategy::Random => RandomPartition(RandomPartitionStrategy::new(self.kafka_num_partitions)),
            env_var::PartitionStrategy::RoundRobin => RoundRobinPartition(RoundRobinPartitionStrategy::new(self.kafka_num_partitions)),
            env_var::PartitionStrategy::StickyRoundRobin => StickyRoundRobinPartition(StickyRoundRobinPartitionStrategy::new(self.kafka_num_partitions)),
        }
    }

    fn build_order_strategy(&self) -> PacketsOrderStrategies {
        let vars = self.vars.as_ref().unwrap();
        info!("Selected Packets Order Strategy: {}",vars.order_strategy());
        
        match vars.order_strategy() {
            env_var::OrderStrategy::NotOrdered =>  PacketsOrderStrategies::NotSorted(PacketsNotSortedStrategy::default()),
            env_var::OrderStrategy::OrderedByAddress =>  PacketsOrderStrategies::SortedByAddress(PacketsSortedByAddressStrategy::default()),
        }
    }

}

#[async_trait]
impl Task for ServerManagerTask {
    async fn run(&mut self) -> Result<(),String> {
        let vars = self.vars.as_ref().unwrap();

        if self.producer.is_none()  {
            return Err("kafka producer is not initializated".to_string());
        }

        let producer = mem::take(&mut self.producer).unwrap();

        //Define shutdown channel
        let (tx_shutdown, mut rx_shutdown) = broadcast::channel::<()>(20);
        
        //Communication channel between receiver and dispatcher tasks
        let (dispatcher_tx,dispatcher_rx) = bounded_async(vars.cache_size);

        //Define auxiliary traits for dispatcher task
        let partition_strategy = self.build_partition_strategy();
        let checkpoint_strategy = self.build_checkpoint_strategy();
        let order_strategy = self.build_order_strategy();
        //Define channel to send statistics update
        let (stats_tx,stats_rx) = unbounded_async();

        //Istantiate closure to build socketaddr for the receiver 
        let func = build_socket_from_env;

        //Istantiate tasks
        let mut stat_task = StatisticsTask::new(vars, tx_shutdown.subscribe(),stats_rx, true);
        let mut receiver_task = ReceiverTask::new(
            func, 
            dispatcher_tx,
            tx_shutdown.subscribe(),
            tx_shutdown.clone(), 
            vars);
            
        let mut dispatcher_task = DispatcherTask::new(
            tx_shutdown.subscribe(),
            dispatcher_rx,
            stats_tx,
            checkpoint_strategy,
            partition_strategy,
            order_strategy,
            producer,
            vars.kafka_topic.to_owned());

        //Schedule tasks
        let mut set = JoinSet::new();
        set.spawn(async move {stat_task.run().await});
        set.spawn(async move {receiver_task.run().await});
        set.spawn(async move {dispatcher_task.run().await});
      
        //Handle CTRL-C signal
        select! {
            _ = signal::ctrl_c() => {
                    info!("Received CTRL-C signal");
                    Self::propagate_shutdown(&tx_shutdown);
            },
            
            _ = rx_shutdown.recv() => {
                info!("Shutting down manager task");
            }
        }
        while (set.join_next().await).is_some() {

        }
        info!("Bye Bye");
        Ok(())
    }
}