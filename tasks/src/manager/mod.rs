use std::{mem, time::Duration};

use async_trait::async_trait;

use rdkafka::{producer::FutureProducer, ClientConfig, error::KafkaError, consumer::{BaseConsumer, Consumer}};
use tokio::{runtime::{Builder,Runtime}, sync::{broadcast, mpsc::{unbounded_channel, channel}}, select, signal, task::JoinSet};
use utilities::{env_var::{EnvVars, self}, logger, logger::info};

use crate::{Task, statistics::StatisticsTask, receiver::{ReceiverTask, build_socket_from_env}, dispatcher::{DispatcherTask}, DataPacket, PartitionStrategy, NonePartitionStrategy, RandomPartitionStrategy, RoundRobinPartitionStrategy, StickyRoundRobinPartitionStrategy, ShouldGoOn};

#[derive(Default)]
pub struct ServerManagerTask {
    vars: Option<EnvVars>,
    producer: Option<FutureProducer>,
    kafka_num_partitions: i32,
    should_go_on_strategy : Option<Box<dyn ShouldGoOn + Send>>
}

impl ServerManagerTask {
    pub fn should_go_on_strategy(mut self, should_go_on_strategy : Box<dyn ShouldGoOn + Send>) -> Self {
        self.should_go_on_strategy = Some(should_go_on_strategy);
        self
    }

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
        let mut rt = Runtime::new();
        if worker_threads > 0 {
            rt = Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .enable_all()
                .build();
        } 
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

    fn build_partition_strategy(&self) -> Box<dyn PartitionStrategy + Send + 'static>  {
        let vars = self.vars.as_ref().unwrap();
        info!("Selected Partion Strategy: {}",vars.kafka_partition_strategy());
        
        match vars.kafka_partition_strategy() {
            env_var::PartitionStrategy::None =>  Box::new(NonePartitionStrategy::default()),
            env_var::PartitionStrategy::Random => Box::new(RandomPartitionStrategy::new(self.kafka_num_partitions)),
            env_var::PartitionStrategy::RoundRobin => Box::new(RoundRobinPartitionStrategy::new(self.kafka_num_partitions)),
            env_var::PartitionStrategy::StickyRoundRobin => Box::new(StickyRoundRobinPartitionStrategy::new(self.kafka_num_partitions)),
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

        if self.should_go_on_strategy.is_none()  {
            return Err("ShouldGoOn Startegy Must be specified".to_string());
        }

        let producer = mem::take(&mut self.producer).unwrap();

        //Define shutdown channel
        let (tx_shutdown, mut rx_shutdown) = broadcast::channel::<()>(20);
        
        //Communication channel between receiver and dispatcher tasks
        let (dispatcher_tx, dispatcher_rx) = channel::<DataPacket>(vars.cache_size);

        //Define auxiliary traits for dispatcher task
        let partition_strategy = self.build_partition_strategy();

        //Define channel to send statistics update
        let (stats_tx,stats_rx) = unbounded_channel();

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
            mem::take(&mut self.should_go_on_strategy).unwrap(),
            partition_strategy,
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
                info!("Shutting manager task");
            }
        }
        while (set.join_next().await).is_some() {

        }
        info!("Bye Bye");
        Ok(())
    }
}