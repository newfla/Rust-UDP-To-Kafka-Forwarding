use std::str::FromStr;

use envconfig::Envconfig;
use strum::{EnumString,Display};
use crate::{logger::{debug,info}, logger};

#[derive(EnumString,Display)]
pub enum PartitionStrategy {
    #[strum(serialize="NONE")]
    None,
    #[strum(serialize="RANDOM")]
    Random,
    #[strum(serialize="ROUND_ROBIN")]
    RoundRobin,
    #[strum(serialize="STICKY_ROUND_ROBIN")]
    StickyRoundRobin
}

#[derive(EnumString,Display)]
pub enum CheckpointStrategy {
    #[strum(serialize="OPEN_DOORS")]
    OpenDoors,
    #[strum(serialize="CLOSED_DOORS")]
    ClosedDoors,
    #[strum(serialize="FLIP_COIN")]
    FlipCoin,
}

#[derive(EnumString,Display)]
pub enum OrderStrategy {
    #[strum(serialize="NOT_ORDERED")]
    NotOrdered,
    #[strum(serialize="ORDERED_BY_ADDRESS")]
    OrderedByAddress,
}

#[derive(Envconfig, Debug, Clone)]
pub struct EnvVars {
    #[envconfig(from = "LISTEN_IP", default="127.0.0.1")]
    pub listen_ip: String,
    
    #[envconfig(from = "LISTEN_PORT", default="8888")]
    pub listen_port: u16,

    #[envconfig(from = "USE_DTLS", default="false")]
    pub use_dtls: bool,

    #[envconfig(from = "SERVER_KEY")]
    pub server_key: Option<String>,

    #[envconfig(from = "SERVER_CERT")]
    pub server_cert: Option<String>,

    #[envconfig(from = "BUFFER_SIZE", default="1024")]
    pub buffer_size: usize,

    #[envconfig(from = "STATS_INTERVAL", default="10")]
    pub stats_interval: u64,

    #[envconfig(from = "WORKER_THREADS", default="0")]
    pub worker_threads: usize,

    #[envconfig(from = "CACHE_SIZE", default="50000")]
    pub cache_size: usize,

    #[envconfig(from = "KAFKA_BROKERS")]
    pub kafka_brokers: String,

    #[envconfig(from = "KAFKA_TOPIC")]
    pub kafka_topic: String,

    #[envconfig(from = "KAFKA_PARTITION_STRATEGY", default="NONE")]
    kafka_partition_strategy: String,

    #[envconfig(from = "CHECKPOINT_STRATEGY", default="OPEN_DOORS")]
    checkpoint_strategy: String,

    #[envconfig(from = "KAFKA_BATCH_NUM_MESSAGES", default="10000")]
    pub kafka_batch_num_messages: u32,

    #[envconfig(from = "KAFKA_QUEUE_BUFFERING_MAX_MS", default="5")]
    pub kafka_queue_buffering_max_ms: u32,

    #[envconfig(from = "KAFKA_QUEUE_BUFFERING_MAX_MESSAGES", default="100000")]
    pub kafka_queue_buffering_max_messages: u32,

    #[envconfig(from = "KAFKA_QUEUE_BUFFERING_MAX_KBYTES", default="1048576")]
    pub kafka_queue_buffering_max_kbytes: u32,

    #[envconfig(from = "KAFKA_COMPRESSION_CODEC", default="lz4")]
    pub kafka_compression_codec: String,

    #[envconfig(from = "KAFKA_REQUEST_REQUIRED_ACKS", default="1")]
    pub kafka_request_required_acks: u8,

    #[envconfig(from = "KAFKA_RETRIES", default="1")]
    pub kafka_retries: u32,

    #[envconfig(from = "USE_PROTO", default="false")]
    pub use_proto: bool,
}

impl EnvVars {
    pub fn kafka_partition_strategy(&self) -> PartitionStrategy {
        PartitionStrategy::from_str(&self.kafka_partition_strategy).unwrap()
    }

    pub fn checkpoint_strategy(&self) -> CheckpointStrategy {
        CheckpointStrategy::from_str(&self.checkpoint_strategy).unwrap()
    }
}

pub fn load_env_var() -> Option<EnvVars> {
    logger!();

    let vars = EnvVars::init_from_env();
    match vars {
        Ok(values) => {
            debug!("{:?}",values);
            info!("Environment Variables correctly loaded");
            
            Some(values)

        },
        Err(error) => {
            info!("Environment Variables NOT correctly loaded");
            debug!("{}", error);
            None
        }
    }
}

#[cfg(test)]
mod env_var_tests {
    use crate::env_var::{load_env_var, PartitionStrategy};

    //Running positive and negative case togheter to avoid issue with logger mut static var 
    #[test]
    fn test_env() {
        let mut vars= load_env_var();
        assert!(vars.is_none());
        
        std::env::set_var("KAFKA_BROKERS","test");
        std::env::set_var("KAFKA_TOPIC","test");
        std::env::set_var("KAFKA_NUM_PARTITIONS","1");

        vars = load_env_var();
        assert!(vars.is_some());
        matches!(vars.unwrap().kafka_partition_strategy(),PartitionStrategy::None);

        std::env::set_var("KAFKA_PARTITION_STRATEGY","RANDOM");
        vars = load_env_var();
        matches!(vars.unwrap().kafka_partition_strategy(),PartitionStrategy::Random);
    }
}
