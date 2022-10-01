use std::{time::Instant, net::SocketAddr, collections::HashMap};
use rand::{thread_rng, Rng};
use async_trait::async_trait;
use tokio::sync::broadcast;
use utilities::logger::{error, debug};

mod statistics;
mod receiver;
mod dispatcher;
mod sender;
pub mod manager;


type DataPacket = (Vec<u8>, SocketAddr, Instant);

#[async_trait]
pub trait Task {
    async fn run(&mut self) -> Result<(),String>;

    fn propagate_shutdown(shutdown_sender: &broadcast::Sender<()>){
        let shutdown_error_msg = "Failed to propagte spread shutdown signal!";
        if shutdown_sender.send(()).is_err() {
            error!("{}",shutdown_error_msg);
        }
    }
}

pub trait PartitionStrategy {
    fn partition(&mut self, _addr: &SocketAddr) -> Option<i32> {
        None
    }
}

#[derive(Default)]
pub struct NonePartitionStrategy {}
impl PartitionStrategy for NonePartitionStrategy {}

pub struct RandomPartitionStrategy {
    num_partitions: i32
}

impl RandomPartitionStrategy {
    pub fn new(kafka_num_partitions: i32) -> Self {
        Self { num_partitions: kafka_num_partitions }
    }
}

impl PartitionStrategy for RandomPartitionStrategy {
    fn partition(&mut self, _addr: &SocketAddr) -> Option<i32> {
        Some(thread_rng().gen_range(0..self.num_partitions))
    }
}

pub struct RoundRobinPartitionStrategy  {
    start_partition: i32,
    num_partitions: i32
}

impl RoundRobinPartitionStrategy  {
    pub fn new(kafka_num_partitions: i32) -> Self {
        Self { 
            start_partition: thread_rng().gen_range(0..kafka_num_partitions),
            num_partitions: kafka_num_partitions
        }
    }
}

impl PartitionStrategy for RoundRobinPartitionStrategy  {
    fn partition(&mut self, addr: &SocketAddr) -> Option<i32> {
        let next = self.start_partition + 1 % self.num_partitions;
        self.start_partition = next;

        debug!("SockAddr: {} partition: {}",addr, next);

        Some(next)
    }
}

pub struct StickyRoundRobinPartitionStrategy {
    map_partition: HashMap<SocketAddr, i32>,
    start_partition: i32,
    num_partitions: i32
}

impl StickyRoundRobinPartitionStrategy {
    pub fn new(kafka_num_partitions: i32) -> Self {
        Self {
            map_partition: HashMap::default(),
            start_partition: thread_rng().gen_range(0..kafka_num_partitions),
            num_partitions: kafka_num_partitions
        }
    }
}

impl PartitionStrategy for StickyRoundRobinPartitionStrategy {
    fn partition(&mut self, addr: &SocketAddr) -> Option<i32> {
        if let Some (x) = self.map_partition.get(addr) {
            return Some(*x);
        }
        let next = (self.start_partition + 1) % self.num_partitions;
        self.map_partition.insert(*addr, next);
        self.start_partition = next;

        debug!("SockAddr: {} partition: {}",addr, next);

        Some(next)
    }
}

pub trait ShouldGoOn {
    fn should_go_on(&self, _data: (&DataPacket,&Option<i32>)) -> bool {
        true
    }
}

#[derive(Default)]
pub struct AlwaysShouldGoOn {}

impl ShouldGoOn for AlwaysShouldGoOn {}

#[derive(Default)]
pub struct NeverShouldGoOn {}

impl ShouldGoOn for NeverShouldGoOn {
    fn should_go_on(&self, _data: (&DataPacket,&Option<i32>)) -> bool {
        false
    }
}
