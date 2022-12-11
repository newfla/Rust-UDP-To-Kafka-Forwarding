use std::{time::Instant, net::SocketAddr};
use fastrand::{Rng};
use async_trait::async_trait;
use tokio::sync::broadcast;
use ahash::AHashMap;
use ustr::ustr;
use derive_new::new;
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
    fn partition(&mut self, addr: &SocketAddr) -> (Option<i32>,&'static str) {
        (None, ustr(&(addr.to_string()+"|auto")).as_str())
    }
}

#[derive(Default)]
pub struct NonePartitionStrategy {}
impl PartitionStrategy for NonePartitionStrategy {}

#[derive(new)]
pub struct RandomPartitionStrategy {
    #[new(default)]
    rng: Rng,
    num_partitions: i32
}

impl PartitionStrategy for RandomPartitionStrategy {
    fn partition(&mut self, addr: &SocketAddr) -> (Option<i32>, &'static str) {
        let next = self.rng.i32(0..self.num_partitions);
        let key = addr.to_string() +"|"+ &next.to_string();
        (Some(next),ustr(&key).as_str())
    }
}

#[derive(new)]
pub struct RoundRobinPartitionStrategy  {
    #[new(value = "fastrand::i32(0..num_partitions)")]
    start_partition: i32,
    num_partitions: i32
}


impl PartitionStrategy for RoundRobinPartitionStrategy  {
    fn partition(&mut self, addr: &SocketAddr) -> (Option<i32>, &'static str){
        let next = (self.start_partition + 1) % self.num_partitions;
        self.start_partition = next;

        debug!("SockAddr: {} partition: {}",addr, next);

        let key = addr.to_string() +"|"+ &next.to_string();
        (Some(next),ustr(&key).as_str())
    }
}

#[derive(new)]
pub struct StickyRoundRobinPartitionStrategy {
    #[new(default)]
    map_partition: AHashMap<SocketAddr,(Option<i32>,&'static str)>,
    #[new(value = "fastrand::i32(0..num_partitions)")]
    start_partition: i32,
    num_partitions: i32
}

impl PartitionStrategy for StickyRoundRobinPartitionStrategy {
    fn partition(&mut self, addr: &SocketAddr) -> (Option<i32>, &'static str) {
        if let Some (val) = self.map_partition.get(addr) {
            return *val;
        }
        let next = (self.start_partition + 1) % self.num_partitions;
        let key = addr.to_string() +"|"+ &next.to_string();
        let val = (Some(next),ustr(&key).as_str());
        self.map_partition.insert(*addr,val);
        self.start_partition = next;

        debug!("SockAddr: {} partition: {}",addr, next);

        val
    }
}

pub enum PartitionStrategies {
    NonePartition(NonePartitionStrategy),
    RandomPartition(RandomPartitionStrategy),
    RoundRobinPartition(RoundRobinPartitionStrategy),
    StickyRoundRobinPartition(StickyRoundRobinPartitionStrategy)
}

impl PartitionStrategy for PartitionStrategies {
    fn partition(&mut self, addr: &SocketAddr) -> (Option<i32>, &'static str) {
        match self {
            PartitionStrategies::NonePartition(strategy) => strategy.partition(addr),
            PartitionStrategies::RandomPartition(strategy) => strategy.partition(addr),
            PartitionStrategies::RoundRobinPartition(strategy) => strategy.partition(addr),
            PartitionStrategies::StickyRoundRobinPartition(strategy) => strategy.partition(addr)
        }
    }
}

pub trait CheckpointStrategy {
    fn check(&self, _data: (&DataPacket,&Option<i32>)) -> bool {
        true
    }
}

#[derive(Default)]
pub struct OpenDoorsStrategy {}

impl CheckpointStrategy for OpenDoorsStrategy {}

#[derive(Default)]
pub struct ClosedDoorsStrategy {}

impl CheckpointStrategy for ClosedDoorsStrategy {
    fn check(&self, _data: (&DataPacket,&Option<i32>)) -> bool {
        false
    }
}

#[derive(Default)]
pub struct FlipCoinStrategy {}

impl CheckpointStrategy for FlipCoinStrategy {
    fn check(&self, _data: (&DataPacket,&Option<i32>)) -> bool {
        fastrand::bool()
    }
}

pub enum CheckpointStrategies {
    OpenDoors(OpenDoorsStrategy),
    ClosedDoors(ClosedDoorsStrategy),
    FlipCoin(FlipCoinStrategy),
}

impl CheckpointStrategy for CheckpointStrategies {
    fn check(&self, data: (&DataPacket,&Option<i32>)) -> bool {
        match self {
            CheckpointStrategies::OpenDoors(strategy) => strategy.check(data),
            CheckpointStrategies::ClosedDoors(strategy) => strategy.check(data),
            CheckpointStrategies::FlipCoin(strategy) => strategy.check(data)
        }
    }
}