use std::{time::Instant, net::SocketAddr, sync::Arc};
use ahash::AHashMap;
use fastrand::Rng;
use async_trait::async_trait;
use tokio::sync::Notify;
use ustr::{ustr, Ustr};
use derive_new::new;
use utilities::logger::debug;

mod statistics;
mod receiver;
mod dispatcher;
mod sender;
pub mod manager;

type DataPacket = (Vec<u8>, SocketAddr, Instant);
type PartitionDetails = (Option<i32>, Ustr, Ustr);
type Ticket = Arc<Notify>;

#[async_trait]
pub trait Task {
    async fn run(&mut self);
}

pub trait PartitionStrategy {
    fn partition(&mut self, addr: &SocketAddr) -> PartitionDetails {
        let key = ustr(&(addr.to_string()+"|auto"));
        (None, key,key)
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
    fn partition(&mut self, addr: &SocketAddr) -> PartitionDetails {
        let next = self.rng.i32(0..self.num_partitions);
        let addr_str = addr.to_string();
        let key = ustr(&(addr_str.clone() + "|"+ &next.to_string()));
        let order_key = ustr(&addr_str);
        (Some(next),key,order_key)
    }
}

#[derive(new)]
pub struct RoundRobinPartitionStrategy  {
    #[new(value = "fastrand::i32(0..num_partitions)")]
    start_partition: i32,
    num_partitions: i32
}


impl PartitionStrategy for RoundRobinPartitionStrategy  {
    fn partition(&mut self, addr: &SocketAddr) -> PartitionDetails {
        let next = (self.start_partition + 1) % self.num_partitions;
        self.start_partition = next;

        debug!("SockAddr: {} partition: {}",addr, next);

        let addr_str = addr.to_string();
        let key = ustr(&(addr_str.clone() + "|"+ &next.to_string()));
        let order_key = ustr(&addr_str);
        (Some(next),key,order_key)
    }
}

#[derive(new)]
pub struct StickyRoundRobinPartitionStrategy {
    #[new(default)]
    map_partition: AHashMap<SocketAddr,PartitionDetails>,
    #[new(value = "fastrand::i32(0..num_partitions)")]
    start_partition: i32,
    num_partitions: i32
}

impl PartitionStrategy for StickyRoundRobinPartitionStrategy {
    fn partition(&mut self, addr: &SocketAddr) -> PartitionDetails {
        if let Some (val) = self.map_partition.get(addr) {
            return *val;
        }
        let next = (self.start_partition + 1) % self.num_partitions;
        let key = ustr(&(addr.to_string() +"|"+ &next.to_string()));
        let val = (Some(next),key,key);
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
    fn partition(&mut self, addr: &SocketAddr) -> PartitionDetails {
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