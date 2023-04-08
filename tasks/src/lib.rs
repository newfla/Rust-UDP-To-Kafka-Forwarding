use std::{net::SocketAddr, sync::Arc};
use cached::proc_macro::cached;
use fastrand::Rng;
use async_trait::async_trait;
use statistics::StatisticData;
use tokio::sync::Notify;
use ustr::{ustr, Ustr};
use derive_new::new;
use utilities::{logger::debug};
use coarsetime::Instant;

mod statistics;
mod receiver;
mod dispatcher;
mod sender;
pub mod manager;

type DataPacket = (Vec<u8>, (usize,SocketAddr), Instant);
type PartitionDetails = (Option<i32>, Ustr, Ustr);
type Ticket = Arc<Notify>;
type Strategies = (CheckpointStrategies, PartitionStrategies);
type DataTransmitted = Option<StatisticData>;

#[async_trait]
pub trait Task {
    async fn run(&mut self);
}

pub trait PartitionStrategy {
    fn partition(&mut self, addr: &SocketAddr) -> PartitionDetails;
}

#[derive(new)]
pub enum PartitionStrategies {
    NonePartition,
    RandomPartition{#[new(default)]  rng: Rng, num_partitions: i32},
    RoundRobinPartition{#[new(value = "fastrand::i32(0..num_partitions)")] start_partition: i32, num_partitions: i32},
    StickyRoundRobinPartition {#[new(value = "fastrand::i32(0..num_partitions)")] start_partition: i32, num_partitions: i32}
}

impl PartitionStrategy for PartitionStrategies {
    fn partition(&mut self, addr: &SocketAddr) -> PartitionDetails {
        match self {
            PartitionStrategies::NonePartition => none_partition(addr),
            PartitionStrategies::RandomPartition { rng, num_partitions } => random_partition(addr, *num_partitions, rng),
            PartitionStrategies::RoundRobinPartition { start_partition, num_partitions } => round_robin_partition(addr, start_partition, *num_partitions),
            PartitionStrategies::StickyRoundRobinPartition { start_partition, num_partitions } => sticky_partition(addr, start_partition, *num_partitions),
            
        }
    }
}

#[cached(key = "SocketAddr", convert = r#"{ *addr }"#)]
fn none_partition(addr: &SocketAddr) -> PartitionDetails {
    let key = ustr(&(addr.to_string()+"|auto"));
    (None, key,key)
}

fn random_partition(addr: &SocketAddr, num_partitions: i32, rng: &Rng) -> PartitionDetails {
    let next = rng.i32(0..num_partitions);
    let addr_str = addr.to_string();
    let key = ustr(&(addr_str.clone() + "|"+ &next.to_string()));
    let order_key = ustr(&addr_str);

    (Some(next),key,order_key)
}

fn round_robin_partition(addr: &SocketAddr, start_partition: &mut i32, num_partitions: i32) -> PartitionDetails {
    let next = (*start_partition + 1) % num_partitions;
    *start_partition = next;

    debug!("SockAddr: {} partition: {}",addr, next);

    let addr_str = addr.to_string();
    let key = ustr(&(addr_str.clone() + "|"+ &next.to_string()));
    let order_key = ustr(&addr_str);

    (Some(next),key,order_key)
}

fn sticky_partition(addr: &SocketAddr, start_partition: &mut i32, num_partitions: i32) -> PartitionDetails {
    let next = (*start_partition + 1) % num_partitions;
    *start_partition = next;

    sticky_partition_internal(addr, next)
}

#[cached(key = "SocketAddr", convert = r#"{ *addr }"#)]
fn sticky_partition_internal(addr: &SocketAddr, next: i32) -> PartitionDetails {
    let key = ustr(&(addr.to_string() +"|"+ &next.to_string()));
    let val = (Some(next),key,key);

    debug!("SockAddr: {} partition: {}",addr, next);

    val
}

pub trait CheckpointStrategy {
    fn check(&self, data: (&DataPacket,&Option<i32>)) -> bool;
}

pub enum CheckpointStrategies {
    OpenDoors,
    ClosedDoors,
    FlipCoin,
}

impl CheckpointStrategy for CheckpointStrategies {
    fn check(&self, _data: (&DataPacket,&Option<i32>)) -> bool {
        match self {
            CheckpointStrategies::OpenDoors => true,
            CheckpointStrategies::ClosedDoors => false,
            CheckpointStrategies::FlipCoin => fastrand::bool()
        }
    }
}
