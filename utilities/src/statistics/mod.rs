
use std::fmt::Display;
use coarsetime::{Duration, Instant};

use byte_unit::Byte;
pub use derive_new::new;
use nohash_hasher::IntSet;

pub trait Stats {
    fn add_loss(&mut self);
    fn add_stat(&mut self, recv_time: Instant, send_time: Instant, size: usize, key: u64);
    fn calculate_and_reset(&mut self) -> Option<StatSummary>;
    fn calculate(&self) -> Option<StatSummary>;
    fn reset(&mut self);
}


#[derive(Debug, PartialEq)]
pub struct StatSummary {
    bandwidth: f32,
    min_latency: Duration,
    max_latency: Duration,
    average_latency: Duration,
    lost_packets: usize,
    unique_connections: usize
}

impl Display for StatSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let min = self.min_latency.as_millis();
        let max = self.max_latency.as_millis();
        let average = self.average_latency.as_millis();

        let bandwidth = self.bandwidth*8.;
        let bandwidth = Byte::from_bytes(bandwidth as u128).get_appropriate_unit(false).to_string();
        let bandwidth = &bandwidth[0..bandwidth.len()-1];

        writeln!(f,"\nLost packets: {}", self.lost_packets).and(
        writeln!(f,"Unique connections: {}", self.unique_connections)).and(
        writeln!(f,"Bandwidth: {bandwidth}bit/s")).and(
        writeln!(f,"Latency: <min: {min}, max: {max}, average: {average}> ms")
        )
    }
}

#[derive(Clone,new)]
pub struct StatsHolder {
    period: Duration,
    #[new(default)]
    stats_vec: Vec<StatElement>,
    #[new(default)]
    lost_packets: usize,
    #[new(default)]
    active_connections: IntSet<u64>

}

impl Default for StatsHolder {
    fn default() -> Self {
        StatsHolder { period: Duration::new(10,0), stats_vec: Vec::default(), lost_packets: usize::default(), active_connections:IntSet::default()}
    }   
}


impl Stats for StatsHolder {
    fn add_stat(&mut self, recv_time: Instant, send_time:Instant, size: usize, key:u64){

        let latency = send_time.duration_since(recv_time);

        self.stats_vec.push(StatElement{latency,size});

        self.active_connections.insert(key);

    }

    fn reset(&mut self) {
        self.stats_vec.clear();
        self.lost_packets = usize::default();
        self.active_connections.clear();
    }

    fn calculate(&self) -> Option<StatSummary> {
        if self.stats_vec.is_empty(){
            return None
        }

        let latency = self.stats_vec.iter()
                                                    .map(|elem| {elem.latency});

        let packet_processed = latency.len();

        let min_latency = latency.clone().min().unwrap();
        let max_latency = latency.clone().max().unwrap();
        let average_latency = latency.reduce(|acc, e| acc + e).unwrap() / packet_processed as u32;

        let mut bandwidth = self.stats_vec.iter()
                                                .map(|elem| {elem.size})
                                                .sum::<usize>() as f32;

        bandwidth /= self.period.as_secs() as f32;


        Some( StatSummary { bandwidth,
                            min_latency,
                            max_latency,
                            average_latency,
                            lost_packets: self.lost_packets,
                            unique_connections: self.active_connections.len()})
    }

    fn calculate_and_reset(&mut self) -> Option<StatSummary> {
        let res = self.calculate();
        self.reset();
        res
    }
    
    fn add_loss(&mut self) {
        self.lost_packets += 1;
    }
}


#[derive(Copy,Clone)]
struct StatElement {
    latency: Duration,
    size: usize
}

#[cfg(test)]
mod statistics_tests {
    use coarsetime::{Duration, Instant};

    use crate::statistics::*;

    #[test]
    fn test_stats_holder(){
        let mut stats: Box<dyn Stats> = Box::new(StatsHolder::default());
        let reference = Instant::now();

        stats.add_loss();

        stats.add_stat(
            reference,
            reference + Duration::new(2,0),
            128,
            1);

        stats.add_stat(
            reference + Duration::new(3,0),
            reference + Duration::new(4,0),
            128,
            2);

        stats.add_stat(
            reference + Duration::new(4,0),
            reference + Duration::new(10,0),
            256,
            1);

        let stats_oracle = StatSummary {
            bandwidth:512. / 10.,
            min_latency: Duration::new(1,0),
            max_latency: Duration::new(6,0),
            average_latency: Duration::new(3,0),
            lost_packets: 1,
            unique_connections: 2
        };

        let stats = stats.calculate_and_reset();
        assert_ne!(stats,None);

        let stats = stats.unwrap();
        assert_eq!(stats,stats_oracle);

        println!("{}",stats);
    }
}
