
use std::{time::{Duration, Instant}, fmt::Display};

use byte_unit::Byte;
use derive_new::new;
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
    processed_packets: usize,
    bandwidth: f64,
    min_latency: Duration,
    max_latency: Duration,
    average_latency: Duration,
    loss_packets: usize,
    unique_connections: usize
}

impl Display for StatSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let min = self.min_latency.as_millis();
        let max = self.max_latency.as_millis();
        let average = self.average_latency.as_millis();

        let bandwidth = self.bandwidth*8.;
        let bandwidth = Byte::from_bytes(bandwidth as u128).get_appropriate_unit(false).to_string();
        let bandwidth =bandwidth[0..bandwidth.len()-1].to_string();

        writeln!(f,"\nPackets processed: {}",self.processed_packets).and(
            writeln!(f,"Loss packets: {}",self.loss_packets)).and(
            writeln!(f,"Unique connections: {}",self.unique_connections)).and(
            writeln!(f,"Bandwidth: {}bit/s", bandwidth)).and(
            writeln!(f,"Latency: <min: {}, max: {}, average: {}> ms",min,max,average)
        )
    }
}

#[derive(Clone,new)]
pub struct StatsHolder {
    period: Duration,
    #[new(default)]
    stats_vec: Vec<StatElement>,
    #[new(default)]
    loss_packets: usize,
    #[new(default)]
    active_connections: IntSet<u64>

}

impl Default for StatsHolder {
    fn default() -> Self {
        StatsHolder { period: Duration::new(10,0), stats_vec: Vec::default(), loss_packets: usize::default(), active_connections:IntSet::default()}
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
        self.loss_packets = usize::default();
        self.active_connections.clear();
    }

    fn calculate(&self) -> Option<StatSummary> {
        if self.stats_vec.is_empty(){
            None
        } else {
            let latency: Vec<Duration> = self.stats_vec.iter()
                                                       .map(|elem| {elem.latency})
                                                       .collect();

            let min_latency = *latency.iter().min().unwrap();
            let max_latency = *latency.iter().max().unwrap();
            let average_latency = latency.iter().sum::<Duration>().div_f64(latency.len() as f64);

            let mut bandwidth: f64 = self.stats_vec.iter()
                                                   .map(|elem| {elem.size})
                                                   .sum::<usize>() as f64;

            bandwidth/=self.period.as_secs() as f64;

            let packet_processed = latency.len();

            let unique_connections = self.active_connections.len();

            Some( StatSummary { processed_packets: packet_processed, 
                                bandwidth,
                                min_latency,
                                max_latency,
                                average_latency,
                                loss_packets: self.loss_packets,
                                unique_connections})
        }
    }

    fn calculate_and_reset(&mut self) -> Option<StatSummary> {
        let res = self.calculate();
        self.reset();
        res
    }
    
    fn add_loss(&mut self) {
        self.loss_packets+=1;
    }  
}


#[derive(Copy,Clone)]
struct StatElement {
    latency: Duration,
    size: usize
}

#[cfg(test)]
mod statistics_tests {
    use std::time::{Duration,Instant};

    use crate::statistics::*;

    #[test]
    fn test_simple_stats_holder(){
        let mut stats: Box<dyn Stats> = Box::new(StatsHolder::default());
        let reference = Instant::now();

        stats.add_loss();

        stats.add_stat(
            reference,
            reference.checked_add(Duration::new(2,0)).unwrap(),
            128,
            1);

        stats.add_stat(
            reference.checked_add(Duration::new(3,0)).unwrap(),
            reference.checked_add(Duration::new(4,0)).unwrap(),
            128,
            2);

        stats.add_stat(
            reference.checked_add(Duration::new(4,0)).unwrap(),
            reference.checked_add(Duration::new(10,0)).unwrap(),
            256,
            1);

        let stats_oracle = StatSummary {
            processed_packets: 3,
            bandwidth:512. / 10.,
            min_latency: Duration::new(1,0),
            max_latency: Duration::new(6,0),
            average_latency: Duration::new(3,0),
            loss_packets: 1,
            unique_connections: 2
        };

        let stats = stats.calculate_and_reset();
        assert_ne!(stats,None);

        let stats = stats.unwrap();
        assert_eq!(stats,stats_oracle);

        println!("{}",stats);
    }
}
