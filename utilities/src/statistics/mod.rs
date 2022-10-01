
use std::{cmp,collections::HashMap, net::SocketAddr, time::{Duration, Instant}, fmt::Display, ops::Add};

use byte_unit::Byte;

pub trait Stats {
    fn add_stat(&mut self, addr: SocketAddr, recv_time: Instant, send_time:Instant, size: usize);
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
    average_latency: Duration
}

impl Add for StatSummary {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            processed_packets: self.processed_packets + rhs.processed_packets,
            bandwidth: self.bandwidth + rhs.bandwidth,
            min_latency: cmp::min(self.min_latency, rhs.min_latency),
            max_latency: cmp::max(self.max_latency, rhs.max_latency),
            average_latency: (self.average_latency + rhs.average_latency).div_f64(2.),
        }
    }
}

impl Display for StatSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let min = self.min_latency.as_millis();
        let max = self.max_latency.as_millis();
        let average = self.average_latency.as_millis();

        let bandwidth = self.bandwidth*8.;
        let bandwidth = Byte::from_bytes(bandwidth as u128).get_appropriate_unit(false).to_string();
        let bandwidth =bandwidth[0..bandwidth.len()-1].to_string();

        writeln!(f,"Packets processed: {}",self.processed_packets).and(
            writeln!(f,"Bandwidth: {}bit/s", bandwidth)).and(
            writeln!(f,"Latency: <min: {}, max: {}, average: {}> ms",min,max,average)
        )
    }
}

#[derive(Clone)]
pub struct StatsHolder {
    period: Duration,
    stats_map: HashMap<SocketAddr,Vec<StatElement>>
}

impl Default for StatsHolder {
    fn default() -> Self {
        StatsHolder { period: Duration::new(10,0), stats_map: HashMap::default() }
    }   
}
impl StatsHolder {
    pub fn new(period: Duration) -> Self {
        StatsHolder { period, stats_map: HashMap::default() }
    }
}

impl Stats for StatsHolder {
    fn add_stat(&mut self, addr: SocketAddr, recv_time: Instant, send_time:Instant, size: usize){

        let latency = send_time.duration_since(recv_time);

        self.stats_map.entry(addr).or_default().push(StatElement{latency,size});

    }

    fn reset(&mut self) {
        self.stats_map.clear();
    }

    fn calculate(&self) -> Option<StatSummary> {
        if self.stats_map.is_empty(){
            None
        } else {
            let latency: Vec<Duration> = self.stats_map.values()
                                                       .flatten()
                                                       .map(|elem| {elem.latency})
                                                       .collect();

            let min_latency = *latency.iter().min().unwrap();
            let max_latency = *latency.iter().max().unwrap();
            let average_latency = latency.iter().sum::<Duration>().div_f64(latency.len() as f64);

            let mut bandwidth: f64 = self.stats_map.values()
                                                   .flatten()
                                                   .map(|elem| {elem.size})
                                                   .sum::<usize>() as f64;

            bandwidth/=self.period.as_secs() as f64;

            let packet_processed = latency.len();

            Some( StatSummary { processed_packets: packet_processed, 
                                bandwidth,
                                min_latency,
                                max_latency,
                                average_latency})
        }
    }

    fn calculate_and_reset(&mut self) -> Option<StatSummary> {
        let res = self.calculate();
        self.reset();
        res
    }        
}

#[derive(Clone)]
pub struct SimpleStatsHolder {
    period: Duration,
    stats_vec: Vec<StatElement>
}

impl Default for SimpleStatsHolder {
    fn default() -> Self {
        SimpleStatsHolder { period: Duration::new(10,0), stats_vec: Vec::default() }
    }   
}

impl SimpleStatsHolder {
    pub fn new(period: Duration) -> Self {
        SimpleStatsHolder { period, stats_vec: Vec::default() }
    }
}

impl Stats for SimpleStatsHolder {
    fn add_stat(&mut self, _addr: SocketAddr, recv_time: Instant, send_time:Instant, size: usize){

        let latency = send_time.duration_since(recv_time);

        self.stats_vec.push(StatElement{latency,size});

    }

    fn reset(&mut self) {
        self.stats_vec.clear();
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

            Some( StatSummary { processed_packets: packet_processed, 
                                bandwidth,
                                min_latency,
                                max_latency,
                                average_latency})
        }
    }

    fn calculate_and_reset(&mut self) -> Option<StatSummary> {
        let res = self.calculate();
        self.reset();
        res
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
    fn test_stats_holder(){
        let mut stats: Box<dyn Stats> = Box::new(StatsHolder::default());
        let reference = Instant::now();

        stats.add_stat("127.0.0.1:8080".parse().unwrap(), 
                        reference,
                        reference.checked_add(Duration::new(2,0)).unwrap(),
                        128);

        stats.add_stat("127.0.0.1:8080".parse().unwrap(), 
        reference.checked_add(Duration::new(3,0)).unwrap(),
        reference.checked_add(Duration::new(4,0)).unwrap(),
        128);

        stats.add_stat("127.0.0.1:8081".parse().unwrap(), 
        reference.checked_add(Duration::new(4,0)).unwrap(),
        reference.checked_add(Duration::new(10,0)).unwrap(),
        256);

        let stats_oracle = StatSummary {
            processed_packets: 3,
            bandwidth:512. / 10.,
            min_latency: Duration::new(1,0),
            max_latency: Duration::new(6,0),
            average_latency: Duration::new(3,0)
        };

        let stats = stats.calculate_and_reset();
        assert_ne!(stats,None);

        let stats = stats.unwrap();
        assert_eq!(stats,stats_oracle);

        println!("{}",stats);
    }

    #[test]
    fn test_simple_stats_holder(){
        let mut stats: Box<dyn Stats> = Box::new(SimpleStatsHolder::default());
        let reference = Instant::now();

        stats.add_stat("127.0.0.1:8080".parse().unwrap(), 
                        reference,
                        reference.checked_add(Duration::new(2,0)).unwrap(),
                        128);

        stats.add_stat("127.0.0.1:8080".parse().unwrap(), 
        reference.checked_add(Duration::new(3,0)).unwrap(),
        reference.checked_add(Duration::new(4,0)).unwrap(),
        128);

        stats.add_stat("127.0.0.1:8081".parse().unwrap(), 
        reference.checked_add(Duration::new(4,0)).unwrap(),
        reference.checked_add(Duration::new(10,0)).unwrap(),
        256);

        let stats_oracle = StatSummary {
            processed_packets: 3,
            bandwidth:512. / 10.,
            min_latency: Duration::new(1,0),
            max_latency: Duration::new(6,0),
            average_latency: Duration::new(3,0)
        };

        let stats = stats.calculate_and_reset();
        assert_ne!(stats,None);

        let stats = stats.unwrap();
        assert_eq!(stats,stats_oracle);

        println!("{}",stats);
    }
}