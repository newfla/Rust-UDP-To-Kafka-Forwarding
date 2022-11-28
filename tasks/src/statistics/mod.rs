use std::time::{Duration,Instant};

use async_trait::async_trait;

use kanal::AsyncReceiver;
use tokio::{sync::broadcast, time::interval, select};
use utilities::{logger::*, env_var::EnvVars, statistics::SimpleStatsHolder};
use utilities::statistics::{Stats, StatsHolder};

use crate::Task;

pub enum StatisticIncoming{
    DataTransmitted(StatisticData),
    DataLoss
}
pub struct StatisticData {
    addr: std::net::SocketAddr, 
    recv_time: Instant, 
    send_time: Instant, 
    size: usize
}
impl StatisticData {
    pub fn new(addr: std::net::SocketAddr, recv_time: Instant, send_time: Instant, size: usize) -> Self {
        Self { addr, recv_time, send_time, size}
    }
}

pub struct StatisticsTask {
    shutdown_receiver: broadcast::Receiver<()>,
    stats_rx: AsyncReceiver<StatisticIncoming>,
    timeout: Duration,
    holder: Box<dyn Stats + Send>
}

impl StatisticsTask {
    pub fn new(vars: &EnvVars, shutdown_receiver: broadcast::Receiver<()>,stats_rx: AsyncReceiver<StatisticIncoming>, simple: bool) -> Self {
        let timeout = Duration::new(vars.stats_interval,0);
        let holder: Box<dyn Stats + Send> = if simple {
            Box::new(SimpleStatsHolder::new(timeout))
        }
        else {
           Box::new(StatsHolder::new(timeout))
        };
        
        Self {timeout, holder,stats_rx, shutdown_receiver}
    }
}

#[async_trait]
impl Task for StatisticsTask {
    async fn run(&mut self) -> Result<(),String> {
        //Arm the timer to produce statistics at regular intervals 
        let mut timer = interval(self.timeout);

        loop  {
            select! {
                _ = self.shutdown_receiver.recv() => {
                    info!("Shutting down statistics task");
                    return Ok(());
                }
                _ = timer.tick() => {
                    if let Some(summary) = self.holder.calculate_and_reset() {
                        info!("{}",summary);
                    } else {
                        info!("No data in transit in the last {} seconds", self.timeout.as_secs());
                    }
                }
                stat = self.stats_rx.recv() => {
                    if let Ok(data) = stat {
                        match data {
                            StatisticIncoming::DataTransmitted(msg) => self.holder.add_stat(msg.addr, msg.recv_time, msg.send_time, msg.size),
                            StatisticIncoming::DataLoss => self.holder.add_loss(),
                        }
                        
                    }
                }
            }
        }
    }
}