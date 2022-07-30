use std::{time::{Duration,Instant}, sync::{Arc, Mutex}};

use async_trait::async_trait;

use tokio::{sync::{broadcast, mpsc::UnboundedReceiver}, time::interval, select, task::spawn_blocking};
use utilities::{logger::*, env_var::EnvVars};
use utilities::statistics::{Stats, StatsHolder};

use crate::Task;
pub struct StatisticIncoming {
    addr: std::net::SocketAddr, 
    recv_time: Instant, 
    send_time: Instant, 
    size: usize
}
impl StatisticIncoming {
    pub fn new(addr: std::net::SocketAddr, recv_time: Instant, send_time: Instant, size: usize) -> Self {
        Self { addr, recv_time, send_time, size}
    }
}

pub struct StatisticsTask {
    shutdown_receiver: broadcast::Receiver<()>,
    stats_rx: UnboundedReceiver<StatisticIncoming>,
    timeout: Duration,
    holder: Arc<Mutex<StatsHolder>> 
}

impl StatisticsTask {
    pub fn new(vars: &EnvVars, shutdown_receiver: broadcast::Receiver<()>,stats_rx: UnboundedReceiver<StatisticIncoming>) -> Self {
        let holder = Arc::new(Mutex::new(StatsHolder::new( Duration::new(vars.stats_interval,0))));
        Self { timeout: Duration::new(vars.stats_interval,0), holder,stats_rx, shutdown_receiver}
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
                    
                    let holder = Arc::clone(&self.holder);
                    if let Ok(Some(summary)) = spawn_blocking(move || {holder.lock().unwrap().calculate_and_reset()}).await {
                        info!("{}",summary);
                    } else {
                        info!("No data in transit in the last {} seconds", self.timeout.as_secs());
                    }
                }
                stat = self.stats_rx.recv() => {
                    if let Some(msg) = stat {
                        self.holder.lock().unwrap().add_stat(msg.addr, msg.recv_time, msg.send_time, msg.size);
                    }
                }
            }
        }
    }
}