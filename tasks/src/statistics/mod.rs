use async_trait::async_trait;

use coarsetime::{Instant, Duration};
use derive_new::new;
use kanal::AsyncReceiver;
use tokio::{time::interval_at, select};
use tokio_util::sync::CancellationToken;
use utilities::{logger::*, env_var::EnvVars, statistics::{Stats,StatsHolder}};

use crate::{Task, DataTransmitted};

#[derive(new)]
pub struct StatisticData {
    recv_time: Instant, 
    send_time: Instant, 
    size: usize,
    conn_key: u64
}

pub struct StatisticsTask {
    shutdown_token: CancellationToken,
    stats_rx: AsyncReceiver<DataTransmitted>,
    timeout: Duration,
    holder: StatsHolder
}

impl StatisticsTask {
    pub fn new(vars: &EnvVars, shutdown_token: CancellationToken, stats_rx: AsyncReceiver<DataTransmitted>) -> Self {
        let timeout = Duration::new(vars.stats_interval,0);
        let holder = StatsHolder::new(timeout);
        
        Self {timeout, holder,stats_rx, shutdown_token}
    }
}

#[async_trait]
impl Task for StatisticsTask {
    async fn run(&mut self) {
        //Arm the timer to produce statistics at regular intervals
        let start = tokio::time::Instant::now() + self.timeout.into();
        let mut timer = interval_at(start, self.timeout.into());

        loop  {
            select! {
                _ = self.shutdown_token.cancelled() => {
                    info!("Shutting down statistics task");
                    break
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
                            Some(msg) => self.holder.add_stat(msg.recv_time, msg.send_time, msg.size, msg.conn_key),
                            None => self.holder.add_loss(),
                        }
                    }
                }
            }
        }
    }
}
