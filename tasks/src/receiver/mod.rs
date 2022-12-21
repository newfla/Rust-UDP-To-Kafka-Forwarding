use std::{net::SocketAddr, time::Instant};

use async_trait::async_trait;

use kanal::AsyncSender;
use tokio::{net::UdpSocket, sync::broadcast, select};
use utilities::{logger::*, env_var::EnvVars};

use crate::{Task, DataPacket};

pub fn build_socket_from_env(vars: &EnvVars) -> SocketAddr {
    let ip: String = vars.listen_ip.to_owned();
    let port = vars.listen_port.to_string();
    (ip + ":" +&port).parse().unwrap()
}

pub struct ReceiverTask {
    addr: SocketAddr,
    buffer_size: usize,
    dispatcher_sender: AsyncSender<DataPacket>,
    shutdown_receiver: broadcast::Receiver<()>,
    shutdown_sender: broadcast::Sender<()>
}

impl ReceiverTask {
    pub fn new<F>(func: F, dispatcher: AsyncSender<DataPacket>, shutdown_receiver: broadcast::Receiver<()>, 
        shutdown_sender: broadcast::Sender<()>, vars: &EnvVars) -> Self 
    where
    F:Fn(&EnvVars) -> SocketAddr, {
            let addr = func(vars);
            let buffer_size = vars.buffer_size;
            Self {addr, dispatcher_sender: dispatcher, shutdown_receiver, shutdown_sender, buffer_size}
    }
}

#[async_trait]
impl Task for ReceiverTask {
    async fn run(&mut self) -> Result<(),String> {
        //Socket binding handling 
        let socket = UdpSocket::bind(self.addr).await;
        if let Err(err)= socket {
            error!("Socket binding failed. Reaseon: {}",err);
            Self::propagate_shutdown(&self.shutdown_sender);
            return Err(err.to_string());
        }

        let socket = socket.unwrap();
        let mut buf = vec![0u8; self.buffer_size];
        info!("Receiver task correctly started");

        //Handle incoming UDP packets 
        loop {
            select! {
                _ = self.shutdown_receiver.recv() => { 
                    info!("Shutting down receiver task");
                    return Ok(());
                }

                data = socket.recv_from(&mut buf) => {
                    match data {
                        Err(err) => {
                            error!("Socket recv failed. Reason: {}", err);
                            Self::propagate_shutdown(&self.shutdown_sender);
                            return Err(err.to_string());
                        },
                        Ok((len, addr)) => {
                            debug!("Received {} bytes from {}", len, addr);
                            //Not really unsafe :)
                            unsafe{
                                if self.dispatcher_sender.send((buf.get_unchecked(..len).to_vec(), addr, Instant::now())).await.is_err() {
                                    error!("Failed to send data to dispatcher");
                                    Self::propagate_shutdown(&self.shutdown_sender);
                                }
                            }
                        },
                    }
                }
            };
        }
    }
}
