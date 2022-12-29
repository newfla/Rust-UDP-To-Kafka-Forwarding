use std::{net::SocketAddr, time::Instant};

use async_trait::async_trait;
use kanal::AsyncSender;
use tokio::{net::UdpSocket, select};
use tokio_util::sync::CancellationToken;
use utilities::{logger::*, env_var::EnvVars};

use crate::{Task, DataPacket};

pub struct ReceiverTask {
    addr: SocketAddr,
    buffer_size: usize,
    dispatcher_sender: AsyncSender<DataPacket>,
    shutdown_token: CancellationToken
}

impl ReceiverTask {
    pub fn new(dispatcher: AsyncSender<DataPacket>, shutdown_token: CancellationToken, vars: &EnvVars) -> Self {
            let addr = Self::build_socket_from_env(vars);
            let buffer_size = vars.buffer_size;
            Self {addr, dispatcher_sender: dispatcher, shutdown_token, buffer_size}
    }

    fn build_socket_from_env(vars: &EnvVars) -> SocketAddr {
        let ip = vars.listen_ip.to_owned();
        let port = vars.listen_port.to_string();
        (ip + ":" +&port).parse().unwrap()
    }
}

#[async_trait]
impl Task for ReceiverTask {
    async fn run(&mut self) {
        //Socket binding handling 
        let socket = UdpSocket::bind(self.addr).await;
        if let Err(err)= socket {
            error!("Socket binding failed. Reaseon: {}",err);
            self.shutdown_token.cancel();
            return;
        }

        let socket = socket.unwrap();
        let mut buf = vec![0u8; self.buffer_size];
        info!("Receiver task correctly started");

        //Handle incoming UDP packets 
        loop {
            select! {
                _ = self.shutdown_token.cancelled() => { 
                    info!("Shutting down receiver task");
                }

                data = socket.recv_from(&mut buf) => {
                    match data {
                        Err(err) => {
                            error!("Socket recv failed. Reason: {}", err);
                            self.shutdown_token.cancel();
                        },
                        Ok((len, addr)) => {
                            debug!("Received {} bytes from {}", len, addr);
                            //Not really unsafe :)
                            unsafe{
                                if self.dispatcher_sender.send((buf.get_unchecked(..len).to_vec(), addr, Instant::now())).await.is_err() {
                                    error!("Failed to send data to dispatcher");
                                    self.shutdown_token.cancel();
                                }
                            }
                        },
                    }
                }
            };
        }
    }
}
