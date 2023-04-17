use std::{net::SocketAddr, future::{IntoFuture, Future}, pin::Pin};

use branches::unlikely;
use coarsetime::Instant;
use kanal::AsyncSender;
use tokio::{net::UdpSocket, select, spawn, io};
use tokio_util::sync::CancellationToken;
use utilities::{logger::*, env_var::EnvVars};
use tokio_dtls_stream_sink::{Server, Session};
use openssl::ssl::{SslContext, SslFiletype, SslMethod};

use crate::DataPacket;

#[derive(Clone)]
pub struct ReceiverTask {
    addr: SocketAddr,
    buffer_size: usize,
    dispatcher_sender: AsyncSender<DataPacket>,
    shutdown_token: CancellationToken,
    dtls_settings: (bool, Option<String>,Option<String>)
}

impl ReceiverTask {
    pub fn new(dispatcher: AsyncSender<DataPacket>, shutdown_token: CancellationToken, vars: &EnvVars) -> Self {
            let addr = Self::build_socket_from_env(vars);
            let buffer_size = vars.buffer_size;
            let dtls_settings = (vars.use_dtls,vars.server_cert.clone(),vars.server_key.clone());
            Self {addr, dispatcher_sender: dispatcher, shutdown_token, buffer_size,dtls_settings}
    }

    fn build_socket_from_env(vars: &EnvVars) -> SocketAddr {
        let ip = vars.listen_ip.to_owned();
        let port = vars.listen_port.to_string();
        (ip + ":" +&port).parse().unwrap()
    }

    async fn error_run(&self){
        error!("Key AND/OR Certificate for TLS were not provided");
        debug!("Certificate: {:?}", self.dtls_settings.1);
        debug!("Key: {:?}", self.dtls_settings.2);
        self.shutdown_token.cancel();
    }

    async fn plain_run(&self, socket:UdpSocket){

        //Handle incoming UDP packets 
        //We don't need to check shutdown_token.cancelled() using select!. Infact, dispatcher_sender.send().is_err() <=> shutdown_token.cancelled() 
        loop {
            let mut buf  = Vec::with_capacity(self.buffer_size);
            let _ = socket.readable().await;

            match socket.try_recv_buf_from(&mut buf) {
                Err(ref e) if unlikely(e.kind() == io::ErrorKind::WouldBlock) => {
                    continue;
                },
                Err(err) => {
                    error!("Socket recv failed. Reason: {}", err);
                    self.shutdown_token.cancel();
                    break;
                },
                Ok(data) => {
                    if unlikely(self.dispatcher_sender.send((buf,data,Instant::now())).await.is_err()) {
                        error!("Failed to send data to dispatcher");
                        info!("Shutting down receiver task");
                        self.shutdown_token.cancel();
                        break;
                    }
                }
            }
        }
    }

    async fn dtls_run(&self, socket:UdpSocket, cert:String, key:String){
        let mut server = Server::new(socket);
        match Self::build_ssl_context(cert,key) {
            Err(_) => {
                info!("Shutting down receiver task");
                self.shutdown_token.cancel();
            },

            Ok(ctx) => {
                loop {
                    select! {
                        _ = self.shutdown_token.cancelled() => { 
                            info!("Shutting down receiver task");
                            break;
                        }
                        //Accept new DTLS connections (handshake phase)
                        session = server.accept(Some(&ctx)) => if let Ok(session) = session {
                            self.handle_dtls_session(session)
                        }
                    }
                }
            }
        } 
    }

    fn handle_dtls_session(&self, mut session:Session){
        let dispatcher_sender= self.dispatcher_sender.clone();
        let shutdown_token= self.shutdown_token.clone();

        let mut buf = vec![0u8; self.buffer_size];
        let peer = session.peer();
        spawn(async move {
            //Handle incoming UDP packets for each peer
            //session.read.is_err() => closed connection
            //We don't need to check shutdown_token.cancelled() using select!. Infact dispatcher_sender.send().is_err() => shutdown_token.cancelled() 
            loop {
                select! {
                    _ = shutdown_token.cancelled() => break,
                    res = session.read(&mut buf) => match res {
                        Err(err) => {
                            error!("Peer connection closed. Reason: {}", err);
                            break;
                        },
                        Ok(len) => {
                            if unlikely(dispatcher_sender.send((buf.clone(),(len,peer),Instant::now())).await.is_err()) {
                                error!("Failed to send data to dispatcher");
                                shutdown_token.cancel();
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    fn build_ssl_context(cert:String, key:String) -> Result<SslContext,()> {
        let mut ctx = SslContext::builder(SslMethod::dtls()).unwrap();

        let setup_context = ctx.set_private_key_file(key, SslFiletype::PEM)
            .and_then(|_| ctx.set_certificate_chain_file(cert))
            .and_then(|_| ctx.check_private_key());
        
        match setup_context {
            Ok(_) => Ok(ctx.build()),
            Err(_) => {
                error!("Something is wrong with certificate AND/OR key file(s)");
                Err(())
            }
        }
    }

    async fn run(self) {
        //Socket binding handling 
        let socket = UdpSocket::bind(self.addr).await;
        if let Err(err) = socket {
            error!("Socket binding failed. Reaseon: {}", err);
            self.shutdown_token.cancel();
            return;
        }

        let socket = socket.unwrap();
        match &self.dtls_settings {
            (false, _, _) => self.plain_run(socket).await,
            (true, Some(cert), Some(key)) => self.dtls_run(socket, cert.to_owned(), key.to_owned()).await,
            (true,_,_) =>self.error_run().await
        }
    }
}

impl IntoFuture for ReceiverTask {
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.run())
    }
}
