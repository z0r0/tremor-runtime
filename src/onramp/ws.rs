// Copyright 2018-2020, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::onramp::prelude::*;
use async_std::sync::Sender;
use futures::{select, FutureExt, StreamExt};
use halfbrown::HashMap;
use serde_yaml::Value;
use simd_json::prelude::*;
use tungstenite::protocol::Message;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The port to listen on.
    pub port: u16,
    /// Host to listen on
    pub host: String,
}

pub struct Ws {
    pub config: Config,
}

impl onramp::Impl for Ws {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            Ok(Box::new(Self { config }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}
enum WsOnrampMessage {
    Data(u64, EventOriginUri, Vec<u8>, Sender<Event>),
}

use async_std::net::{TcpListener, TcpStream};
use async_std::task;
use futures_util::sink::SinkExt;

async fn handle_connection(
    loop_tx: Sender<WsOnrampMessage>,
    raw_stream: TcpStream,
    mut preprocessors: Preprocessors,
) -> Result<()> {
    let mut ws_stream = async_tungstenite::accept_async(raw_stream).await?;

    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-ws".to_string(),
        host: "tremor-ws-client-host.remote".to_string(),
        port: None,
        // TODO add server port here (like for tcp onramp) -- can be done via WsServerState
        path: vec![String::default()],
    };

    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(t)) => {
                let mut ingest_ns = nanotime();
                if let Ok(data) = handle_pp(&mut preprocessors, &mut ingest_ns, t.into_bytes()) {
                    for d in data {
                        let (link_tx, link_rx) = channel(1);

                        loop_tx
                            .send(WsOnrampMessage::Data(
                                ingest_ns,
                                // TODO possible to avoid clone here? we clone again inside send_event
                                origin_uri.clone(),
                                d,
                                link_tx,
                            ))
                            .await;

                        let event = link_rx.recv().await?;
                        let (event_data, _meta) = event.value_meta_iter().next().unwrap();

                        // as json
                        //let mut event_bytes = Vec::new();
                        //event_data.write(&mut event_bytes)?;
                        // as string
                        let event_text = if let Some(s) = event_data.as_str() {
                            //s.as_bytes().to_vec()
                            s.to_string()
                        } else {
                            println!("Data not as str (message text)");
                            //simd_json::to_vec(&event_data)?
                            "Invalid Data (not str)".to_string()
                        };

                        //ws_stream.start_send(msg);
                        ws_stream.send(Message::Text(event_text)).await?;
                    }
                }
            }
            Ok(Message::Binary(b)) => {
                let mut ingest_ns = nanotime();
                if let Ok(data) = handle_pp(&mut preprocessors, &mut ingest_ns, b) {
                    for d in data {
                        let (link_tx, link_rx) = channel(1);

                        loop_tx
                            .send(WsOnrampMessage::Data(
                                ingest_ns,
                                // TODO possible to avoid clone here? we clone again inside send_event
                                origin_uri.clone(),
                                d,
                                link_tx,
                            ))
                            .await;

                        let event = link_rx.recv().await?;
                        let (event_data, _meta) = event.value_meta_iter().next().unwrap();

                        // as json
                        //let mut event_bytes = Vec::new();
                        //event_data.write(&mut event_bytes)?;
                        // as string
                        let event_bytes = if let Some(s) = event_data.as_str() {
                            s.as_bytes().to_vec()
                        } else {
                            println!("Data not as str (message bin)");
                            simd_json::to_vec(&event_data)?
                        };

                        //ws_stream
                        //    .send(Message::Binary("test binary message".into()))
                        //    .await?;
                        ws_stream.send(Message::Binary(event_bytes)).await?;
                    }
                }
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => (),
            Ok(Message::Close(_)) => break,
            Err(e) => error!("WS error returned while waiting for client data: {}", e),
        }
    }
    Ok(())
}

// for select!
#[allow(clippy::mut_mut)]
async fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    config: Config,
    preprocessors: Vec<String>,
    mut codec: Box<dyn Codec>,
    mut metrics_reporter: RampReporter,
) -> Result<()> {
    let (loop_tx, loop_rx) = channel(64);

    let mut link_txes = HashMap::new();

    let addr = format!("{}:{}", config.host, config.port);

    let mut pipelines = Vec::new();
    let mut id = 0;
    let mut no_pp = vec![];

    // Create the event loop and TCP listener we'll accept connections on.
    let listener = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);

    loop {
        loop {
            match handle_pipelines(&rx, &mut pipelines, &mut metrics_reporter).await? {
                PipeHandlerResult::Retry => continue,
                PipeHandlerResult::Terminate => return Ok(()),
                PipeHandlerResult::Normal => break,
                PipeHandlerResult::Response(_event) => {
                    dbg!("FROM PIPELINE (HP)");
                    //dbg!(&event);
                    // TODO might want to continue here?
                    break;
                }
            }
        }

        select! {
            msg = listener.accept().fuse() => if let Ok((stream, _socket)) = msg {
                let preprocessors = make_preprocessors(&preprocessors)?;

                task::spawn(handle_connection(loop_tx.clone(), stream, preprocessors));
            },
            msg = loop_rx.recv().fuse() => if let Ok(WsOnrampMessage::Data(mut ingest_ns, origin_uri, data, link_tx)) = msg {
                id += 1;
                link_txes.insert(id, link_tx);
                send_event(
                    &pipelines,
                    &mut no_pp,
                    &mut  codec,
                    &mut metrics_reporter,
                    &mut ingest_ns,
                    &origin_uri,
                    id,
                    data
                );
            },
            msg = rx.recv().fuse() => if let Ok(msg) = msg {
                match handle_pipelines_msg(msg, &mut pipelines, &mut metrics_reporter)? {
                    PipeHandlerResult::Retry | PipeHandlerResult::Normal => continue,
                    PipeHandlerResult::Terminate => break,
                    PipeHandlerResult::Response(event) => {
                        //dbg!("FROM PIPELINE (HPM)");
                        link_txes.get(&event.id).unwrap().send(event).await;
                        continue;
                    }
                }
            }
        }
    }
    Ok(())
}

impl Onramp for Ws {
    fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let (tx, rx) = channel(1);
        let config = self.config.clone();
        let codec = codec::lookup(&codec)?;
        // we need to change this here since ws is special
        let preprocessors = preprocessors.to_vec();
        task::Builder::new()
            .name(format!("onramp-ws-{}", "???"))
            .spawn(async move {
                if let Err(e) =
                    onramp_loop(&rx, config, preprocessors, codec, metrics_reporter).await
                {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "string"
    }
}
