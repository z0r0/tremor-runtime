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

use crate::offramp::prelude::*;
use async_std::sync::{channel, Receiver, Sender};
use async_tungstenite::async_std::connect_async;
use futures::{SinkExt, StreamExt};
use halfbrown::HashMap;
use std::time::Duration;
use tremor_script::prelude::*;
use tungstenite::protocol::Message;
use url::Url;

type WsAddr = Sender<(WsMessage, Option<String>)>;

enum WsMessage {
    Binary(Vec<u8>),
    Text(String),
}

#[derive(Deserialize, Debug)]
pub struct Config {
    /// Host to use as source
    pub url: String,
    #[serde(default)]
    pub binary: bool,
}

/// An offramp that writes to a websocket endpoint
pub struct Ws {
    addr: Option<WsAddr>,
    config: Config,
    pipelines: HashMap<TremorURL, pipeline::Addr>,
    postprocessors: Postprocessors,
    rx: Receiver<Option<WsAddr>>,
    response_rx: Receiver<Vec<u8>>,
}

async fn ws_loop(url: String, offramp_tx: Sender<Option<WsAddr>>, response_tx: Sender<Vec<u8>>) {
    //let mut connection_pool = HashMap::new();

    loop {
        //let mut ws_stream = if let Ok((ws_stream, _)) = connect_async(&url).await {
        //    ws_stream
        //} else {
        //    error!("Failed to connect to {}, retrying in 1s", url);
        //    offramp_tx.send(None).await;
        //    task::sleep(Duration::from_secs(1)).await;
        //    continue;
        //};
        let (tx, rx) = channel(64);
        offramp_tx.send(Some(tx)).await;

        while let Ok((msg, override_url)) = rx.recv().await {
            let destination = override_url.unwrap_or(url.clone());

            if let Ok((mut ws_stream, _)) = connect_async(&destination).await {
                let r = match msg {
                    WsMessage::Text(t) => {
                        dbg!(&t);
                        ws_stream.send(Message::Text(t)).await
                    }
                    WsMessage::Binary(t) => {
                        println!("SENDING BINARY");
                        dbg!(&t);
                        ws_stream.send(Message::Binary(t)).await
                    }
                };
                if let Err(e) = r {
                    error!(
                        "Websocket send error: {} for endppoint {}, reconnecting",
                        e, url
                    );
                    break;
                }
                dbg!(&r);

                dbg!("START RECEIVING RESPONSE");

                // TODO do this only for LP
                // also duplicate of ws onramp logic: consolidate
                if let Some(msg) = ws_stream.next().await {
                    match msg {
                        Ok(Message::Text(t)) => {
                            dbg!(&t);
                            response_tx.send(t.into_bytes()).await;
                        }
                        Ok(Message::Binary(t)) => {
                            println!("GOT BINARY");
                            dbg!(&t);
                            response_tx.send(t).await;
                        }
                        Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {
                            println!("GOT PING");
                        }
                        Ok(Message::Close(_)) => {
                            println!("GOT CLOSE");
                            break;
                        }
                        Err(e) => error!("WS error returned while waiting for client data: {}", e),
                    }
                }

                dbg!("DONE RECEIVING RESPONSE");
            } else {
                error!("Failed to connect to {}, retrying in 1s", url);
                offramp_tx.send(None).await;
                task::sleep(Duration::from_secs(1)).await;
                // TODO better choice here
                continue;
            }
        }
    }
}

impl offramp::Impl for Ws {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            // Ensure we have valid url
            Url::parse(&config.url)?;
            let (tx, rx) = channel(1);
            let (response_tx, response_rx) = channel(1);

            task::spawn(ws_loop(config.url.clone(), tx, response_tx));

            Ok(Box::new(Self {
                addr: None,
                config,
                pipelines: HashMap::new(),
                postprocessors: vec![],
                rx,
                response_rx,
            }))
        } else {
            Err("[WS Offramp] Offramp requires a config".into())
        }
    }
}

impl Offramp for Ws {
    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) -> Result<()> {
        dbg!("on event");
        let pipelines: Vec<(TremorURL, pipeline::Addr)> = self
            .pipelines
            .iter()
            .map(|(i, p)| (i.clone(), p.clone()))
            .collect();

        task::block_on(async {
            while !self.rx.is_empty() {
                self.addr = self.rx.recv().await.unwrap_or_default();
            }

            if let Some(addr) = &self.addr {
                for (value, meta) in event.value_meta_iter() {
                    // TODO better way to handle this? only do this for non-batched events
                    let url = Some(
                        meta.get("url")
                            .and_then(Value::as_str)
                            .ok_or_else(|| Error::from("'url' not set for ws offramp!"))?
                            .to_string(),
                    );
                    let raw = codec.encode(value)?;
                    let datas = postprocess(&mut self.postprocessors, event.ingest_ns, raw)?;
                    for raw in datas {
                        if self.config.binary {
                            addr.send((WsMessage::Binary(raw), url.clone())).await;
                        } else if let Ok(txt) = String::from_utf8(raw) {
                            addr.send((WsMessage::Text(txt), url.clone())).await;
                        } else {
                            error!("[WS Offramp] Invalid utf8 data for text message");
                            return Err(Error::from("Invalid utf8 data for text message"));
                        }
                    }
                }
            } else {
                return Err(Error::from("not connected"));
            };

            // TODO do this only for LP
            if let Ok(data) = self.response_rx.recv().await {
                dbg!(&data);
                let event_meta = Value::object();
                //event_meta.insert("status", r.status).unwrap();
                let response_data = LineValue::try_new(vec![data], |data| {
                    std::str::from_utf8(data[0].as_slice())
                        .map(|v| ValueAndMeta::from_parts(Value::from(v), event_meta))
                });
                if let Ok(d) = response_data {
                    let response = Event {
                        is_batch: false,
                        //id: 0, // TODO better id?
                        id: event.id,
                        //data: (Value::null(), m).into(),
                        data: d,
                        ingest_ns: nanotime(),
                        origin_uri: None, // TODO set based on response
                        kind: None,
                    };
                    //dbg!(&response);
                    // TODO send only to the linked pipeline
                    for (pid, p) in &pipelines {
                        // TODO adopt try_send everywhere?
                        // TODO avoid clone here
                        if p.addr
                            .try_send(pipeline::Msg::Response(response.clone()))
                            .is_err()
                        {
                            error!("Failed to send response to pipeline {}", pid)
                        };
                    }
                }
            }

            Ok(())
        })
    }

    fn add_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr) {
        self.pipelines.insert(id, addr);
    }
    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }
    fn default_codec(&self) -> &str {
        // TODO align with ws onramp?
        "json"
    }
    fn start(&mut self, _codec: &Box<dyn Codec>, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
}
