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
//use futures::{select, FutureExt, StreamExt};
//use async_std::sync::{Arc, Mutex};
use futures::{select, FutureExt};
use halfbrown::HashMap;
use serde_yaml::Value;
use simd_json::prelude::*;
use tide::http::Method;
use tide::{Body, Request, Response};

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    /// host to listen to, defaults to "0.0.0.0"
    #[serde(default = "dflt_host")]
    pub host: String,
    /// port to listen to, defaults to 8000
    #[serde(default = "dflt_port")]
    pub port: u16,
    pub resources: Vec<EndpointConfig>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone, Deserialize)]
pub struct EndpointConfig {
    path: String,
    allow: Vec<ResourceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResourceConfig {
    method: HttpMethod,
    params: Option<Vec<String>>,
    status_code: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub enum HttpMethod {
    GET,
    POST,
    PUT,
    PATCH,
    DELETE,
    // HEAD,
}

fn dflt_host() -> String {
    String::from("0.0.0.0")
}

fn dflt_port() -> u16 {
    8000
}

#[derive(Clone, Debug)]
pub struct Rest {
    pub config: Config,
}

impl onramp::Impl for Rest {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self { config }))
        } else {
            Err("Missing config for REST onramp".into())
        }
    }
}

impl Onramp for Rest {
    fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let (tx, rx) = channel(1);
        let config = self.config.clone();
        let codec = codec::lookup(&codec)?;
        // rest is special
        let preprocessors = preprocessors.to_vec();
        task::Builder::new()
            .name(format!("onramp-rest-{}", "???"))
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
        "json"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TremorRestRequest {
    path: String,
    query_params: String,
    actual_path: String,
    path_params: HashMap<String, String>,
    headers: HashMap<String, String>,
    body: Vec<u8>,
    method: String,
}

type RestOnrampMessage = (EventOriginUri, TremorRestRequest, Sender<Event>);

//#[derive(Clone)]
struct OnrampState {
    tx: Sender<RestOnrampMessage>,
    //link_rx: Receiver<Event>,
    //config: Config,
    //event_id: Arc<Mutex<u64>>,
}

// We got to allow this because of the way that the onramp works
// by creating new instances during runtime.
#[allow(clippy::needless_pass_by_value)]
//#[allow(clippy::mut_mut)]
async fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    config: Config,
    preprocessors: Vec<String>,
    mut codec: Box<dyn codec::Codec>,
    mut metrics_reporter: RampReporter,
) -> Result<()> {
    let (loop_tx, loop_rx) = channel(64);

    //let (link_tx, link_rx) = channel(1);

    let mut link_txes = HashMap::new();

    let addr = format!("{}:{}", config.host, config.port);

    let mut pipelines = Vec::new();
    let mut id = 0;
    //let mut no_pp = vec![];
    // TODO test
    let mut preprocessors = make_preprocessors(&preprocessors)?;

    // start the rest server
    task::Builder::new()
        .name(format!("onramp-rest-server-{}", "???"))
        .spawn(async move {
            let mut server = tide::Server::with_state(OnrampState {
                tx: loop_tx,
                //link_rx,
                //config,
                //event_id: Arc::new(Mutex::new(0)),
            });
            server
                // TODO does not cover /. also support other methods
                .at("/*")
                .get(|mut req: Request<OnrampState>| async move {
                    let state = req.state();

                    // TODO is clone necessary?
                    let tx = state.tx.clone();
                    //let link_rx = state.link_rx.clone();
                    let url = req.url().clone();

                    /*
                    let event_id = state.event_id.clone();

                    let mut current_event_id = event_id.lock().await;
                    *current_event_id += 1;
                    //dbg!(&current_event_id);
                    //state.event_id += 1;
                    //event_id += 1;
                    //dbg!(event_id.into_inner());
                    */

                    let request = TremorRestRequest {
                        path: url.path().to_string(),
                        actual_path: String::default(), // TODO
                        query_params: url.query().unwrap_or("").to_string(),
                        path_params: HashMap::default(), // TODO
                        headers: HashMap::default(),     // TODO
                        body: req.body_bytes().await?,
                        method: req.method().to_string(),
                    };
                    // TODO cache parts of this and update host only on new request
                    let origin_uri = tremor_pipeline::EventOriginUri {
                        scheme: "tremor-rest".to_string(),
                        host: req
                            .host()
                            .unwrap_or("tremor-rest-client-host.remote")
                            .to_string(),
                        port: None,
                        // TODO add server port here (like for tcp onramp) -- can be done via OnrampState
                        path: vec![String::default()],
                    };

                    //dbg!(req.url().to_string());
                    //dbg!(&request);

                    //id2 += 1;
                    //let (link_tx, link_rx) = channel(1);
                    //link_tx_rx.insert(id, (link_tx, link_rx));

                    let (link_tx, link_rx) = channel(1);

                    // TODO check for failure?
                    tx.send((origin_uri, request, link_tx)).await;

                    let event = link_rx.recv().await?;
                    //dbg!("BEFORE RESPONSE");
                    //dbg!(&event.data);

                    // TODO tackle all values
                    //for value in event.value_iter() {
                    //    dbg!(&value.encode());
                    //}
                    let (event_data, meta) = event.value_meta_iter().next().unwrap();
                    //dbg!(&event_data);
                    //dbg!(&meta);

                    let mut event_bytes = Vec::new();
                    event_data.write(&mut event_bytes)?;

                    //let event_data = format!("Hello from {}!", &req.state().config.host);

                    let status = meta
                        .get("status")
                        .and_then(|s| s.as_u16())
                        .unwrap_or_else(|| {
                            println!("Could not find status in the event meta");
                            match req.method() {
                                // TODO enable GET only for linked transport usecase...
                                Method::Get => 200,
                                Method::Post => 201,
                                Method::Delete => 200,
                                _ => 204,
                            }
                        });

                    let mut res = Response::new(status);
                    //res.set_body(Body::from_string("".to_string()));
                    //res.set_body(Body::from_string(event_data.encode()));
                    res.set_body(Body::from_bytes(event_bytes));
                    Ok(res)
                });

            // TODO better messages here
            println!("[Onramp] Listening on: {}", addr);
            if let Err(e) = server.listen(&addr).await {
                error!("[Onramp] Error: {}", e)
            }

            warn!("[Onramp] Rest server stopped");
        })?;

    loop {
        //println!("before handle pipelines");
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
        //println!("after handle pipelines");

        select! {
                msg = loop_rx.recv().fuse() => if let Ok((origin_uri, data, link_tx)) = msg {
                //dbg!(&data);
                let data = json!(data).encode().into_bytes();
                let mut ingest_ns = nanotime();
                id += 1;
                //dbg!(&id);
                //dbg!(&event_id);
                //dbg!(&link_tx);
                link_txes.insert(id, link_tx);

                send_event(
                    &pipelines,
                    //&mut no_pp,
                    &mut preprocessors,
                    &mut codec,
                    &mut metrics_reporter,
                    &mut ingest_ns,
                    &origin_uri,
                    id,
                    data
                );
            },
            msg = rx.recv().fuse() => if let Ok(msg) = msg {
                //println!("before handle pipelines msg");
                match handle_pipelines_msg(msg, &mut pipelines, &mut metrics_reporter)? {
                    PipeHandlerResult::Retry | PipeHandlerResult::Normal => continue,
                    PipeHandlerResult::Terminate => break,
                    PipeHandlerResult::Response(event) => {
                        //dbg!("FROM PIPELINE (HPM)");
                        dbg!(&event.id);
                        //dbg!(&link_txes.get(&event.id));
                        link_txes.get(&event.id).unwrap().send(event).await;
                        //link_tx.send(event).await;
                        continue;
                    }
                }
            }
        }
    }
    Ok(())
}

/*
fn header(headers: &actix_web::http::header::HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(key, value)| {
            Some((key.as_str().to_string(), value.to_str().ok()?.to_string()))
        })
        .collect()
}

fn path_params(patterns: Vec<EndpointConfig>, path: &str) -> (String, HashMap<String, String>) {
    for pattern in patterns {
        let mut path = actix_web::dev::Path::new(path);
        if ResourceDef::new(&pattern.path).match_path(&mut path) {
            return (
                pattern.path,
                path.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
        }
    }
    (String::default(), HashMap::default())
}
*/
