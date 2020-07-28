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
use crossbeam_channel::bounded;
use halfbrown::HashMap;
use simd_json::borrowed::Object;
use simd_json::json;
use std::str;
use std::time::Instant;
use tremor_script::prelude::*;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// list of endpoint urls
    pub endpoints: Vec<String>,
    /// maximum number of paralel in flight batches (default: 4)
    #[serde(default = "dflt::d_4")]
    pub concurrency: usize,
    /// If put should be used instead of post.
    #[serde(default = "dflt::d")]
    pub put: bool,
    #[serde(default = "dflt::d")]
    pub headers: HashMap<String, String>,
}

impl ConfigImpl for Config {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TremorRestResponse {
    status: u16,
    // TODO implement once https://github.com/http-rs/surf/pull/191/files is merged
    //headers: HashMap<String, String>,
    mime: Option<String>,
    // TODO would this be useful?
    //http_version: String,
    //body: Vec<u8>,
    body: String,
}

pub struct Rest {
    client_idx: usize,
    config: Config,
    // TODO just u64 here
    //queue: AsyncSink<(u64, Option<Vec<u8>>)>,
    queue: AsyncSink<u64>,
    pipelines: HashMap<TremorURL, pipeline::Addr>,
    postprocessors: Postprocessors,
}

impl offramp::Impl for Rest {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;

            let queue = AsyncSink::new(config.concurrency);
            Ok(Box::new(Self {
                client_idx: 0,
                pipelines: HashMap::new(),
                postprocessors: vec![],
                config,
                queue,
            }))
        } else {
            Err("Rest offramp requires a configuration.".into())
        }
    }
}

impl Rest {
    async fn flush(
        endpoint: &str,
        config: Config,
        payload: Vec<u8>,
    ) -> Result<(u64, TremorRestResponse)> {
        let start = Instant::now();
        let c = if config.put {
            surf::put(endpoint)
        } else {
            //surf::post(endpoint)
            surf::get(endpoint)
        };
        dbg!(payload.len());
        // TODO resolve issues with get here
        //c = c.body_bytes(&payload);
        //for (k, v) in config.headers {
        //    use http_types::headers::HeaderName;
        //    match HeaderName::from_bytes(k.as_str().as_bytes().to_vec()) {
        //        Ok(h) => {
        //            c = c.set_header(h, v.as_str());
        //        }
        //        Err(e) => error!("Bad header name: {}", e),
        //    }
        //}

        let mut reply = c.await?;
        let status = reply.status();
        let mime = reply.mime().map(|m| m.to_string());

        let body = if status.is_client_error() || status.is_server_error() {
            if let Ok(body) = reply.body_string().await {
                error!("HTTP request failed: {} => {}", status, body)
            } else {
                error!("HTTP request failed: {}", status)
            }
            //vec![]
            "".to_string()
        } else {
            // TODO switch to bytes
            //let r = reply.body_bytes().await?;
            let r = reply.body_string().await?;
            r
        };

        let d = duration_to_millis(start.elapsed());
        Ok((
            d,
            TremorRestResponse {
                body,
                status: status.into(),
                mime,
            },
        ))
    }

    fn enqueue_send_future(
        &mut self,
        event_id: u64,
        payload: Vec<u8>,
        endpoint: Option<String>,
    ) -> Result<()> {
        let destination = endpoint.unwrap_or({
            self.client_idx = (self.client_idx + 1) % self.config.endpoints.len();
            self.config.endpoints[self.client_idx].clone()
        });
        dbg!(&destination);
        let (tx, rx) = bounded(1);
        let config = self.config.clone();
        let pipelines: Vec<(TremorURL, pipeline::Addr)> = self
            .pipelines
            .iter()
            .map(|(i, p)| (i.clone(), p.clone()))
            .collect();
        task::spawn(async move {
            let result = Self::flush(&destination, config, payload).await;
            let mut m = Object::new();
            if let Ok((t, r)) = result {
                m.insert("time".into(), t.into());

                // handling linked transport case
                //dbg!(&r);
                let data = json!(r).encode().into_bytes();
                // TODO apply proper codec based on mime
                //let response_data = LineValue::new(vec![data], |_| Value::null().into());
                //let response = LineValue::try_new(vec![data], |data| {
                //    Value::from(std::str::from_utf8(data[0].as_slice())?).into()
                //})?;
                let response_data = LineValue::try_new(vec![data], |data| {
                    simd_json::to_borrowed_value(&mut data[0]).map(ValueAndMeta::from)
                });
                if let Ok(d) = response_data {
                    let response = Event {
                        is_batch: false,
                        //id: 0, // TODO better id?
                        id: event_id,
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
                } else {
                    //error!("REST offramp error: {:?}", response_data);
                    error!("REST offramp error: could not convert response to line value");
                }
            } else {
                error!("REST offramp error: {:?}", result);
                m.insert("error".into(), "Failed to send".into());
            };
            let insight = Event {
                is_batch: false,
                id: 0,
                data: (Value::null(), m).into(),
                ingest_ns: nanotime(),
                origin_uri: None,
                kind: None,
            };

            for (pid, p) in &pipelines {
                //p.addr.send(pipeline::Msg::Insight(insight.clone()));
                // TODO adopt try_send everywhere?
                if p.addr
                    .try_send(pipeline::Msg::Insight(insight.clone()))
                    .is_err()
                {
                    error!("Failed to send contraflow to pipeline {}", pid)
                };
            }

            // TODO send result here
            //if let Err(e) = tx.send(result) {
            if let Err(e) = tx.send(Ok(0)) {
                error!("Failed to send reply: {}", e)
            }
        });
        self.queue.enqueue(rx)?;
        Ok(())
    }
    fn maybe_enque(
        &mut self,
        event_id: u64,
        payload: Vec<u8>,
        endpoint: Option<String>,
    ) -> Result<()> {
        match self.queue.dequeue() {
            Err(SinkDequeueError::NotReady) if !self.queue.has_capacity() => {
                let mut m = Object::new();
                m.insert(
                    "error".into(),
                    "Dropped data due to REST endpoint overload".into(),
                );

                let insight = Event {
                    is_batch: false,
                    id: 0,
                    data: (Value::null(), m).into(),
                    ingest_ns: nanotime(),
                    origin_uri: None,
                    kind: None,
                };

                let pipelines: Vec<(TremorURL, pipeline::Addr)> = self
                    .pipelines
                    .iter()
                    .map(|(i, p)| (i.clone(), p.clone()))
                    .collect();
                for (pid, p) in pipelines {
                    if p.addr
                        .try_send(pipeline::Msg::Insight(insight.clone()))
                        .is_err()
                    {
                        error!("Failed to send contraflow to pipeline {}", pid)
                    };
                }
                error!("Dropped data due to overload");
                Err("Dropped data due to overload".into())
            }
            _ => {
                if self
                    .enqueue_send_future(event_id, payload, endpoint)
                    .is_err()
                {
                    // TODO: handle reply to the pipeline
                    error!("Failed to enqueue send request");
                    Err("Failed to enqueue send request".into())
                } else {
                    Ok(())
                }
            }
        }
    }
}

impl Offramp for Rest {
    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) -> Result<()> {
        // TODO this should be configurable?
        let mut payload = Vec::with_capacity(4096);
        let mut endpoint = None;
        //dbg!(&event.id);
        for (value, meta) in event.value_meta_iter() {
            // TODO better way to handle this?
            endpoint = Some(
                meta.get("endpoint")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::from("'endpoint' not set for rest offramp!"))?
                    .to_string(),
            );
            let mut raw = codec.encode(value)?;
            payload.append(&mut raw);
            payload.push(b'\n');
        }
        self.maybe_enque(event.id, payload, endpoint)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    fn start(&mut self, _codec: &Box<dyn Codec>, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
    fn add_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr) {
        self.pipelines.insert(id, addr);
    }
    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }
}
