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
use crate::errors::{Error, Result};
use crate::registry::ServantId;
use crate::repository::PipelineArtefact;
use crate::url::TremorURL;
//use crate::offramp;
use crate::{offramp, onramp};
//use async_std::stream::Stream;
use async_std::sync::{self, channel};
use async_std::task::{self, JoinHandle};
//use crossbeam_channel::{bounded, Sender as CbSender};
//use crossbeam_channel::Sender as CbSender;
use std::borrow::Cow;
use std::fmt;
//use std::thread;
use futures::stream::StreamExt;
use tremor_pipeline::Event;

pub(crate) type Sender = sync::Sender<ManagerMsg>;

/// Address for a a pipeline
#[derive(Clone)]
pub struct Addr {
    //pub(crate) addr: CbSender<Msg>,
    pub(crate) addr: sync::Sender<Msg>,
    pub(crate) id: ServantId,
}

impl fmt::Debug for Addr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pipeline({})", self.id)
    }
}

#[derive(Debug)]
pub(crate) enum Msg {
    Event {
        event: Event,
        input: Cow<'static, str>,
    },
    ConnectOnramp(Cow<'static, str>, TremorURL, onramp::Addr),
    ConnectOfframp(Cow<'static, str>, TremorURL, offramp::Addr),
    ConnectPipeline(Cow<'static, str>, TremorURL, Addr),
    Disconnect(Cow<'static, str>, TremorURL),
    #[allow(dead_code)]
    Signal(Event),
    Insight(Event),
    Response(Event),
}

#[derive(Debug)]
pub enum Dest {
    Offramp(offramp::Addr),
    Pipeline(Addr),
    Onramp(onramp::Addr), // for linked transports
}
impl Dest {
    pub async fn send_event(&self, input: Cow<'static, str>, event: Event) -> Result<()> {
        match self {
            Self::Offramp(addr) => addr.send(offramp::Msg::Event { input, event })?,
            //Self::Pipeline(addr) => addr.addr.send(Msg::Event { input, event })?,
            Self::Pipeline(addr) => addr.addr.send(Msg::Event { input, event }).await,
            // TODO only send event?
            Self::Onramp(addr) => addr.send(onramp::Msg::Event { input, event }).await,
        }
        Ok(())
    }
}

pub struct Create {
    pub config: PipelineArtefact,
    pub id: ServantId,
}

pub(crate) enum ManagerMsg {
    Stop,
    Create(async_std::sync::Sender<Result<Addr>>, Create),
}

#[derive(Default, Debug)]
pub(crate) struct Manager {
    qsize: usize,
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self { qsize }
    }
    pub fn start(self) -> (JoinHandle<bool>, Sender) {
        let (tx, rx) = channel(64);
        let h = task::spawn(async move {
            info!("Pipeline manager started");
            loop {
                match rx.recv().await {
                    Ok(ManagerMsg::Stop) => {
                        info!("Stopping pipelines...");
                        break;
                    }
                    Ok(ManagerMsg::Create(r, create)) => r.send(self.start_pipeline(create)).await,
                    Err(e) => {
                        info!("Stopping pipelines... {}", e);
                        break;
                    }
                }
            }
            info!("Pipeline manager stopped");
            true
        });
        (h, tx)
    }

    #[allow(clippy::too_many_lines)]
    fn start_pipeline(&self, req: Create) -> Result<Addr> {
        #[inline]
        async fn send_events(
            eventset: &mut Vec<(Cow<'static, str>, Event)>,
            dests: &halfbrown::HashMap<Cow<'static, str>, Vec<(TremorURL, Dest)>>,
        ) -> Result<()> {
            for (output, event) in eventset.drain(..) {
                if let Some(dest) = dests.get(&output) {
                    let len = dest.len();
                    //We know we have len, so grabbing len - 1 elementsis safe
                    for (id, offramp) in unsafe { dest.get_unchecked(..len - 1) } {
                        offramp
                            .send_event(
                                id.instance_port()
                                    .ok_or_else(|| {
                                        Error::from(format!("missing instance port in {}.", id))
                                    })?
                                    .clone()
                                    .into(),
                                event.clone(),
                            )
                            .await?;
                    }
                    //We know we have len, so grabbing the last elementsis safe
                    let (id, offramp) = unsafe { dest.get_unchecked(len - 1) };
                    offramp
                        .send_event(
                            id.instance_port()
                                .ok_or_else(|| {
                                    Error::from(format!("missing instance port in {}.", id))
                                })?
                                .clone()
                                .into(),
                            event,
                        )
                        .await?;
                };
            }
            Ok(())
        }
        let config = req.config;
        let id = req.id.clone();
        let mut dests: halfbrown::HashMap<Cow<'static, str>, Vec<(TremorURL, Dest)>> =
            halfbrown::HashMap::new();
        let mut eventset: Vec<(Cow<'static, str>, Event)> = Vec::new();
        let (tx, mut rx) = channel::<Msg>(self.qsize);
        let mut pipeline = config.to_executable_graph(tremor_pipeline::buildin_ops)?;
        let mut pid = req.id.clone();
        pid.trim_to_instance();
        pipeline.id = pid.to_string();
        //dbg!("BEFORE pipline");
        task::Builder::new()
            .name(format!("pipeline-{}", id.clone()))
            .spawn(async move {
                info!("[Pipeline:{}] starting task.", id);
                //dbg!("Processing start");
                //for req in rx() {
                while let Some(req) = rx.next().await {
                    //dbg!("Processing");
                    match req {
                        Msg::Event { input, event } => {
                            //dbg!(&input);
                            match pipeline.enqueue(&input, event, &mut eventset) {
                                Ok(()) => {
                                    if let Err(e) = send_events(&mut eventset, &dests).await {
                                        error!("Failed to send event: {}", e)
                                    }
                                }
                                Err(e) => error!("error: {:?}", e),
                            }
                        }
                        Msg::Response(response) => {
                            //dbg!(&response);
                            // TODO don't hardcode input name here
                            match pipeline.enqueue("from-offramp", response, &mut eventset) {
                                Ok(()) => {
                                    if let Err(e) = send_events(&mut eventset, &dests).await {
                                        error!("Failed to send response event: {}", e)
                                    }
                                }
                                Err(e) => error!("error: {:?}", e),
                            }
                        }
                        Msg::Insight(insight) => {
                            pipeline.contraflow(insight);
                        }
                        Msg::Signal(signal) => match pipeline.enqueue_signal(signal, &mut eventset)
                        {
                            Ok(()) => {
                                if let Err(e) = send_events(&mut eventset, &dests).await {
                                    error!("Failed to send event: {}", e)
                                }
                            }
                            Err(e) => error!("error: {:?}", e),
                        },

                        Msg::ConnectOfframp(output, offramp_id, offramp) => {
                            info!(
                                "[Pipeline:{}] connecting {} to offramp {}",
                                id, output, offramp_id
                            );
                            if let Some(offramps) = dests.get_mut(&output) {
                                offramps.push((offramp_id, Dest::Offramp(offramp)));
                            } else {
                                dests.insert(output, vec![(offramp_id, Dest::Offramp(offramp))]);
                            }
                        }
                        Msg::ConnectPipeline(output, pipeline_id, pipeline) => {
                            info!(
                                "[Pipeline:{}] connecting {} to pipeline {}",
                                id, output, pipeline_id
                            );
                            if let Some(offramps) = dests.get_mut(&output) {
                                offramps.push((pipeline_id, Dest::Pipeline(pipeline)));
                            } else {
                                dests.insert(output, vec![(pipeline_id, Dest::Pipeline(pipeline))]);
                            }
                        }
                        // TODO can we make this work without this?
                        Msg::ConnectOnramp(output, onramp_id, onramp) => {
                            info!(
                                "[Pipeline:{}] connecting {} to onramp {}",
                                id, output, onramp_id
                            );
                            if let Some(onramps) = dests.get_mut(&output) {
                                onramps.push((onramp_id, Dest::Onramp(onramp)));
                            } else {
                                dests.insert(output, vec![(onramp_id, Dest::Onramp(onramp))]);
                            }
                        }
                        Msg::Disconnect(output, to_delete) => {
                            let mut remove = false;
                            if let Some(offramp_vec) = dests.get_mut(&output) {
                                offramp_vec.retain(|(this_id, _)| this_id != &to_delete);
                                remove = offramp_vec.is_empty();
                            }
                            if remove {
                                dests.remove(&output);
                            }
                        }
                    };
                }
                dbg!("Processing final");
                info!("[Pipeline:{}] stopping task.", id);
            })?;
        //dbg!("AFTER pipline");
        Ok(Addr {
            id: req.id,
            addr: tx,
        })
    }
}
