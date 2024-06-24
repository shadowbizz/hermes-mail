use crate::{
    data::{self, CodesVec, DashboardConfig, Receiver, Receivers, Sender, Senders},
    stats::Stats,
    websocket,
};
use chrono::{DateTime, Datelike, Duration, Local, Timelike};
use indicatif::ProgressStyle;
use lettre::transport::smtp::response::Code;
use rand::{seq::SliceRandom, thread_rng};
use serde::Serialize;
use std::{
    cmp::Ordering,
    collections::HashMap,
    env,
    path::PathBuf,
    process,
    sync::Arc,
    thread::{self, JoinHandle},
};
use thiserror::Error;
use tracing::{debug, error, info, info_span, warn, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

pub mod task;

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("for file: '{file}'; err: {err}")]
    CSVError { file: PathBuf, err: csv::Error },
    #[error("queue is missing field: '{0}'")]
    MissingFieldError(String),
    #[error("{0}")]
    DataError(data::Error),
}

pub struct Builder {
    content: Option<PathBuf>,
    daily_limit: u32,
    dashboard_config: Option<DashboardConfig>,
    rate: Duration,
    receivers: Option<PathBuf>,
    save_progress: bool,
    skip_codes: Vec<u16>,
    skip_permanent: bool,
    skip_weekends: bool,
    senders: Option<PathBuf>,
    workers: usize,
    read_receipts: bool,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            content: None,
            daily_limit: 100,
            dashboard_config: None,
            rate: Duration::try_seconds(60).unwrap(),
            read_receipts: false,
            receivers: None,
            save_progress: false,
            senders: None,
            skip_codes: Vec::new(),
            skip_permanent: false,
            skip_weekends: false,
            workers: 2,
        }
    }
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn senders(mut self, file: PathBuf) -> Self {
        self.senders = Some(file);
        self
    }

    pub fn receivers(mut self, file: PathBuf) -> Self {
        self.receivers = Some(file);
        self
    }

    pub fn content(mut self, dir: PathBuf) -> Self {
        self.content = Some(dir);
        self
    }

    pub fn rate(mut self, dur: i64) -> Self {
        self.rate = Duration::try_seconds(dur).unwrap();
        self
    }

    pub fn read_receipts(mut self) -> Self {
        self.read_receipts = true;
        self
    }

    pub fn daily_limit(mut self, rate: u32) -> Self {
        self.daily_limit = rate;
        self
    }

    pub fn workers(mut self, num: usize) -> Self {
        self.workers = num;
        self
    }

    pub fn skip_weekends(mut self) -> Self {
        self.skip_weekends = true;
        self
    }

    pub fn skip_permanent(mut self) -> Self {
        self.skip_weekends = true;
        self
    }

    pub fn save_progress(mut self) -> Self {
        self.save_progress = true;
        self
    }

    pub fn skip_codes(mut self, codes: CodesVec) -> Self {
        self.skip_codes = codes.data;
        self.skip_codes.sort();
        self
    }

    pub fn dashboard_config(mut self, d: DashboardConfig) -> Self {
        self.dashboard_config = Some(d);
        self
    }

    fn read_inputs(
        senders: PathBuf,
        receivers: PathBuf,
    ) -> Result<(Senders, Receivers), BuildError> {
        let senders = data::read_input::<Sender>(&senders)
            .map_err(|err| BuildError::CSVError { file: senders, err })?;
        let mut receivers =
            data::read_input::<Receiver>(&receivers).map_err(|err| BuildError::CSVError {
                file: receivers,
                err,
            })?;

        receivers.shuffle(&mut thread_rng());

        Ok((senders, receivers))
    }

    fn init_senders(
        senders: Senders,
        content: Option<PathBuf>,
    ) -> Result<HashMap<String, Arc<Sender>>, BuildError> {
        senders
            .into_iter()
            .map(|mut s| {
                let email = s.email.clone();
                {
                    let s = Arc::get_mut(&mut s).unwrap();
                    if let Some(content) = content.as_ref() {
                        s.plain = content.join(&s.plain);
                        if let Some(html) = s.html.as_ref() {
                            s.html = Some(content.join(html));
                        }
                    }

                    match s.init_templates() {
                        Ok(_) => {}
                        Err(err) => return Err(BuildError::DataError(err)),
                    }
                }
                Ok((email, s.clone()))
            })
            .collect()
    }

    pub fn build(self) -> Result<Queue, BuildError> {
        if self.senders.is_none() {
            return Err(BuildError::MissingFieldError("sender file".into()));
        } else if self.receivers.is_none() {
            return Err(BuildError::MissingFieldError("builder file".into()));
        }

        let (senders, receivers) =
            Builder::read_inputs(self.senders.unwrap(), self.receivers.unwrap())?;

        let stats: HashMap<String, Stats> = senders
            .iter()
            .map(|s| (s.email.clone(), Stats::new(s.email.clone())))
            .collect();

        let senders = Builder::init_senders(senders, self.content)?;

        let workers = match self.workers.gt(&senders.len()) {
            true => senders.len(),
            false => self.workers,
        };

        let failures = Receivers::with_capacity(receivers.len());
        Ok(Queue {
            daily_limit: self.daily_limit,
            dashboard_config: self.dashboard_config,
            failures,
            rate: self.rate,
            read_receipts: self.read_receipts,
            receivers,
            save_progress: self.save_progress,
            senders,
            skip_weekends: self.skip_weekends,
            skip_permanent: self.skip_permanent,
            skip_codes: self.skip_codes,
            start: Local::now(),
            stats,
            workers,
        })
    }
}

pub struct Queue {
    daily_limit: u32,
    dashboard_config: Option<DashboardConfig>,
    failures: Receivers,
    rate: Duration,
    receivers: Receivers,
    read_receipts: bool,
    save_progress: bool,
    senders: HashMap<String, Arc<Sender>>,
    skip_codes: Vec<u16>,
    skip_permanent: bool,
    skip_weekends: bool,
    start: DateTime<Local>,
    stats: HashMap<String, Stats>,
    workers: usize,
}

impl Queue {
    pub fn builder() -> Builder {
        Builder::default()
    }

    fn reset_daily_lim(&mut self) {
        debug!(msg = "resetting daily limits");
        self.start = Local::now();
        self.stats
            .iter_mut()
            .for_each(|(_, stat)| stat.reset_daily());
    }

    fn save_stats(&self) -> Result<(), csv::Error> {
        let cwd = env::current_dir().unwrap();
        let file = cwd.join("stats.csv");
        debug!(msg = "saving stats", file = format!("{file:?}"));

        let mut writer = csv::Writer::from_path(file)?;
        for (_, stats) in self.stats.iter() {
            writer.serialize(stats)?;
        }

        Ok(())
    }

    fn remove_receiver(&mut self, receiver: &Arc<Receiver>) {
        debug!(msg = "removing receiver", email = receiver.email);
        self.receivers = self
            .receivers
            .iter()
            .filter_map(|r| {
                if r.email != receiver.email {
                    Some(r.clone())
                } else {
                    None
                }
            })
            .collect();
    }

    fn collect_tasks(
        &mut self,
        tasks: Vec<JoinHandle<task::TaskResult>>,
        outbound_tx: &websocket::SocketChannelSender,
    ) -> Result<usize, task::Error> {
        let mut sent = 0;
        for res in tasks {
            debug!(msg = "collecting task results");
            let res = match res.join() {
                Ok(r) => r,
                Err(e) => {
                    error!(msg = "collect err", err = format!("{e:?}"));
                    continue;
                }
            };
            match res {
                Ok(task) => {
                    let stats = self.stats.get_mut(&task.sender.email).unwrap();
                    stats.inc_sent(1);
                    info!(
                        msg = "success",
                        sender = task.sender.email,
                        receiver = task.receiver.email
                    );

                    if let Some(dash) = self.dashboard_config.as_ref() {
                        match serde_json::to_string(&stats) {
                            Ok(stats) => websocket::Message::send_sender_stats(
                                outbound_tx,
                                dash.instance.clone(),
                                dash.user.clone(),
                                stats,
                            ),
                            Err(err) => {
                                error!(msg = "failed to send sender stats", err = format!("{err}"))
                            }
                        };
                    }

                    self.remove_receiver(&task.receiver);
                    sent += 1;
                }

                Err(err) => match err {
                    task::Error::SendError { task, err } => {
                        error!(
                            msg = "failure",
                            error = format!("{err}"),
                            sender = task.sender.email,
                            receiver = task.receiver.email,
                            soft = !err.is_permanent(),
                        );

                        let stats = self.stats.get_mut(&task.sender.email).unwrap();
                        if self.skip_permanent && err.is_permanent() {
                            stats.block();
                            stats.inc_bounced(1);
                            self.remove_receiver(&task.receiver);
                            self.failures.push(task.receiver);

                            if let Some(dash) = self.dashboard_config.as_ref() {
                                websocket::Message::send_block(
                                    outbound_tx,
                                    dash.instance.clone(),
                                    dash.user.clone(),
                                    task.sender.email.clone(),
                                );
                            }
                        } else if let Some(code) = Queue::code_to_int(err.status()) {
                            if self.skip_codes.binary_search(&code).is_ok() {
                                stats.block();
                                stats.inc_bounced(1);
                                self.remove_receiver(&task.receiver);
                                self.failures.push(task.receiver);

                                if let Some(dash) = self.dashboard_config.as_ref() {
                                    websocket::Message::send_block(
                                        outbound_tx,
                                        dash.instance.clone(),
                                        dash.user.clone(),
                                        task.sender.email.clone(),
                                    )
                                }
                            }
                        }

                        if let Some(dash) = self.dashboard_config.as_ref() {
                            let stats = self.stats.get_mut(&task.sender.email).unwrap();
                            match serde_json::to_string(&stats) {
                                Ok(stats) => websocket::Message::send_sender_stats(
                                    outbound_tx,
                                    dash.instance.clone(),
                                    dash.user.clone(),
                                    stats,
                                ),
                                Err(err) => error!(
                                    msg = "failed to send sender stats",
                                    err = format!("{err}")
                                ),
                            };
                        }
                    }
                    _ => return Err(err),
                },
            }
        }

        Ok(sent)
    }

    fn pos_min_timeout(&mut self, stack_size: usize) -> Option<usize> {
        if stack_size >= self.stats.len() {
            return None;
        }

        let sender = match self.stats.iter().min_by(|x, y| {
            let (x, y) = (x.1, y.1);
            if x.timeout.is_none() {
                return Ordering::Less;
            } else if y.timeout.is_none() {
                return Ordering::Greater;
            }
            let (x, y) = (x.timeout.unwrap(), y.timeout.unwrap());
            x.cmp(&y)
        }) {
            Some((email, _)) => email,
            None => return None,
        };

        let sender = sender.to_owned();
        match self.receivers.iter().position(|r| r.sender.eq(&sender)) {
            Some(p) => Some(p),
            None => {
                self.stats.remove(&sender);
                self.pos_min_timeout(stack_size + 1)
            }
        }
    }

    fn new_progress_span(&self) -> tracing::Span {
        let span = info_span!("queue");

        span.pb_set_style(
            &ProgressStyle::with_template(&format!(
                " {} {}{{bar:30.bold}}{} {}",
                console::style("Sending:").bold().dim().cyan(),
                console::style("[").bold(),
                console::style("]").bold(),
                console::style("[{pos}/{len}]").bold().dim().green(),
            ))
            .unwrap()
            .progress_chars("=> "),
        );
        span.pb_set_length(self.receivers.len() as u64);
        span
    }

    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        let (inbound_tx, inbound_rx) = crossbeam_channel::unbounded();
        let (outbound_tx, outbound_rx) = futures_channel::mpsc::unbounded();

        if let Some(dash) = self.dashboard_config.as_mut() {
            let ws_url = dash.host.replace("http", "ws");
            let ib_tx = inbound_tx.clone();
            let instance = dash.instance.clone();
            tokio::spawn(async move {
                websocket::connect_and_listen(
                    format!("{}/ws/instances/{}", ws_url, instance),
                    ib_tx,
                    outbound_rx,
                )
                .await
            });

            if let Some(imap_user) = dash.unblocker_user.clone() {
                let senders = self.senders.keys().map(|email| email.to_owned()).collect();

                let i_tx = inbound_tx.clone();
                thread::spawn(move || imap_user.query_block_status(senders, i_tx));
            }
        }

        self.start = Local::now();
        let (mut ptr, mut sent, mut skips) = (0, 0, 0);
        info!(msg = "starting queue", start = format!("{}", self.start));

        let progress = self.new_progress_span();
        let progress_enter = progress.enter();
        'main: loop {
            if self.skip_weekends {
                Queue::skip_weekend();
            }

            let mut tasks: Vec<JoinHandle<task::TaskResult>> = Vec::new();
            for _ in 0..self.workers {
                if self.receivers.is_empty() {
                    info!(msg = "sent all emails", total_sent = sent);
                    break 'main;
                }

                if Queue::is_tomorrow(self.start) {
                    debug!(
                        msg = "updated start time",
                        time = format!("{}", Local::now())
                    );
                    self.reset_daily_lim();
                }

                let receiver = self.receivers[ptr % self.receivers.len()].clone();
                let stat = match self.stats.get_mut(&receiver.sender) {
                    Some(stat) => stat,
                    None => {
                        warn!(
                            msg = "non-existent sender",
                            sender = receiver.sender,
                            receiver = receiver.email
                        );
                        self.senders.remove(&receiver.sender);
                        self.remove_receiver(&receiver);
                        self.failures.push(receiver);
                        ptr += 1;
                        continue;
                    }
                };

                if stat.is_blocked() {
                    debug!(
                        msg = "skipping flagged sender",
                        sender = receiver.sender,
                        receiver = receiver.email,
                    );

                    ptr += 1;
                    continue;
                }

                if let Some(timeout) = stat.is_timed_out() {
                    if skips < self.receivers.len() {
                        skips += 1;
                        continue;
                    }
                    skips = 0;
                    let pos = self.pos_min_timeout(0);
                    if let Some(pos) = pos {
                        ptr = pos;
                        let sender = &self.receivers[ptr].sender;
                        let stat = self.stats.get_mut(&self.receivers[ptr].sender).unwrap();
                        debug!(msg = "got sender with least timeout", sender = sender);
                        if let Some(t) = stat.timeout {
                            Queue::pause(t);
                        }
                        continue 'main;
                    }
                    Queue::pause(timeout);
                    continue 'main;
                }

                if !Queue::is_tomorrow(self.start) && stat.today > self.daily_limit {
                    warn!(
                        msg = "sender hit daily limit; skipping",
                        sender = receiver.sender,
                        receiver = receiver.email
                    );
                    stat.set_timeout(Duration::try_hours(24).unwrap());
                    ptr += 1;
                    continue 'main;
                }

                let sender = self.senders.get(&receiver.sender).unwrap();
                let task = task::Task::new(sender.clone(), receiver);

                tasks.push(task.spawn(self.read_receipts));

                stat.set_timeout(self.rate);
                ptr += 1;
            }

            let _sent = self.collect_tasks(tasks, &outbound_tx).unwrap_or(0);

            Span::current().pb_inc(_sent as u64);
            sent += _sent;

            self.send_task_stats(sent, &outbound_tx);

            self.read_messages(&inbound_rx, &outbound_tx);
            if self.save_progress {
                self.save_progress();
            }
        }

        std::mem::drop(progress_enter);
        std::mem::drop(progress);

        Ok(())
    }

    fn send_task_stats(&self, sent: usize, outbound_tx: &websocket::SocketChannelSender) {
        if let Some(dash) = self.dashboard_config.as_ref() {
            match serde_json::to_string(&sent) {
                Ok(sent) => {
                    websocket::Message::send_task_stats(
                        outbound_tx,
                        dash.instance.clone(),
                        dash.user.clone(),
                        sent,
                    );
                }
                Err(err) => error!(msg = "failed to send task stats", err = format!("{err}")),
            }
        }
    }

    fn read_messages(
        &mut self,
        inbound_rx: &crossbeam_channel::Receiver<websocket::Message>,
        outbound_tx: &websocket::SocketChannelSender,
    ) {
        debug!(msg = "reading inbound messages");
        for _ in 0..inbound_rx.len() {
            let message = match inbound_rx.recv() {
                Ok(msg) => msg,
                Err(err) => {
                    error!(msg = "message read err", err = format!("{err}"));
                    continue;
                }
            };

            match message.kind {
                websocket::MessageKind::Block => {
                    if let Some(sender) = self.stats.get_mut(&message.data) {
                        sender.block();
                    }
                }
                websocket::MessageKind::Unblock => {
                    if let Some(sender) = self.stats.get_mut(&message.data) {
                        sender.unblock();
                    }
                }
                websocket::MessageKind::Stop => {
                    self.save_progress();
                    warn!("received stop signal; stopping process.");
                    process::exit(-1);
                }
                websocket::MessageKind::LocalBlock => {
                    let data: websocket::LocalBlockBody = match serde_json::from_str(&message.data)
                    {
                        Ok(d) => d,
                        Err(e) => {
                            error!(msg = "local block serde err", err = format!("{e}"));
                            continue;
                        }
                    };

                    if let Some(stat) = self.stats.get_mut(&data.email) {
                        stat.inc_bounced(data.amnt as u64);
                        stat.block();
                    }

                    if let Some(dash) = self.dashboard_config.as_ref() {
                        websocket::Message::send_block(
                            outbound_tx,
                            dash.instance.clone(),
                            dash.user.clone(),
                            data.email,
                        );
                    }
                }
                _ => continue,
            }
        }
    }

    fn save_progress(&self) {
        self.save_stats()
            .unwrap_or_else(|e| warn!(msg = "could not save statistics", error = format!("{e}")));

        Self::save_receivers(&self.failures, "failures.csv")
            .unwrap_or_else(|e| warn!(msg = "could not save statistics", error = format!("{e}")));

        Self::save_receivers(&self.receivers, "remaining.csv")
            .unwrap_or_else(|e| warn!(msg = "could not save statistics", error = format!("{e}")));
    }

    fn code_to_int(code: Option<Code>) -> Option<u16> {
        match code {
            None => None,
            Some(code) => {
                let (s, b, d) = (code.severity, code.category, code.detail);
                let code = (s as u16) * 100 + (b as u16) * 10 + (d as u16);
                Some(code)
            }
        }
    }

    fn is_tomorrow(start: DateTime<Local>) -> bool {
        Local::now() > (start + Duration::try_hours(24).unwrap())
    }

    fn calculate_time_until(days: i64) -> Duration {
        let now = Local::now();
        let mut dur = Duration::try_days(days).unwrap();
        dur -= Duration::try_hours(now.hour() as i64).unwrap();
        dur -= Duration::try_minutes(now.minute() as i64).unwrap();
        dur -= Duration::try_seconds(now.second() as i64).unwrap();

        dur
    }

    fn skip_weekend() {
        match Local::now().weekday() {
            chrono::Weekday::Sat => {
                let dur = Queue::calculate_time_until(2);
                warn!(msg = "sleeping for the weekend", dur = format!("{dur}"));
                thread::sleep(dur.to_std().unwrap());
            }
            chrono::Weekday::Sun => {
                let dur = Queue::calculate_time_until(1);
                warn!(msg = "sleeping for the weekend", dur = format!("{dur}"));
                thread::sleep(dur.to_std().unwrap());
            }
            _ => {}
        };
    }

    fn pause(timeout: DateTime<Local>) {
        let (now, timeout) = (Local::now(), timeout);
        if now.lt(&timeout) {
            let diff = timeout - now;
            warn!(msg = "pausing", duration = format!("{diff}"));
            thread::sleep(diff.to_std().unwrap())
        }
    }

    fn save_receivers<S>(records: &[S], filename: &str) -> Result<(), csv::Error>
    where
        S: Serialize,
    {
        let cwd = env::current_dir().unwrap();
        let file = cwd.join(filename);
        debug!(msg = "saving receivers", file = format!("{file:?}"));

        let mut writer = csv::Writer::from_path(file)?;
        for record in records {
            writer.serialize(record)?;
        }

        Ok(())
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        self.save_progress()
    }
}
