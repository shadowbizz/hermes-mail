use crate::{
    data::{self, Receiver, Sender},
    stats::Stats,
};
use chrono::{DateTime, Datelike, Duration, Local, Timelike};
use lettre::transport::smtp::response::Code;
use rand::{seq::SliceRandom, thread_rng};
use serde::Serialize;
use std::{
    cmp::Ordering,
    collections::HashMap,
    env,
    num::ParseIntError,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    thread::{self, JoinHandle},
};
use thiserror::Error;
use tracing::{debug, error, info, warn};

pub mod task;

type Senders = Vec<Arc<Sender>>;
type Receivers = Vec<Arc<Receiver>>;

#[derive(Debug, Clone)]
pub struct CodesVec {
    data: Vec<u16>,
}

impl Default for CodesVec {
    fn default() -> Self {
        Self { data: Vec::new() }
    }
}

impl FromStr for CodesVec {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Ok(CodesVec::default());
        }
        let data = s
            .split(',')
            .map(|s| s.parse::<u16>())
            .collect::<Result<Vec<u16>, ParseIntError>>()?;

        Ok(CodesVec { data })
    }
}

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
    senders: Option<PathBuf>,
    receivers: Option<PathBuf>,
    workers: u8,
    rate: Duration,
    daily_limit: u32,
    skip_weekends: bool,
    skip_permanent: bool,
    skip_codes: Vec<u16>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            senders: None,
            receivers: None,
            workers: 2,
            rate: Duration::try_seconds(60).unwrap(),
            daily_limit: 100,
            skip_weekends: false,
            skip_permanent: false,
            skip_codes: Vec::new(),
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

    pub fn rate(mut self, dur: i64) -> Self {
        self.rate = Duration::try_seconds(dur).unwrap();
        self
    }

    pub fn daily_rate(mut self, rate: u32) -> Self {
        self.daily_limit = rate;
        self
    }

    pub fn workers(mut self, num: u8) -> Self {
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

    pub fn skip_codes(mut self, codes: CodesVec) -> Self {
        self.skip_codes = codes.data;
        self.skip_codes.sort();
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

    fn init_senders(mut senders: Senders) -> Result<HashMap<String, Arc<Sender>>, BuildError> {
        senders
            .iter_mut()
            .map(|s| {
                let email = s.email.clone();
                {
                    let s = Arc::get_mut(s).unwrap();
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

        let senders = Builder::init_senders(senders)?;

        Ok(Queue {
            start: Local::now(),
            senders,
            receivers,
            stats,
            skip_weekends: self.skip_weekends,
            skip_permanent: self.skip_permanent,
            skip_codes: self.skip_codes,
            rate: self.rate,
            daily_limit: self.daily_limit,
            workers: self.workers,
            failed: Vec::new(),
        })
    }
}

pub struct Queue {
    start: DateTime<Local>,
    senders: HashMap<String, Arc<Sender>>,
    receivers: Receivers,
    stats: HashMap<String, Stats>,
    skip_weekends: bool,
    skip_permanent: bool,
    skip_codes: Vec<u16>,
    rate: Duration,
    daily_limit: u32,
    workers: u8,
    failed: Receivers,
}

impl Queue {
    pub fn builder() -> Builder {
        Builder::default()
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

    fn reset_daily_lim(&mut self) {
        debug!(msg = "resetting daily limits");
        self.stats.iter_mut().for_each(|(_, stat)| stat.reset_day());
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
        tasks: Vec<JoinHandle<task::Result>>,
    ) -> Result<usize, task::Error> {
        let len = tasks.len();
        for res in tasks {
            debug!(msg = "collecting task results");
            match res.join().expect("could not join task thread") {
                Ok(task) => {
                    let stats = self.stats.get_mut(&task.sender.email).unwrap();
                    stats.inc_sent(1);
                    info!(
                        msg = "success",
                        sender = task.sender.email,
                        receiver = task.receiver.email
                    );
                    self.remove_receiver(&task.receiver);
                }

                Err(err) => match err {
                    task::Error::AddressError { .. }
                    | task::Error::RenderError { .. }
                    | task::Error::TransportError { .. }
                    | task::Error::MessageBuildError { .. } => {
                        return Err(err);
                    }
                    task::Error::SendError { task, err } => {
                        error!(
                            msg = "failure",
                            error = format!("{err}"),
                            sender = task.sender.email,
                            receiver = task.receiver.email
                        );

                        let stats = self.stats.get_mut(&task.sender.email).unwrap();
                        if self.skip_permanent && err.is_permanent() {
                            stats.skip();
                            stats.inc_bounced(1);
                        }
                        match Queue::code_to_int(err.status()) {
                            Some(code) => {
                                if self.skip_codes.binary_search(&code).is_ok() {
                                    stats.skip();
                                    stats.inc_bounced(1);
                                }
                            }
                            None => stats.inc_failed(1),
                        };
                        self.remove_receiver(&task.receiver);
                        self.failed.push(task.receiver);
                    }
                },
            }
        }

        Ok(len)
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

    fn pos_min_timeout(&mut self, ptr: &mut usize) {
        let (sender, _) = self
            .senders
            .iter()
            .min_by(|x, y| {
                let (x, y) = (self.stats.get(x.0).unwrap(), self.stats.get(y.0).unwrap());
                if x.timeout.is_none() {
                    return Ordering::Less;
                } else if y.timeout.is_none() {
                    return Ordering::Greater;
                }

                let (x, y) = (x.timeout.unwrap(), y.timeout.unwrap());
                x.cmp(&y)
            })
            .unwrap();
        let sender = sender.to_owned();

        match self.receivers.iter().position(|r| r.sender.eq(&sender)) {
            Some(p) => {
                *ptr = p;
            }
            None => {
                self.senders.remove(&sender);
                self.pos_min_timeout(ptr);
            }
        }
    }

    pub fn run(&mut self) -> Result<(), task::Error> {
        self.start = Local::now();
        let (mut ptr, mut sent, mut skips) = (0, 0, 0);
        info!(msg = "starting queue", start = format!("{}", self.start));

        'main: loop {
            if self.skip_weekends {
                Queue::skip_weekend();
            }

            let mut tasks: Vec<JoinHandle<task::Result>> = Vec::new();
            for _ in 0..self.workers {
                if self.receivers.is_empty() {
                    info!(msg = "finished sending mails", total_sent = sent);
                    break 'main;
                }

                if Queue::is_tomorrow(self.start) {
                    info!(msg = "it is tomorrow", time = format!("{}", Local::now()));
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
                        self.failed.push(receiver);
                        ptr += 1;
                        continue;
                    }
                };

                if stat.should_skip() {
                    stat.inc_skipped(1);
                    warn!(
                        msg = "skipping flagged sender",
                        sender = receiver.sender,
                        receiver = receiver.email,
                    );

                    self.remove_receiver(&receiver);
                    self.failed.push(receiver);
                    ptr += 1;
                    continue;
                }

                if stat.is_timed_out() {
                    warn!(
                        msg = "skipping timed-out sender",
                        sender = receiver.sender,
                        receiver = receiver.email
                    );

                    skips += 1;
                    if skips >= self.receivers.len() {
                        self.pos_min_timeout(&mut ptr);
                        let sender = &self.receivers[ptr].sender;
                        let stats = self.stats.get_mut(&self.receivers[ptr].sender).unwrap();
                        info!(msg = "got sender with least timeout", sender = sender);
                        if stats.is_timed_out() {
                            let (now, timeout) = (Local::now(), stats.timeout.unwrap());
                            if now.lt(&timeout) {
                                let diff = timeout - now;
                                warn!(msg = "global timeout", duration = format!("{diff}"));
                                thread::sleep(diff.to_std().unwrap())
                            }
                        }
                        skips = 0;
                        continue;
                    }
                    ptr += 1;
                    continue;
                }

                if !Queue::is_tomorrow(self.start) && stat.today > self.daily_limit {
                    warn!(
                        msg = "sender hit daily limit; skipping",
                        sender = receiver.sender,
                        receiver = receiver.email
                    );
                    stat.set_timeout(Duration::try_hours(24).unwrap());
                    ptr += 1;
                    continue;
                }

                let sender = self.senders.get(&receiver.sender).unwrap();
                let task = task::Task::new(sender.clone(), receiver.clone());

                tasks.push(task.spawn());

                stat.set_timeout(self.rate);
                ptr += 1;
            }

            sent += self.collect_tasks(tasks)?;
            self.save_progress();
        }

        Ok(())
    }

    fn save_progress(&self) {
        self.save_stats()
            .unwrap_or_else(|e| warn!(msg = "could not save statistics", error = format!("{e}")));
        Queue::save_receivers(&self.receivers, "failed.csv")
            .unwrap_or_else(|e| warn!(msg = "could not save failures", error = format!("{e}")));
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
}

impl Drop for Queue {
    fn drop(&mut self) {
        self.save_progress()
    }
}
