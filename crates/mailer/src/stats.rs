use chrono::{DateTime, Duration, Local};
use serde::Serialize;

#[derive(Serialize)]
pub(super) struct Stats {
    sender: String,
    pub(crate) today: u32,
    total: u64,
    bounced: u64,
    failed: u64,
    #[serde(skip_serializing)]
    skip: bool,
    skipped: u64,
    #[serde(skip_serializing)]
    pub(crate) timeout: Option<DateTime<Local>>,
}

impl Stats {
    pub fn new(addr: String) -> Self {
        Self {
            sender: addr,
            today: 0,
            total: 0,
            bounced: 0,
            failed: 0,
            skipped: 0,
            skip: false,
            timeout: None,
        }
    }

    pub fn set_timeout(&mut self, dur: Duration) {
        self.timeout = Some(Local::now() + dur);
    }

    pub fn is_timed_out(&mut self) -> bool {
        match self.timeout {
            Some(t) => {
                if Local::now().gt(&t) {
                    self.timeout = None;
                    return false;
                }
                true
            }
            None => false,
        }
    }

    pub fn inc_sent(&mut self, amnt: u32) {
        self.today += amnt;
        self.total += amnt as u64;
    }

    pub fn inc_bounced(&mut self, amnt: u64) {
        self.bounced += amnt;
        self.failed += amnt;
    }

    pub fn inc_failed(&mut self, amnt: u64) {
        self.failed += amnt;
    }

    pub fn inc_skipped(&mut self, amnt: u64) {
        self.skipped += amnt;
    }

    pub fn reset_day(&mut self) {
        self.today = 0;
    }

    pub fn skip(&mut self) {
        self.skip = true
    }

    pub fn should_skip(&mut self) -> bool {
        self.skip
    }
}
