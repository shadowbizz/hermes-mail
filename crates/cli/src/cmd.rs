use clap::{ArgAction::SetTrue, Args, Parser, Subcommand};
use hermes_mailer::queue::{Builder, CodesVec};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about, long_about = None)]
/// hello
pub struct Cmd {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Send emails
    Send(SendCommand),
}

#[derive(Args)]
pub struct SendCommand {
    /// Path to file containing senders and their info
    #[arg(short, long, value_name = "FILE")]
    pub senders: PathBuf,
    /// Path to file containing receivers and their info
    #[arg(short, long, value_name = "FILE")]
    pub receivers: PathBuf,
    /// Sets the number of workers that will send email simultaneously
    #[arg(short, long, value_name = "NUMBER")]
    pub workers: Option<u8>,
    /// Sets the interval (in seconds) between each sender action
    #[arg(long, value_name = "NUMBER")]
    pub rate: Option<i64>,
    /// Sets the daily limit of emails that will be sent per sender
    #[arg(long, value_name = "NUMBER")]
    pub daily_rate: Option<u32>,
    /// Skip sending emails on the weekends
    #[arg(long, action = SetTrue)]
    pub skip_weekends: bool,
    /// Skip senders that have received prior permanent SMTP codes
    #[arg(long, action = SetTrue)]
    pub skip_permanent: bool,
    /// Skip senders which encounter the specified codes
    #[arg(long, value_name = "LIST", value_parser = clap::value_parser!(CodesVec))]
    pub skip_codes: Option<CodesVec>,
}

impl SendCommand {
    pub(crate) fn send(self) -> Result<(), super::Error> {
        let mut builder = Builder::new()
            .senders(self.senders.clone())
            .receivers(self.receivers.clone())
            .skip_codes(self.skip_codes.clone().unwrap_or(CodesVec::default()));

        if let Some(workers) = self.workers {
            builder = builder.workers(workers)
        }

        if let Some(rate) = self.rate {
            builder = builder.rate(rate)
        }

        if let Some(rate) = self.daily_rate {
            builder = builder.daily_rate(rate)
        }

        if self.skip_weekends {
            builder = builder.skip_weekends()
        }

        if self.skip_permanent {
            builder = builder.skip_weekends()
        }

        let mut q = builder.build()?;
        q.run()?;

        Ok(())
    }
}
