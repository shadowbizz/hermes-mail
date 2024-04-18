use clap::Parser;
use console::style;
use std::{env, io, process};
use tracing::{self, Level};
use tracing_appender::{self, non_blocking::WorkerGuard};
use tracing_subscriber::{
    fmt::{self, time, writer::MakeWriterExt},
    layer::SubscriberExt,
};

mod cmd;

type Error = Box<dyn std::error::Error>;

fn main() {
    let cmd = cmd::Cmd::parse();

    let (res, guard) = match cmd.command {
        cmd::Commands::Send(args) => (args.send(), init_logger()),
    };

    let _guard = guard.unwrap_or_else(|e| print_error(e));
    res.unwrap_or_else(|e| print_error(e));
}

fn print_error(e: Error) -> ! {
    eprintln!("{} {e}", style("error:").red().bright().bold());
    process::exit(1)
}

fn init_logger() -> Result<WorkerGuard, Error> {
    let time_format = time::ChronoLocal::new("%d-%m-%y %H:%M:%S%z".into());

    let appender = tracing_appender::rolling::daily(env::current_dir()?, "hermes.error.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(appender);

    let subscriber = tracing_subscriber::registry()
        .with(
            fmt::Layer::new()
                .with_writer(io::stdout.with_max_level(Level::INFO))
                .pretty()
                .with_timer(time_format.clone())
                .with_target(false)
                .with_line_number(false)
                .with_file(false),
        )
        .with(
            fmt::Layer::new()
                .with_writer(non_blocking.with_max_level(Level::ERROR))
                .json()
                .with_timer(time_format)
                .with_target(false)
                .with_line_number(false)
                .with_file(false),
        );

    tracing::subscriber::set_global_default(subscriber)?;

    Ok(guard)
}
