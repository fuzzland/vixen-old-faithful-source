use std::ops::RangeInclusive;

use clap::Parser;
use tokio::signal;
use yellowstone_faithful_car_source::{OldFaithfulCarConfig, OldFaithfulCarSource};
use yellowstone_vixen::{
    config::VixenConfig, Handler, HandlerResult, Pipeline, Runtime,
    vixen_core::{ParseResult, Parser as VixenParser, Prefilter, TransactionPrefilter, TransactionUpdate},
};

#[derive(Parser, Debug)]
struct Args {
    /// Directory to store/download CAR files.
    #[arg(long, default_value = "./old-faithful-car")]
    data_dir: String,
    /// First epoch to stream (inclusive).
    #[arg(long)]
    start_epoch: u64,
    /// Last epoch to stream (inclusive).
    #[arg(long)]
    end_epoch: u64,
    /// Stop after this many transactions (optional).
    #[arg(long)]
    max_transactions: Option<usize>,
}

#[derive(Debug)]
struct PassthroughParser;

impl VixenParser for PassthroughParser {
    type Input = TransactionUpdate;
    type Output = TransactionUpdate;

    fn id(&self) -> std::borrow::Cow<'static, str> { "car-transaction".into() }

    fn prefilter(&self) -> Prefilter {
        Prefilter {
            transaction: Some(TransactionPrefilter::default()),
            ..Prefilter::default()
        }
    }

    fn parse(
        &self,
        value: &Self::Input,
    ) -> impl std::future::Future<Output = ParseResult<Self::Output>> + Send {
        let cloned = value.clone();
        async move { Ok(cloned) }
    }
}

#[derive(Debug, Default)]
struct LogHandler;

impl Handler<TransactionUpdate, TransactionUpdate> for LogHandler {
    async fn handle(&self, value: &TransactionUpdate, _raw: &TransactionUpdate) -> HandlerResult<()> {
        if let Some(info) = &value.transaction {
            println!(
                "slot={} idx={} sig={}",
                value.slot,
                info.index,
                yellowstone_vixen::bs58::encode(&info.signature).into_string()
            );
        } else {
            println!("slot={} (empty transaction info)", value.slot);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let epochs: Vec<u64> = RangeInclusive::new(args.start_epoch, args.end_epoch).collect();

    let source = OldFaithfulCarConfig {
        data_dir: args.data_dir.into(),
        epochs,
        max_blocks: None,
        max_transactions: args.max_transactions,
        skip_download: false,
    };

    let config = VixenConfig {
        source,
        buffer: Default::default(),
    };

    let runtime = Runtime::<OldFaithfulCarSource>::builder()
        .transaction(Pipeline::new(PassthroughParser, [LogHandler::default()]))
        .build(config);

    // Run until completion or Ctrl+C.
    tokio::select! {
        _ = runtime.try_run_async() => {},
        _ = signal::ctrl_c() => {
            eprintln!("Received Ctrl+C, shutting down");
        }
    }
}
