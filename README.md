# Yellowstone Old Faithful CAR Source

A Vixen source that streams Old Faithful CAR archives as `SubscribeUpdate` messages, plus a small CAR parsing toolkit.

## Note
This source only emits transaction and slot updates; block/account/rewards updates from the CAR aren’t surfaced, so pipelines depending on those won’t fire.
If you need block/account/rewards updates, use Jetstreamer data source (slower but with all those updates). 

## Usage

Add the crate and pick the epochs you want:

```toml
[dependencies]
yellowstone-faithful-car-source = { path = "." }
```

Create the source with your filters and run it via Vixen:

```rust
use yellowstone_faithful_car_source::{OldFaithfulCarConfig, OldFaithfulCarSource};
use yellowstone_vixen::Runtime;

let config = OldFaithfulCarConfig {
    data_dir: "./old-faithful-car".into(),
    epochs: vec![39],
    max_blocks: None,
    max_transactions: None,
    skip_download: false,
};

Runtime::<OldFaithfulCarSource>::builder()
    // register your pipelines here
    .build(config)
    .run();
```

Notes:
- `aria2c` must be available in PATH for downloads.
- Only transaction and slot updates are emitted; other update types are ignored.

## Example: stream an epoch range

Run the included example to download and stream a range of epochs:

```bash
cargo run --example stream -- --start-epoch 39 --end-epoch 39 --data-dir ./old-faithful-car
```

This prints transaction signatures (with slot/index) as they are parsed from the CAR files. Use `--max-transactions N` to stop after a fixed count.

## License

AGPL-3.0-only (see `LICENSE`). The CAR parser code is derived from the AGPL-licensed Old Faithful parser https://github.com/lamports-dev/yellowstone-faithful-car-parser.
