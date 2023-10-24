use clap::Parser;
use rand::prelude::*;
use std::io::{self, prelude::*};

#[derive(Parser, Debug)]
#[command(
    name = "writer test",
    version = "0.1",
    author = "Yasunobu Okamura",
    about = "Writer test"
)]
struct Cli {
    #[arg(short, long, value_name = "FILE", help = "Output file name")]
    output: String,
    #[arg(short, long, value_name = "THREAD", help = "Enable threaded I/O")]
    use_thread: Option<usize>,
    #[arg(
        short,
        long,
        value_name = "BYTES",
        help = "Output file bytes",
        default_value = "500000000"
    )]
    bytes: u64,
    #[arg(short, long, help = "Generate random value in loop")]
    random_generate: bool,
    #[arg(long, conflicts_with = "fast", help = "Best compression")]
    best: bool,
    #[arg(long, conflicts_with = "best", help = "Fast compression")]
    fast: bool,
}

fn main() -> io::Result<()> {
    let cli = Cli::parse();

    let thread_num: usize = cli.use_thread.unwrap_or(0);

    let compression_level = if cli.fast {
        autocompress::CompressionLevel::Fast
    } else if cli.best {
        autocompress::CompressionLevel::Best
    } else {
        autocompress::CompressionLevel::Default
    };

    let mut writer = autocompress::create(&cli.output, compression_level)?;

    if thread_num > 0 {
        let threadio = autocompress::iothread::IoThread::new(thread_num);
        let mut writer = threadio.add_writer(writer);
        write_data(&mut writer, cli.bytes, cli.random_generate)?;
    } else {
        write_data(&mut writer, cli.bytes, cli.random_generate)?;
    }

    Ok(())
}

const TEXT_DATA: &[u8] = b"ATCG";
const RANDOM_TEXT_SIZE: usize = 1000;

fn write_data(mut writer: impl Write, bytes: u64, random_generate: bool) -> io::Result<()> {
    let mut write_buf = Vec::with_capacity(RANDOM_TEXT_SIZE);
    let mut rng = thread_rng();
    for _ in 0..RANDOM_TEXT_SIZE {
        write_buf.push(TEXT_DATA[rng.gen_range(0..4)]);
    }

    let mut remain_bytes = bytes as usize;

    while remain_bytes > 0 {
        if random_generate {
            write_buf.clear();
            for _ in 0..RANDOM_TEXT_SIZE {
                write_buf.push(TEXT_DATA[rng.gen_range(0..4)]);
            }
        }

        let to_write = if write_buf.len() < remain_bytes {
            write_buf.len()
        } else {
            remain_bytes
        };
        writer.write_all(&write_buf[..to_write])?;
        remain_bytes -= to_write;
    }

    Ok(())
}
