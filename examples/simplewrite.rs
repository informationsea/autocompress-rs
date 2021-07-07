use clap::{App, Arg};
use rand::prelude::*;
use std::io::{self, prelude::*};

fn main() -> io::Result<()> {
    let matches = App::new("writer test")
        .version("0.1")
        .author("Yasunobu Okamura")
        .about("Writer test")
        .arg(
            Arg::with_name("output")
                .index(1)
                .value_name("FILE")
                .help("Output file name")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("use-thread")
                .short("t")
                .long("thread")
                .takes_value(true)
                .help("Enable threaded I/O"),
        )
        .arg(
            Arg::with_name("bytes")
                .short("b")
                .long("bytes")
                .help("Output file bytes")
                .value_name("BYTES")
                .default_value("500000000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("random-generate-in-loop")
                .short("r")
                .long("random-generate-in-loop")
                .help("Generate random value in loop"),
        )
        .arg(
            Arg::with_name("best")
                .long("best")
                .help("Best compression")
                .conflicts_with("fast"),
        )
        .arg(
            Arg::with_name("fast")
                .long("Fast")
                .help("Fast compression")
                .conflicts_with("best"),
        )
        .get_matches();

    let thread_num: usize = matches
        .value_of("use-thread")
        .map(|x| x.parse().expect("number of threads must be number"))
        .unwrap_or(0);

    let bytes: u64 = matches
        .value_of("bytes")
        .map(|x| x.parse().expect("output file bytes must be number"))
        .unwrap();

    let random_generate = matches.is_present("random-generate-in-loop");

    let compression_level = if matches.is_present("fast") {
        autocompress::CompressionLevel::Fast
    } else if matches.is_present("best") {
        autocompress::CompressionLevel::Best
    } else {
        autocompress::CompressionLevel::Default
    };

    let mut writer = autocompress::create(matches.value_of("output").unwrap(), compression_level)?;

    if thread_num > 0 {
        let threadio = autocompress::threadio::IoThread::new(thread_num);
        let mut writer = threadio.add_writer(writer);
        write_data(&mut writer, bytes, random_generate)?;
    } else {
        write_data(&mut writer, bytes, random_generate)?;
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
