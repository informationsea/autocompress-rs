#[cfg(feature = "rayon")]
use autocompress::io::ParallelCompressWriter;
use autocompress::{CompressionLevel, Processor};
use clap::{Parser, ValueEnum};
use std::fs::File;
use std::io::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum FileFormat {
    #[cfg(feature = "xz")]
    Xz,
    #[cfg(feature = "zstd")]
    Zstd,
    #[cfg(feature = "bgzip")]
    BGZip,
    #[cfg(feature = "gzip")]
    Gzip,
    #[cfg(feature = "bzip2")]
    Bzip2,
}

impl Into<autocompress::FileFormat> for FileFormat {
    fn into(self) -> autocompress::FileFormat {
        match self {
            #[cfg(feature = "gzip")]
            Self::Gzip => autocompress::FileFormat::Gzip,
            #[cfg(feature = "bgzip")]
            Self::BGZip => autocompress::FileFormat::BGZip,
            #[cfg(feature = "xz")]
            Self::Xz => autocompress::FileFormat::Xz,
            #[cfg(feature = "bzip2")]
            Self::Bzip2 => autocompress::FileFormat::Bzip2,
            #[cfg(feature = "zstd")]
            Self::Zstd => autocompress::FileFormat::Zstd,
        }
    }
}

#[derive(Parser)]
#[clap(name = "autocompress", version, author)]
struct Cli {
    #[clap(short = '1', long, help = "compress faster")]
    fast: bool,
    #[clap(short = '9', long, help = "compress better")]
    best: bool,
    #[clap(name = "FILE", help = "Input file")]
    input: Option<String>,
    #[clap(name = "OUTPUT", help = "Output file")]
    output: Option<String>,
    #[clap(short, long, help = "File format")]
    format: Option<FileFormat>,
    #[clap(short, long, help = "Number of threads")]
    threads: Option<usize>,
}

#[cfg(not(feature = "rayon"))]
fn main() {
    eprintln!("rayon is required")
}

#[cfg(feature = "rayon")]
fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if let Some(threads) = cli.threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .unwrap();
    }

    let mut reader = autocompress::io::RayonReader::new(autocompress::autodetect_open_or_stdin(
        cli.input.clone(),
    )?);

    let compression_level = if cli.fast {
        CompressionLevel::fast()
    } else if cli.best {
        CompressionLevel::best()
    } else {
        CompressionLevel::default()
    };

    let format: autocompress::FileFormat = cli.format.map(|x| x.into()).unwrap_or_else(|| {
        if let Some(output) = cli.output.as_ref() {
            autocompress::FileFormat::from_path(output).unwrap_or(autocompress::FileFormat::Plain)
        } else {
            autocompress::FileFormat::Plain
        }
    });
    eprintln!("Output Format: {:?}", format);

    let processor_generator =
        move || -> Box<dyn Processor + Unpin + Send> { format.compressor(compression_level) };

    let mut writer = ParallelCompressWriter::new(
        if let Some(output) = cli.output.as_ref() {
            Box::new(File::create(output)?) as Box<dyn Write + Send>
        } else {
            Box::new(std::io::stdout())
        },
        processor_generator,
    );
    std::io::copy(&mut reader, &mut writer)?;

    Ok(())
}
