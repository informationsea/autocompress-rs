#[cfg(feature = "tokio_fs")]
use autocompress::io::AsyncProcessorWriter;
use autocompress::{CompressionLevel, Processor};
use clap::{Parser, ValueEnum};
use std::pin::Pin;
#[cfg(feature = "tokio_fs")]
use tokio::io::AsyncWrite;

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
}

#[cfg(not(feature = "tokio_fs"))]
fn main() {
    eprintln!("Tokio is required")
}

#[cfg(feature = "tokio_fs")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let mut reader =
        Box::pin(autocompress::autodetect_async_open_or_stdin(cli.input.clone()).await?);

    let compression_level = if cli.fast {
        CompressionLevel::fast()
    } else if cli.best {
        CompressionLevel::best()
    } else {
        CompressionLevel::default()
    };
    let mut writer: Pin<Box<dyn AsyncWrite>> = if let Some(format) = cli.format {
        let p: Box<dyn Processor + Unpin + Send> = match format {
            #[cfg(feature = "gzip")]
            FileFormat::Gzip => Box::new(autocompress::gzip::GzipCompress::new(
                compression_level.flate2(),
            )),
            #[cfg(feature = "bgzip")]
            FileFormat::BGZip => Box::new(autocompress::bgzip::BgzipCompress::new(
                compression_level.bgzip(),
            )),
            #[cfg(feature = "xz")]
            FileFormat::Xz => Box::new(autocompress::xz::XzCompress::new(compression_level.xz())?),
            #[cfg(feature = "bzip2")]
            FileFormat::Bzip2 => Box::new(autocompress::bzip2::Bzip2Compress::new(
                compression_level.bzip2(),
            )),
            #[cfg(feature = "zstd")]
            FileFormat::Zstd => Box::new(autocompress::zstd::ZstdCompress::new(
                compression_level.zstd(),
            )?),
        };
        let writer: Box<dyn AsyncWrite + Send + Unpin> = if let Some(path) = cli.output {
            Box::new(tokio::fs::File::create(path).await?)
        } else {
            Box::new(tokio::io::stdout())
        };

        Box::pin(AsyncProcessorWriter::with_processor(p, writer))
    } else {
        Box::pin(
            autocompress::autodetect_async_create_or_stdout(cli.output.clone(), compression_level)
                .await?,
        )
    };
    tokio::io::copy(&mut reader, &mut writer).await?;

    Ok(())
}
