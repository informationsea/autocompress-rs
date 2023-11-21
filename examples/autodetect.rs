use autocompress::FileFormat;
use clap::Parser;
use std::fs::File;
use std::io::BufReader;

#[derive(Debug, Parser)]
struct Cli {
    #[clap(help = "Files to detect file format")]
    input_file: Vec<String>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    for one in cli.input_file.iter() {
        let mut reader = BufReader::new(File::open(one)?);
        let format = FileFormat::from_buf_reader(&mut reader)?;
        println!("{}: {:?}", one, format);
    }

    Ok(())
}
