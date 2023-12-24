use super::*;
use crate::{
    gzip::GzipCompress,
    tests::{SmallStepReader, SmallStepWriter},
};
use std::io::{BufReader, BufWriter};

#[test]
pub fn test_rayon_reader_small_step1() -> anyhow::Result<()> {
    let expected_data = include_bytes!("../../../testfiles/pg2701.txt");
    let (send, recv) = channel();

    const N: usize = 10;

    for i in 0..N {
        let send = send.clone();
        rayon::spawn(move || {
            //eprintln!("start {}", i);

            let mut read_buffer = vec![];
            // read test
            let mut reader = RayonReader::with_thread_builder_and_capacity(
                SmallStepReader::new(
                    BufReader::new(std::fs::File::open("testfiles/pg2701.txt").unwrap()),
                    1,
                ),
                RayonThreadBuilder,
                101,
            );
            reader.read_to_end(&mut read_buffer).unwrap();
            std::mem::drop(reader);
            assert_eq!(expected_data.len(), read_buffer.len());
            assert_eq!(&expected_data[..], &read_buffer[..]);

            send.send(i).unwrap();
        });
    }

    //eprintln!("waiting");

    for _x in 0..N {
        let _i = recv.recv().unwrap();
        //eprintln!("Ok {} {_x}", _i);
    }

    Ok(())
}

#[test]
pub fn test_rayon_reader_small_step2() -> anyhow::Result<()> {
    let expected_data = include_bytes!("../../../testfiles/pg2701.txt");
    let (send, recv) = channel();

    const N: usize = 10;

    for i in 0..N {
        let send = send.clone();
        rayon::spawn(move || {
            //eprintln!("start {}", i);

            let mut read_buffer = vec![];
            // read test
            let mut reader = SmallStepReader::new(
                RayonReader::with_thread_builder_and_capacity(
                    std::fs::File::open("testfiles/pg2701.txt").unwrap(),
                    RayonThreadBuilder,
                    101,
                ),
                1,
            );
            reader.read_to_end(&mut read_buffer).unwrap();
            std::mem::drop(reader);
            assert_eq!(expected_data.len(), read_buffer.len());
            assert_eq!(&expected_data[..], &read_buffer[..]);

            send.send(i).unwrap();
        });
    }

    //eprintln!("waiting");

    for _x in 0..N {
        let _i = recv.recv().unwrap();
        //eprintln!("Ok {} {_x}", _i);
    }

    Ok(())
}

#[test]
pub fn test_rayon_writer_small_step1() -> anyhow::Result<()> {
    let expected_data = include_bytes!("../../../testfiles/pg2701.txt");
    let (send, recv) = channel();

    const N: usize = 10;

    for i in 0..N {
        let send = send.clone();
        rayon::spawn(move || {
            //eprintln!("start {}", i);

            let path = format!("target/test_rayon_writer_small_step1_{}.c", i);
            let writer_file =
                SmallStepWriter::new(BufWriter::new(std::fs::File::create(&path).unwrap()), 1);
            // write test
            let mut writer =
                RayonWriter::with_thread_builder_and_capacity(writer_file, RayonThreadBuilder, 101);
            writer.write_all(&expected_data[..]).unwrap();
            writer.flush().unwrap();

            std::thread::sleep(std::time::Duration::from_millis(10));
            let read_buffer = std::fs::read(path).unwrap();
            assert_eq!(expected_data.len(), read_buffer.len());
            assert_eq!(&expected_data[..], &read_buffer[..]);

            send.send(i).unwrap();
        });
    }

    //eprintln!("waiting");

    for _x in 0..N {
        let _i = recv.recv().unwrap();
        //eprintln!("Ok {} {_x}", _i);
    }

    Ok(())
}

#[test]
pub fn test_rayon_writer_small_step2() -> anyhow::Result<()> {
    let expected_data = include_bytes!("../../../testfiles/pg2701.txt");
    let (send, recv) = channel();

    const N: usize = 10;

    for i in 0..N {
        let send = send.clone();
        rayon::spawn(move || {
            //eprintln!("start {}", i);

            let path = format!("target/test_rayon_writer_small_step2_{}.c", i);
            let writer_file = std::fs::File::create(&path).unwrap();
            // write test
            let mut writer = SmallStepWriter::new(
                RayonWriter::with_thread_builder_and_capacity(writer_file, RayonThreadBuilder, 101),
                1,
            );
            writer.write_all(&expected_data[..]).unwrap();
            writer.flush().unwrap();

            std::thread::sleep(std::time::Duration::from_millis(10));
            let read_buffer = std::fs::read(path).unwrap();
            assert_eq!(expected_data.len(), read_buffer.len());
            assert_eq!(&expected_data[..], &read_buffer[..]);

            send.send(i).unwrap();
        });
    }

    //eprintln!("waiting");

    for _x in 0..N {
        let _i = recv.recv().unwrap();
        //eprintln!("Ok {} {_x}", _i);
    }

    Ok(())
}

#[test]
fn test_rayon_parallel_writer_many() -> anyhow::Result<()> {
    let try_count = 10;
    let parallel_count = 20;

    let (send, recv) = channel();

    for _ in 0..parallel_count {
        let send = send.clone();
        rayon::spawn(move || {
            let write_buf = vec![];
            let mut writer = ParallelCompressWriter::with_buffer_size(
                write_buf,
                || GzipCompress::default(),
                101,
                3,
            );
            let expected_data = include_bytes!("../../../testfiles/pg2701.txt");
            for _ in 0..try_count {
                writer.write_all(&expected_data[..]).unwrap();
            }
            writer.flush().unwrap();
            let inner = writer.into_inner().unwrap();
            let mut reader = flate2::read::MultiGzDecoder::new(&inner[..]);
            let mut read_buffer = vec![];
            reader.read_to_end(&mut read_buffer).unwrap();
            assert_eq!(expected_data.len() * try_count, read_buffer.len());
            //assert_eq!(&expected_data[..], &read_buffer[..]);
            send.send(()).unwrap();
        });
    }

    for _ in 0..parallel_count {
        recv.recv().unwrap();
    }

    Ok(())
}
