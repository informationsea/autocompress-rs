use super::*;
use crate::tests::{SmallStepReader, SmallStepWriter};
use std::io::{BufReader, BufWriter};

#[test]
fn test_rayon_reader_into_inner() -> anyhow::Result<()> {
    let expected_data = include_bytes!("../../../testfiles/sqlite3.c");

    let mut read_buffer = vec![];
    let mut reader = RayonReader::with_thread_builder_and_capacity(
        std::fs::File::open("testfiles/sqlite3.c").unwrap(),
        RayonThreadBuilder,
        101,
    );
    reader.read_to_end(&mut read_buffer).unwrap();
    let inner = reader.into_inner();
    assert_eq!(inner.metadata()?.len(), expected_data.len() as u64);
    assert_eq!(expected_data.len(), read_buffer.len());
    assert_eq!(&expected_data[..], &read_buffer[..]);

    Ok(())
}

#[test]
fn test_rayon_writer_into_inner() -> anyhow::Result<()> {
    let expected_data = include_bytes!("../../../testfiles/sqlite3.c");

    let writer_buffer = vec![];
    let mut writer = RayonWriter::new(writer_buffer);
    writer.write_all(&expected_data[..])?;
    let inner = writer.into_inner_writer();
    assert_eq!(expected_data.len(), inner.len());
    assert_eq!(&expected_data[..], &inner[..]);

    Ok(())
}

#[test]
pub fn test_rayon_reader() -> anyhow::Result<()> {
    let expected_data = include_bytes!("../../../testfiles/sqlite3.c");
    let (send, recv) = channel();

    const N: usize = 100;

    for i in 0..N {
        let send = send.clone();
        rayon::spawn(move || {
            //eprintln!("start {}", i);

            let mut read_buffer = vec![];
            // write test
            let mut reader = RayonReader::with_thread_builder_and_capacity(
                std::fs::File::open("testfiles/sqlite3.c").unwrap(),
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
pub fn test_rayon_reader_small_step1() -> anyhow::Result<()> {
    let expected_data = include_bytes!("../../../testfiles/sqlite3.c");
    let (send, recv) = channel();

    const N: usize = 100;

    for i in 0..N {
        let send = send.clone();
        rayon::spawn(move || {
            //eprintln!("start {}", i);

            let mut read_buffer = vec![];
            // read test
            let mut reader = RayonReader::with_thread_builder_and_capacity(
                SmallStepReader::new(
                    BufReader::new(std::fs::File::open("testfiles/sqlite3.c").unwrap()),
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
    let expected_data = include_bytes!("../../../testfiles/sqlite3.c");
    let (send, recv) = channel();

    const N: usize = 100;

    for i in 0..N {
        let send = send.clone();
        rayon::spawn(move || {
            //eprintln!("start {}", i);

            let mut read_buffer = vec![];
            // read test
            let mut reader = SmallStepReader::new(
                RayonReader::with_thread_builder_and_capacity(
                    std::fs::File::open("testfiles/sqlite3.c").unwrap(),
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
pub fn test_rayon_writer() -> anyhow::Result<()> {
    let expected_data = include_bytes!("../../../testfiles/sqlite3.c");
    let (send, recv) = channel();

    const N: usize = 100;

    for i in 0..N {
        let send = send.clone();
        rayon::spawn(move || {
            //eprintln!("start {}", i);

            let path = format!("target/test_rayon_writer_{}.c", i);
            let writer_file = std::fs::File::create(&path).unwrap();
            // write test
            let mut writer =
                RayonWriter::with_thread_builder_and_capacity(writer_file, RayonThreadBuilder, 101);
            writer.write_all(&expected_data[..]).unwrap();
            writer.into_inner_writer().sync_all().unwrap();

            std::thread::sleep(std::time::Duration::from_millis(10));
            let read_buffer = std::fs::read(path).unwrap();
            assert_eq!(expected_data.len(), read_buffer.len());
            assert_eq!(&expected_data[..], &read_buffer[..]);

            send.send(i).unwrap();
        });
    }

    //eprintln!("waiting");

    for x in 0..N {
        let i = recv.recv().unwrap();
        eprintln!("Ok {} {x}", i);
    }

    Ok(())
}

#[test]
pub fn test_rayon_writer_small_step1() -> anyhow::Result<()> {
    let expected_data = include_bytes!("../../../testfiles/sqlite3.c");
    let (send, recv) = channel();

    const N: usize = 100;

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
    let expected_data = include_bytes!("../../../testfiles/sqlite3.c");
    let (send, recv) = channel();

    const N: usize = 100;

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
