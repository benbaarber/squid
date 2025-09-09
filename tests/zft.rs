use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    thread,
};

use anyhow::Result;
use squid::zft;

/// Send a file or directory from one path to another over tcp
fn zft_send(src_path: &Path, dst_dir: &Path) -> Result<()> {
    // Send file over tcp
    let ctx = zmq::Context::new();
    let rcvsock = ctx.socket(zmq::DEALER)?;
    rcvsock.bind("tcp://*:7345")?;

    let ctx_c = ctx.clone();
    let src_path_c = src_path.to_path_buf();
    let sndthread = thread::spawn(move || -> Result<()> {
        let ctx = ctx_c;
        let sndsock = ctx.socket(zmq::DEALER)?;
        sndsock.connect("tcp://127.0.0.1:7345")?;

        zft::send(src_path_c, &sndsock)?;
        Ok(())
    });

    zft::recv(&dst_dir, &rcvsock)?;
    sndthread.join().unwrap()?;

    Ok(())
}

#[test]
fn zft_roundtrip() -> Result<()> {
    let src_dir = PathBuf::from("/tmp/squid_zft");
    fs::create_dir_all(&src_dir)?;
    let dst_dir = PathBuf::from("/tmp/squid_zft_dst");
    fs::create_dir_all(&dst_dir)?;

    // Create file
    let mut f = File::create(src_dir.join("test.bin"))?;
    let mut buf = vec![0u8; 1_000_000];
    rand::fill(&mut buf[..]);
    f.write_all(&buf)?;
    f.flush()?;
    drop(buf);

    // Create nested file
    let nested_src = src_dir.join("dir");
    fs::create_dir_all(&nested_src)?;
    let mut f = File::create(nested_src.join("nest.bin"))?;
    let mut buf = vec![0u8; 1_000_000];
    rand::fill(&mut buf[..]);
    f.write_all(&buf)?;
    f.flush()?;
    drop(buf);

    // Send
    zft_send(&src_dir, &dst_dir)?;

    // Assertions
    let dst_dir = dst_dir.join("squid_zft");
    assert!(dst_dir.exists());
    assert!(dst_dir.join("test.bin").exists());

    let sent = fs::read(src_dir.join("test.bin"))?;
    let rcvd = fs::read(dst_dir.join("test.bin"))?;
    assert_eq!(&sent, &rcvd);

    let nested_dst = dst_dir.join("dir");
    assert!(nested_dst.exists());
    assert!(nested_dst.join("nest.bin").exists());

    let sent = fs::read(nested_src.join("nest.bin"))?;
    let rcvd = fs::read(nested_dst.join("nest.bin"))?;
    assert_eq!(&sent, &rcvd);

    Ok(())
}

// TODO actually make size mismatch and finish this test
#[ignore]
#[test]
fn zmq_roundtrip_size_mismatch() -> Result<()> {
    let src_dir = PathBuf::from("/tmp/squid_zft");
    fs::create_dir_all(&src_dir)?;
    let dst_dir = PathBuf::from("/tmp/squid_zft_dst");
    fs::create_dir_all(&dst_dir)?;

    // Create file
    let src_path = src_dir.join("test.bin");
    let mut f = File::create(&src_path)?;
    let mut buf = vec![0u8; 1_000_000];
    rand::fill(&mut buf[..]);
    f.write_all(&buf)?;
    f.flush()?;
    drop(buf);

    // Send
    zft_send(&src_path, &dst_dir)?;

    // Assertions
    assert!(dst_dir.exists());
    assert!(dst_dir.join("test.bin").exists());

    let sent = fs::read(src_dir.join("test.bin"))?;
    let rcvd = fs::read(dst_dir.join("test.bin"))?;
    assert_eq!(&sent, &rcvd);

    Ok(())
}
