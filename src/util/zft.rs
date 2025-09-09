/// zmq file transfer
///
/// TODO
/// - option to send directory structure without top level dir
/// - checksums?
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Result, bail};
use num_enum::{FromPrimitive, IntoPrimitive};
use tracing::{info, warn};

use crate::{SocketByteExt, de_u64};

const CHUNK: usize = 256 * 1024; // 256kb
const TIMEOUT: i64 = 5000;

#[derive(Debug, PartialEq, FromPrimitive, IntoPrimitive)]
#[repr(u8)]
enum TxP {
    File, // (name: str) (size: u64)
    Dir,  // (name: str)
    Data, // (seq: u64) (data: bytes)
    End,
    #[default]
    Noop,
}

#[derive(Debug, PartialEq, FromPrimitive, IntoPrimitive)]
#[repr(u8)]
enum RxP {
    Ack,
    Retry,
    #[default]
    Noop,
}

/// Send a file
///
/// Socket must be DEALER or PAIR
pub fn send(path: PathBuf, sock: &zmq::Socket) -> Result<()> {
    if path.is_file() {
        send_file(path, sock)?;
    } else if path.is_dir() {
        let Some(dir_name) = path.file_name() else {
            bail!("ZFT send got invalid dir name (..)");
        };
        let Some(dir_name_str) = dir_name.to_str() else {
            bail!("ZFT send got non-utf8 dir name: {:?}", dir_name);
        };
        sock.send_byte(TxP::Dir, zmq::SNDMORE)?;
        sock.send(dir_name_str, 0)?;
        for e in fs::read_dir(path)? {
            if let Ok(e) = e {
                send(e.path(), sock)?;
            }
        }
        sock.send_byte(TxP::End, 0)?;
    }

    Ok(())
}

fn send_file(path: PathBuf, sock: &zmq::Socket) -> Result<()> {
    loop {
        let name = path.file_name().and_then(|s| s.to_str()).ok_or_else(|| {
            std::io::Error::other("ZFT send tried to send a file with a bad name")
        })?;
        let mut f = File::open(&path)?;
        let size = f.metadata()?.len();

        sock.send_byte(TxP::File, zmq::SNDMORE)?;
        sock.send_multipart([name.as_bytes(), &size.to_be_bytes()], 0)?;

        let mut buf = vec![0u8; CHUNK];
        let mut seq = 0u64;
        loop {
            let n = f.read(&mut buf)?;
            if n == 0 {
                break;
            }
            sock.send_byte(TxP::Data, zmq::SNDMORE)?;
            sock.send_multipart([&seq.to_be_bytes(), &buf[..n]], 0)?;
            seq += 1;
        }

        sock.send_byte(TxP::End, 0)?;

        // wait for ack
        if sock.poll(zmq::POLLIN, TIMEOUT)? > 0 {
            let res: RxP = sock.recv_byte(0)?.into();
            match res {
                RxP::Ack => break,
                RxP::Retry => continue,
                x => bail!(
                    "ZFT send got invalid response {:?}, expected one of [Ack, Retry]",
                    x
                ),
            }
        } else {
            bail!("ZFT send timed out waiting for ACK");
        }
    }

    Ok(())
}

/// Receive a file
///
/// Socket must be DEALER or PAIR
pub fn recv(out_dir: &Path, sock: &zmq::Socket) -> Result<()> {
    fs::create_dir_all(out_dir)?;

    let mut path = out_dir.to_path_buf();
    let mut depth = 0;
    loop {
        if sock.poll(zmq::POLLIN, TIMEOUT)? == 0 {
            bail!("ZFT recv timed out waiting for next frame");
        }

        let cmd: TxP = sock.recv_byte(0)?.into();
        match cmd {
            TxP::File => {
                recv_file(&path, sock)?;
                if depth == 0 {
                    break;
                }
            }
            TxP::Dir => {
                let dir_name = String::from_utf8(sock.recv_bytes(0)?)?;
                path.push(&dir_name);
                fs::create_dir_all(&path)?;
                depth += 1;
            }
            TxP::End => {
                path.pop();
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            x => bail!(
                "ZFT recv got invalid command {:?}, expected one of [File, Dir, End]",
                x
            ),
        }
    }

    Ok(())
}

fn recv_file(out_dir: &Path, sock: &zmq::Socket) -> Result<()> {
    let name = String::from_utf8(sock.recv_bytes(0)?)?;
    let size = de_u64(&sock.recv_bytes(0)?)?;

    let fpath = Path::new(&name);
    check_path(fpath)?;
    let fpath = out_dir.join(fpath);

    let mut f = File::create(&fpath)?;
    let mut recvd = 0u64;

    loop {
        if sock.poll(zmq::POLLIN, TIMEOUT)? == 0 {
            bail!("ZFT recv timed out waiting for next frame");
        }

        let cmd: TxP = sock.recv_byte(0)?.into();
        match cmd {
            TxP::Data => {
                let _seq = de_u64(&sock.recv_bytes(0)?)?;
                let data = sock.recv_bytes(0)?;
                // todo check seq continuity

                recvd += data.len() as u64;
                f.write_all(&data)?;
            }
            TxP::End => break,
            x => bail!(
                "ZFT recv got invalid command {:?}, expected one of [Data, End]",
                x
            ),
        }
    }

    f.flush()?;

    if recvd != size {
        warn!(
            "[ZFT] file size mismatch: expected {} bytes, received {} bytes. Retrying...",
            size, recvd
        );
        sock.send_byte(RxP::Retry, 0)?;
    } else {
        info!("[ZFT] received file {:?}", &fpath);
        sock.send_byte(RxP::Ack, 0)?;
    }

    Ok(())
}

fn check_path<P: AsRef<Path>>(path: P) -> Result<()> {
    let p = path.as_ref();
    if p.components().count() > 1 {
        bail!(
            "ZFT recv found unsafe path (more than 1 component): {:?}",
            p
        );
    }
    Ok(())
}
