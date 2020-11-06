use anyhow::{Context, Result};
use std::{
    env,
    fs::File,
    io::{self, BufReader, Error, ErrorKind, Read, Result as IoResult, Write},
    mem,
};

pub fn try_read_exact<R: Read>(reader: &mut R) -> IoResult<Option<u32>> {
    let mut buffer: [u8; mem::size_of::<u32>()] = [0; mem::size_of::<u32>()];
    let mut read = reader.read(&mut buffer)?;
    if read == 0 {
        return Ok(None);
    }
    while read != 4 {
        let add = reader.read(&mut buffer[read..])?;
        if add == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        read += add;
    }
    Ok(Some(u32::from_le_bytes(buffer)))
}

fn main() -> Result<()> {
    let mut args = env::args();
    args.next();
    let prefix = if let Some(x) = args.next() {
        x
    } else {
        eprintln!("usage: from_vertex prefix outputname");
        return Ok(());
    };
    let mut edges = prefix.clone();
    edges.push_str(".edges");
    let mut nodes = prefix.clone();
    nodes.push_str(".nodes");
    let nodes_file = File::open(nodes).context("opening edges file")?;
    let edges_file = File::open(edges).context("opening nodes file")?;
    let mut nodes_reader = BufReader::new(nodes_file);
    let mut edges_reader = BufReader::new(edges_file);
    let stdout = io::stdout();
    let mut out_writer = stdout.lock();

    let mut buffer: [u8; mem::size_of::<u32>()] = [0; mem::size_of::<u32>()];
    while let Some(id) = try_read_exact(&mut nodes_reader)? {
        nodes_reader.read_exact(&mut buffer)?;
        let len = u32::from_le_bytes(buffer);
        write!(out_writer, "{}", id)?;
        for _ in 0..len {
            edges_reader.read_exact(&mut buffer)?;
            let id = u32::from_le_bytes(buffer);
            write!(out_writer, " {}", id)?;
        }
        writeln!(out_writer)?;
    }
    Ok(())
}
