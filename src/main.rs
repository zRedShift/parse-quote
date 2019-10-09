use chrono::NaiveDateTime;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::process;
use std::str;

const INVALID_TIMESTAMP: &str = "Invalid timestamp format";
const GLOBAL_HEADER_SIZE: u64 = 24;
const QUOTE_PACKET_OFFSET: i64 = 46;
const QUOTE_PACKET_SIZE: i64 = 215;
const BIDS_OFFSET: i64 = 12;
const PRICE_OFFSET: usize = 5;
const QUANTITY_OFFSET: usize = 7;
const QUOTE_ACCEPT_OFFSET: i64 = 50;
const QUOTE_ACCEPT_SIZE: usize = 8;
const SECONDS_IN_A_DAY: i64 = 24 * 3_600;
const KST_OFFSET: i64 = 9 * 3_600;
const MAX_DIFF: i64 = 3;
const QUOTE_PACKET_HEADER: [u8; 5] = [0x42, 0x36, 0x30, 0x33, 0x34];

struct QuotePacket {
    time_stamp: NaiveDateTime,
    quote_accept_time: NaiveDateTime,
    issue_code: [u8; 12],
    bids: [(u32, u32); 5],
    asks: [(u32, u32); 5],
}

impl Ord for QuotePacket {
    fn cmp(&self, other: &Self) -> Ordering {
        self.quote_accept_time
            .cmp(&other.quote_accept_time)
            .reverse()
    }
}

impl PartialOrd for QuotePacket {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

impl PartialEq for QuotePacket {
    fn eq(&self, other: &Self) -> bool {
        self.time_stamp == other.time_stamp
    }
}

impl Eq for QuotePacket {}

impl fmt::Display for QuotePacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use fmt::Write;
        write!(f, "{} {} ", self.time_stamp, self.quote_accept_time)?;
        for &c in self.issue_code.iter() {
            f.write_char(c as char)?;
        }
        for &(quantity, price) in self.bids.iter().rev() {
            write!(f, " {}@{}", quantity, price)?;
        }
        for &(quantity, price) in self.asks.iter() {
            write!(f, " {}@{}", quantity, price)?;
        }
        Ok(())
    }
}

fn read_u32(file: &mut File) -> Result<u32, io::Error> {
    let mut buf = [0; 4];
    file.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_bids(file: &mut File, bids: &mut [(u32, u32); 5]) -> Result<(), Box<dyn Error>> {
    let mut buf = [0; PRICE_OFFSET + QUANTITY_OFFSET];
    for (quantity, price) in bids {
        file.read_exact(&mut buf)?;
        let str_buf = str::from_utf8(&buf)?;
        *price = str_buf[0..PRICE_OFFSET].parse()?;
        *quantity = str_buf[PRICE_OFFSET..].parse()?;
    }
    Ok(())
}

fn read_quote_accept_time(
    file: &mut File,
    time_stamp: i64,
) -> Result<NaiveDateTime, Box<dyn Error>> {
    let mut buf = [0; QUOTE_ACCEPT_SIZE];
    file.read_exact(&mut buf)?;
    let str_buf = str::from_utf8(&buf)?;
    let seconds = str_buf[0..2].parse::<i64>()? * 3_600
        + str_buf[2..4].parse::<i64>()? * 60
        + str_buf[4..6].parse::<i64>()?;
    let nanoseconds = str_buf[7..8].parse::<u32>()? * 1_000_000;
    let remainder = time_stamp % SECONDS_IN_A_DAY;
    let difference = (SECONDS_IN_A_DAY - KST_OFFSET + seconds) % SECONDS_IN_A_DAY - remainder;
    NaiveDateTime::from_timestamp_opt(
        if difference.abs() > MAX_DIFF {
            if difference < 0 {
                time_stamp + difference + SECONDS_IN_A_DAY
            } else {
                time_stamp + difference - SECONDS_IN_A_DAY
            }
        } else {
            time_stamp + difference
        },
        nanoseconds,
    )
    .ok_or(INVALID_TIMESTAMP.into())
}

enum Parser {
    Valid(QuotePacket),
    Invalid,
    EOF,
}

fn parse_packet(file: &mut File) -> Result<Parser, Box<dyn Error>> {
    let seconds = match read_u32(file) {
        Ok(seconds) => seconds as i64,
        Err(e) => {
            return if e.kind() == ErrorKind::UnexpectedEof {
                Ok(Parser::EOF)
            } else {
                Err(e.into())
            }
        }
    };
    let date = NaiveDateTime::from_timestamp_opt(seconds, read_u32(file)? * 1_000)
        .ok_or(INVALID_TIMESTAMP)?;
    let packet_size = read_u32(file)? as i64 + 4;
    if packet_size != QUOTE_PACKET_SIZE + QUOTE_PACKET_OFFSET {
        file.seek(SeekFrom::Current(packet_size))?;
        return Ok(Parser::Invalid);
    }
    file.seek(SeekFrom::Current(QUOTE_PACKET_OFFSET))?;
    let mut buf = [0; 5];
    file.read_exact(&mut buf)?;
    if buf != QUOTE_PACKET_HEADER {
        file.seek(SeekFrom::Current(QUOTE_PACKET_SIZE - 5))?;
        return Ok(Parser::Invalid);
    }
    let mut quote_packet: QuotePacket = QuotePacket {
        time_stamp: date,
        quote_accept_time: date,
        issue_code: Default::default(),
        bids: Default::default(),
        asks: Default::default(),
    };
    file.read_exact(&mut quote_packet.issue_code)?;
    file.seek(SeekFrom::Current(BIDS_OFFSET))?;
    read_bids(file, &mut quote_packet.bids)?;
    file.seek(SeekFrom::Current(QUANTITY_OFFSET as i64))?;
    read_bids(file, &mut quote_packet.asks)?;
    file.seek(SeekFrom::Current(QUOTE_ACCEPT_OFFSET as i64))?;
    quote_packet.quote_accept_time = read_quote_accept_time(file, seconds)?;
    file.seek(SeekFrom::Current(1))?;
    Ok(Parser::Valid(quote_packet))
}

fn parse_file(path: &str) -> Result<(), Box<dyn Error>> {
    let file = &mut File::open(path)?;
    file.seek(SeekFrom::Start(GLOBAL_HEADER_SIZE))?;
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    loop {
        match parse_packet(file)? {
            Parser::Valid(q) => writeln!(handle, "{}", q)?,
            Parser::EOF => break,
            _ => {}
        }
    }
    Ok(())
}

fn parse_reorder(path: &str) -> Result<(), Box<dyn Error>> {
    let mut min_heap = BinaryHeap::new();
    let file = &mut File::open(path)?;
    file.seek(SeekFrom::Start(GLOBAL_HEADER_SIZE))?;
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    loop {
        match parse_packet(file)? {
            Parser::Valid(q) => {
                let curr_time = q.time_stamp;
                min_heap.push(q);
                while let Some(true) = min_heap.peek().map(|q| {
                    curr_time.timestamp_nanos() - q.quote_accept_time.timestamp_nanos()
                        > MAX_DIFF * 1_000_000_000
                }) {
                    writeln!(handle, "{}", min_heap.pop().unwrap())?;
                }
            }
            Parser::EOF => break,
            _ => {}
        }
    }
    for q in min_heap.into_sorted_vec().iter().rev() {
        writeln!(handle, "{}", q)?;
    }
    Ok(())
}

fn main() {
    let mut args = env::args().skip(1);
    match (
        args.next().as_ref().map(String::as_str),
        args.next().as_ref(),
    ) {
        (Some(path), None) => parse_file(path),
        (Some("-r"), Some(path)) => parse_reorder(path),
        _ => {
            eprintln!("Usage: parse-quote [-r] filename");
            process::exit(1);
        }
    }
    .unwrap_or_else(|e| {
        eprintln!("Error: {}", e);
        process::exit(1);
    });
}
