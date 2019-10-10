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
use Endianness::*;
use Parser::*;
use Precision::*;

const INVALID_INPUT: &str = "Invalid file format";
const INVALID_TIMESTAMP: &str = "Invalid timestamp format";
const HEADER_TIME_ZONE_OFFSET: i64 = 4;
const HEADER_END_OFFSET: i64 = 12;
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
const QUOTE_PACKET_HEADER: &[u8; 5] = b"B6034";

#[derive(Eq, PartialEq)]
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
        Some(self.cmp(other))
    }
}

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

#[derive(Copy, Clone)]
enum Endianness {
    LittleEndian,
    BigEndian,
}

#[derive(Copy, Clone)]
enum Precision {
    Microsecond = 1_000,
    Nanosecond = 1,
}

fn read_u32(file: &mut File, end: Endianness) -> Result<u32, io::Error> {
    let mut buf = [0; 4];
    file.read_exact(&mut buf)?;
    Ok(match end {
        LittleEndian => u32::from_le_bytes(buf),
        BigEndian => u32::from_be_bytes(buf),
    })
}

fn parse_header(file: &mut File) -> Result<(Endianness, Precision, i64), Box<dyn Error>> {
    let mut buf = [0; 4];
    file.read_exact(&mut buf)?;
    let (end, precision) = match buf {
        [0xD4, 0xC3, 0xB2, 0xA1] => (LittleEndian, Microsecond),
        [0xA1, 0xB2, 0xC3, 0xD4] => (BigEndian, Microsecond),
        [0x4D, 0x3C, 0xB2, 0xA1] => (LittleEndian, Nanosecond),
        [0xA1, 0xB2, 0x3C, 0x4D] => (BigEndian, Nanosecond),
        _ => return Err(INVALID_INPUT.into()),
    };
    file.seek(SeekFrom::Current(HEADER_TIME_ZONE_OFFSET))?;
    let this_zone = i64::from(read_u32(file, end)?);
    file.seek(SeekFrom::Current(HEADER_END_OFFSET))?;
    Ok((end, precision, this_zone))
}

fn parse_bids_or_asks(file: &mut File, bids: &mut [(u32, u32); 5]) -> Result<(), Box<dyn Error>> {
    let mut buf = [0; PRICE_OFFSET + QUANTITY_OFFSET];
    for (quantity, price) in bids {
        file.read_exact(&mut buf)?;
        let str_buf = str::from_utf8(&buf)?;
        *price = str_buf[0..PRICE_OFFSET].parse()?;
        *quantity = str_buf[PRICE_OFFSET..].parse()?;
    }
    Ok(())
}

fn parse_quote_accept_time(
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
    // We converted the timestamp to UTC, while the market feed data is in KST. We'll also convert
    // it to UTC and calculate the date accounting for the subtle difference in time that leads to
    // a few edge cases when for instance the quote accept time is 2011-02-16 8:59:59 and the
    // timestamp is 2011-02-16 0:00:00 leading to the date warping to 2011-02-15 23:59:59.
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
    .ok_or_else(|| INVALID_TIMESTAMP.into())
}

enum Parser {
    Valid(QuotePacket),
    Invalid,
    EOF,
}

fn parse_packet(
    file: &mut File,
    end: Endianness,
    precision: Precision,
    this_zone: i64,
) -> Result<Parser, Box<dyn Error>> {
    let seconds = match read_u32(file, end) {
        // Converting the packet timestamp to UTC
        Ok(seconds) => i64::from(seconds) + this_zone,
        Err(e) => {
            return if e.kind() == ErrorKind::UnexpectedEof {
                Ok(EOF)
            } else {
                Err(e.into())
            };
        }
    };
    let date = NaiveDateTime::from_timestamp_opt(seconds, read_u32(file, end)? * precision as u32)
        .ok_or(INVALID_TIMESTAMP)?;
    let packet_size = i64::from(read_u32(file, end)?) + 4;
    if packet_size != QUOTE_PACKET_SIZE + QUOTE_PACKET_OFFSET {
        file.seek(SeekFrom::Current(packet_size))?;
        return Ok(Invalid);
    }
    file.seek(SeekFrom::Current(QUOTE_PACKET_OFFSET))?;
    let mut buf = [0; 5];
    file.read_exact(&mut buf)?;
    if !buf.eq(QUOTE_PACKET_HEADER) {
        file.seek(SeekFrom::Current(QUOTE_PACKET_SIZE - 5))?;
        return Ok(Invalid);
    }
    let mut quote_packet: QuotePacket = QuotePacket {
        time_stamp: date,
        quote_accept_time: date,
        issue_code: Default::default(),
        bids: Default::default(),
        asks: Default::default(),
    };
    file.read_exact(&mut quote_packet.issue_code)?;
    // Check that the issue code is valid UTF-8 for when we print it later.
    str::from_utf8(&quote_packet.issue_code)?;
    file.seek(SeekFrom::Current(BIDS_OFFSET))?;
    parse_bids_or_asks(file, &mut quote_packet.bids)?;
    file.seek(SeekFrom::Current(QUANTITY_OFFSET as i64))?;
    parse_bids_or_asks(file, &mut quote_packet.asks)?;
    file.seek(SeekFrom::Current(QUOTE_ACCEPT_OFFSET as i64))?;
    quote_packet.quote_accept_time = parse_quote_accept_time(file, seconds)?;
    file.seek(SeekFrom::Current(1))?;
    Ok(Valid(quote_packet))
}

fn parse_file(path: &str) -> Result<(), Box<dyn Error>> {
    let file = &mut File::open(path)?;
    let (end, precision, this_zone) = parse_header(file)?;
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    loop {
        match parse_packet(file, end, precision, this_zone)? {
            Valid(quote_packet) => writeln!(handle, "{}", quote_packet)?,
            EOF => break,
            Invalid => continue,
        }
    }
    Ok(())
}

fn parse_reorder(path: &str) -> Result<(), Box<dyn Error>> {
    let mut min_heap: BinaryHeap<QuotePacket> = BinaryHeap::new();
    let file = &mut File::open(path)?;
    let (end, precision, this_zone) = parse_header(file)?;
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    loop {
        match parse_packet(file, end, precision, this_zone)? {
            Valid(quote_packet) => {
                // Instead of filling up the heap with all the quote packets before printing them
                // for a possibly expensive O(n) space and O(n*log(n)) time complexity where
                // n = number of quote packets, we only keep track of the last 3 seconds of trading
                // since our quote packets are already sorted by ascending order of timestamps
                // and the difference between the latest timestamp and the earliest quote accept
                // time can never exceed 3 seconds. This gives us O(k) space and O(n*log(k)) time
                // complexity where k = number of quote packets that arrived in the last 3 seconds.
                while min_heap.peek().map_or(false, |top| {
                    quote_packet.time_stamp.timestamp_nanos()
                        - top.quote_accept_time.timestamp_nanos()
                        > MAX_DIFF * 1_000_000_000
                }) {
                    writeln!(handle, "{}", min_heap.pop().unwrap())?;
                }
                min_heap.push(quote_packet);
            }
            EOF => break,
            Invalid => continue,
        }
    }
    for quote_packet in min_heap.into_sorted_vec().iter().rev() {
        writeln!(handle, "{}", quote_packet)?;
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
            eprintln!("Usage: parse-quote [-r] <filename>");
            process::exit(1);
        }
    }
    .unwrap_or_else(|e| {
        eprintln!("Error: {}", e);
        process::exit(1);
    });
}
