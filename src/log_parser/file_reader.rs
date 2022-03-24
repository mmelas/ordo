use crate::process;
use crate::fifo;
use std::sync::{Mutex};
use std::io::{self, BufRead, SeekFrom};
use std::path::Path;
use std::fs::File;
use std::{cmp, mem};
use std::io::prelude::*;

// (operator) read a file and wrie to its output queue
// each line as a String

pub struct FileReader {
    pub inputs: *mut fifo::Queue<String>,
    pub outputs: *mut fifo::Queue<String>,
    pub lines: Mutex<Vec<(io::Lines<io::BufReader<File>>, i64)>>,
}

unsafe impl Send for FileReader {}
unsafe impl Sync for FileReader {}

impl FileReader {
    pub fn new() {

    }

    pub fn new_with_vector(
        ins : *mut fifo::Queue<String>, 
        outs : *mut fifo::Queue<String>, 
        lines : Vec<String>
    ) -> FileReader {
        let mut buf_readers = Vec::new();
        for f in lines {
            let file = File::open(f).unwrap();
            let mut file_c = file.try_clone().unwrap();
            let buf_reader = io::BufReader::new(file).lines();
            let line_cnt = FileReader::count_file_lines(&file_c);
            let _ = file_c.rewind();
            buf_readers.push((buf_reader, line_cnt as i64));
        } 
        FileReader {
            inputs: ins, outputs: outs, 
            lines: Mutex::new(buf_readers)
        }
    }

    pub fn new_with_single(
        ins : *mut fifo::Queue<String>, 
        outs : *mut fifo::Queue<String>, 
        f_name : String, partitions : i64
    ) -> FileReader {
        let lines = Mutex::new(Vec::new());

        let file = File::open(&f_name).unwrap();
        let line_count = FileReader::count_file_lines(&file);
        let sep = (line_count / partitions as usize) as i64;
        let rem = (line_count % partitions as usize) as i64;

        for p in 0..partitions {
            let file = File::open(&f_name).unwrap();
            let mut batch_lines = io::BufReader::new(file).lines();
            for _ in 0..p * sep as i64 {
                batch_lines.next();
            }
            // use rem if we are on last slice because it might include one more line
            lines.lock().unwrap().push(
                (batch_lines, sep + rem * (p == partitions - 1) as i64)
            ); 
        }
        FileReader {inputs: ins, outputs: outs, lines: lines}
    }

    fn count_file_bytes(f : &File) -> usize {
        let bytes = io::BufReader::new(f).bytes(); 
        bytes.count()
    }

    fn count_file_lines(f : &File) -> usize {
        let lines = io::BufReader::new(f).lines(); 
        lines.count()
    }

}

impl process::Process for FileReader {
    fn activation(&self) -> i64 {
        return self.lines.lock().unwrap().len() as i64;
    }    

//    fn activate(&self, batch_size : i64) {
//        let mut line_count = 0;
//        let file = self.lines.lock().unwrap().pop().unwrap();
//        let file_c = file.try_clone().unwrap();
//        let mut file_c2 = file.try_clone().unwrap();
//        let cur_pos = file_c2.seek(SeekFrom::Current(0)).unwrap();
//        if let Ok(lines) = read_lines(file) {
//            line_count = lines.count();
//        }
//        let _ = file_c2.seek(SeekFrom::Start(cur_pos));
////        println!("lines {}", line_count);
//
//        let mut lines = io::BufReader::new(file_c); 
//
//        let write_cnt = cmp::min(line_count, batch_size as usize);
//        let mut ws = unsafe{(*self.outputs).reserve(write_cnt).unwrap()};
//        let mut bytes_read = 0;
//        for _ in 0..write_cnt {
//            let mut next_line = String::new();
//            let next_line_bytes = lines.read_line(&mut next_line).unwrap();
//            bytes_read += next_line_bytes;
//            unsafe{ws.update(next_line)};
//        }
//        unsafe{ws.commit()};
//        let _ = file_c2.seek(SeekFrom::Start(bytes_read as u64));
//        if write_cnt != line_count {
//            self.lines.lock().unwrap().push(file_c2);
//        }
//    }

    // TODO:: maybe introduce a variable to 
    // append more than one line on each Queue cell
    fn activate(&self, batch_size : i64) {
        let (mut lines, mut cnt) = self.lines.lock().unwrap().pop().unwrap();
        let write_amount = cmp::min(batch_size, cnt as i64);
        let mut ws = unsafe{
            (*self.outputs).reserve(write_amount as usize).unwrap()
        };

        for _ in 0..write_amount {
            let next_line = lines.next().unwrap().unwrap();
            unsafe{ws.update(next_line)};
        }
        unsafe{ws.commit()};

        cnt -= batch_size;
        if cnt > 0 {
            self.lines.lock().unwrap().push((lines, cnt));
        }
    }
}

//fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
//where P: AsRef<Path>, {
//    let file = File::open(filename)?;
//    Ok(io::BufReader::new(file).lines())
//}

fn read_lines(file: File) -> io::Result<io::Lines<io::BufReader<File>>> {
    Ok(io::BufReader::new(file).lines())
}
