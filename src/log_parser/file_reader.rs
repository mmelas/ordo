use crate::process;
use crate::fifo;
use std::sync::{Mutex};
use std::io::{self, BufRead, SeekFrom};
use std::path::Path;
use std::fs::File;
use std::{cmp, mem};
use std::io::prelude::*;
use crate::metrics::Metrics;
use std::time::Duration;

// (operator) read a file and write to its (operator's)
// output queue each line as a String

const WEIGHT : f64 = 1.00000;

pub struct FileReader {
    id : usize,
    pub inputs: *mut fifo::Queue<Option<String>>,
    pub outputs: *mut fifo::Queue<Option<String>>,
    pub lines: Mutex<Vec<(io::BufReader<File>, u64)>>,
    metrics: *mut Metrics<'static>
}

unsafe impl Send for FileReader {}
unsafe impl Sync for FileReader {}

impl FileReader {
    pub fn new() {

    }

    pub fn new_with_vector(
        id : usize,
        ins : *mut fifo::Queue<Option<String>>, 
        outs : *mut fifo::Queue<Option<String>>, 
        files : Vec<String>,
        metrics : *mut Metrics<'static>
    ) -> FileReader {
        let mut buf_readers = Vec::new();
        for f in files {
            let file = File::open(f).unwrap();
            let file_size = file.metadata().unwrap().len();
            let buf_reader = io::BufReader::new(file);
            buf_readers.push((buf_reader, file_size));
        } 
        FileReader {
            id : id, inputs: ins, outputs: outs, 
            lines: Mutex::new(buf_readers),
            metrics: metrics
        }
    }

    pub fn new_with_single(
        id : usize,
        ins : *mut fifo::Queue<Option<String>>, 
        outs : *mut fifo::Queue<Option<String>>, 
        f_name : String, partitions : i64,
        metrics : *mut Metrics<'static>
    ) -> FileReader {
        let lines = Mutex::new(Vec::new());

        let file = File::open(&f_name).unwrap();
        let file_size = file.metadata().unwrap().len();
        drop(file);
        let sep = (file_size / partitions as u64) as i64;

        let mut prev_idx = 0;
        for p in 1..partitions + 1 {
            let mut next_br;
            let os = p*sep;
            let file = File::open(&f_name).unwrap();
            next_br = FileReader::get_next_br(file, os);
            let upper_bound = next_br.seek(SeekFrom::Current(0)).unwrap();
            let _ = next_br.seek(SeekFrom::Start(prev_idx));
            lines.lock().unwrap().push(
                (next_br, upper_bound)
            ); 
            prev_idx = upper_bound;
        }
        FileReader {id : id, inputs: ins, outputs: outs, lines: lines, metrics : metrics}
    }

    fn get_next_br(mut f : File, os : i64) -> io::BufReader<File> {
        let _ = f.seek(SeekFrom::Start(os as u64));
        let mut br = io::BufReader::new(f);
        let mut bytes = br.by_ref().bytes();

        loop { 
            match bytes.next() {
                //10 is new line in ASCII
                Some(byte) => if byte.unwrap() == 10 {
                    break;
                },
                None => break,
            }
        }
        return br;
    }

}

impl process::Process for FileReader {
    fn activation(&self) -> i64 {
        return self.lines.lock().unwrap().len() as i64;
    }    

    fn activate(&self, mut batch_size : i64) {
        batch_size = (batch_size as f64 * WEIGHT) as i64;
        let lines = self.lines.lock().unwrap().pop();
        let (mut buf_reader, upper_bound) = match lines {
            Some(x) => x,
            None => return
        };

        // Read lines of current bufreader
        let mut current_pos = buf_reader.seek(SeekFrom::Current(0)).unwrap();

        let mut ws;
        loop {
            ws = unsafe {
                (*self.outputs).reserve(batch_size as usize)
            };
            if ws.is_some() {
                break;
            }
            println!("HIHI fr");
        }
        let mut wslice = ws.unwrap();

        let mut total_lines = 0;
        while batch_size > 0 && current_pos < upper_bound {
            let mut next_line = String::new();
            batch_size -= 1;
            current_pos += buf_reader.read_line(&mut next_line).unwrap() as u64;
            //if total_lines % 4 == 0 {
            unsafe{wslice.update(Some(next_line))}
            //    next_line = String::new();
           // }
            total_lines += 1;
        } 
//        unsafe{(*self.metrics).proc_metrics[self.id].update(total_lines, total_lines)}
        unsafe{(*self.metrics).proc_metrics[self.id].update(total_lines, total_lines)}

        unsafe{wslice.commit()};

        if buf_reader.seek(SeekFrom::Current(0)).unwrap() < upper_bound {
            self.lines.lock().unwrap().push((buf_reader, upper_bound));
        }
    }

//    fn activate(&self, mut batch_size : i64) {
//        let lines = self.lines.lock().unwrap().pop();
//        let tuple = match lines {
//            Some(x) => x,
//            None => return
//        };
//        let (mut buf_reader, upper_bound) = tuple;
//
//        let mut current_pos = buf_reader.seek(SeekFrom::Current(0)).unwrap();
//        loop {
//            let ws = unsafe {
//                (*self.outputs).reserve(batch_size as usize)
//            };
//
//            match ws {
//                Some(mut x) => {
//                    // Read lines of current bufreader
//                    while batch_size > 0 && 
//                          current_pos < upper_bound {
//                        batch_size -= 1;
//                        let mut next_line = "".to_owned();
//                        current_pos += buf_reader.read_line(&mut next_line).unwrap() as u64;
//                        unsafe{x.update(next_line);}
//                    } 
//                    unsafe{x.commit()};
//                    break;
//                },
//                None => {}
//            }
//        }
//
//        if current_pos < upper_bound {
//            self.lines.lock().unwrap().push((buf_reader, upper_bound));
//        }
//    }
}
