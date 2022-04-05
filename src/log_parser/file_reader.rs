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
    pub lines: Mutex<Vec<(io::BufReader<File>, u64)>>,
}

unsafe impl Send for FileReader {}
unsafe impl Sync for FileReader {}

impl FileReader {
    pub fn new() {

    }

    pub fn new_with_vector(
        ins : *mut fifo::Queue<String>, 
        outs : *mut fifo::Queue<String>, 
        files : Vec<String>
    ) -> FileReader {
        let mut buf_readers = Vec::new();
        for f in files {
            let file = File::open(f).unwrap();
            let file_size = file.metadata().unwrap().len();
            let buf_reader = io::BufReader::new(file);
            buf_readers.push((buf_reader, file_size));
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
        FileReader {inputs: ins, outputs: outs, lines: lines}
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
        bytes.next(); //skip new line byte
        return br;
    }

}

impl process::Process for FileReader {
    fn activation(&self) -> i64 {
        return self.lines.lock().unwrap().len() as i64;
    }    

    fn activate(&self, mut batch_size : i64) {
        let lines = self.lines.lock().unwrap().pop();
        let tuple = match lines {
            Some(x) => x,
            None => return
        };
        let (mut buf_reader, upper_bound) = tuple;
        let mut vec_lines = Vec::new();

        // Read lines of current bufreader
        let mut current_pos = buf_reader.seek(SeekFrom::Current(0)).unwrap();
        let mut next_line;
        while batch_size > 0 && current_pos < upper_bound {
            batch_size -= 1;
            next_line = "".to_owned();
            current_pos += buf_reader.read_line(&mut next_line).unwrap() as u64;
            vec_lines.push(next_line);
        } 
        

        let mut ws;
        loop {
            ws = unsafe {
                (*self.outputs).reserve(vec_lines.len())
            };
            if ws.is_some() {
                break;
            }
        }
        let mut wslice = ws.unwrap();
        for line in vec_lines {
            unsafe{wslice.update(line)};
        }
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
