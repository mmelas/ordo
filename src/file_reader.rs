use crate::process;
use crate::fifo;
use std::sync::{Mutex};
use std::io::{self, BufRead};
use std::path::Path;
use std::fs::File;
use std::cmp;

// (operator) read a file and wrie to its output queue
// each line as a String

pub struct FileReader {
    pub inputs: *mut fifo::Queue<String>,
    pub outputs: *mut fifo::Queue<String>,
    pub fds: Mutex<Vec<String>>,
}

unsafe impl Send for FileReader {}
unsafe impl Sync for FileReader {}

impl FileReader {
    pub fn new(ins : *mut fifo::Queue<String>, outs : *mut fifo::Queue<String>) -> FileReader {
        FileReader {inputs: ins, outputs: outs, fds: Mutex::new(Vec::new())}
    }

    pub fn add_file(&mut self, f_name : String) {
        self.fds.lock().unwrap().push(f_name);
    }
}

impl process::Process for FileReader {
    fn activation(&self) -> i64 {
        return self.fds.lock().unwrap().len() as i64;
    }    

    fn activate(&self, batch_size : i64) {
        let mut line_count = 0;
        let f_name = self.fds.lock().unwrap().pop().unwrap();
        let f_name_c = f_name.clone();
        if let Ok(lines) = read_lines(f_name) {
            line_count = lines.count();
        }

        let mut rem = line_count;
        if let Ok(mut lines) = read_lines(f_name_c) {
            // Consumes the iterator, returns an (Optional) String
            while rem > 0 {
                let write_cnt = cmp::min(rem, batch_size as usize);
                let mut ws = unsafe{(*self.outputs).reserve(write_cnt).unwrap()};
                rem -= write_cnt;
                for _ in 0..write_cnt {
                    let next_line = lines.next().unwrap().unwrap();
                    unsafe{ws.update(next_line)};
                }
                unsafe{ws.commit()};
            }
        }
    }
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
