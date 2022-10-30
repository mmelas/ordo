use crate::process;
use crate::fifo;
use std::sync::{Arc, Mutex, RwLock};
use std::io::{self, BufRead, SeekFrom};
use std::path::Path;
use std::fs::File;
use std::io::prelude::*;
use crate::metrics::Metrics;
use std::time::Duration;
use crate::params;
use std::sync::atomic::Ordering;

// (operator) read a file and write to its (operator's)
// output queue each line as a String

const WEIGHT : f64 = 0.10000;

pub struct FileReader {
    id : usize,
    target : RwLock<i64>,
    pub inputs: *mut fifo::Queue<Option<Arc<Vec<u8>>>>,
    pub outputs: *mut fifo::Queue<Option<Arc<Vec<u8>>>>,
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
        ins : *mut fifo::Queue<Option<Arc<Vec<u8>>>>,
        outs : *mut fifo::Queue<Option<Arc<Vec<u8>>>>,
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
            id : id, target : RwLock::new(300), inputs: ins, outputs: outs, 
            lines: Mutex::new(buf_readers),
            metrics: metrics
        }
    }

    pub fn new_with_single(
        id : usize,
        ins : *mut fifo::Queue<Option<Arc<Vec<u8>>>>,
        outs : *mut fifo::Queue<Option<Arc<Vec<u8>>>>,
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
            let os = p * sep;
            let file = File::open(&f_name).unwrap();
            next_br = FileReader::get_next_br(file, os);
            let upper_bound = next_br.seek(SeekFrom::Current(0)).unwrap();
            let _ = next_br.seek(SeekFrom::Start(prev_idx));
            lines.lock().unwrap().push(
                (next_br, upper_bound)
            ); 
            prev_idx = upper_bound;
        }
        lines.lock().unwrap().reverse();
        FileReader {id : id, target : RwLock::new(300), inputs: ins, outputs: outs, lines: lines, metrics : metrics}
    }

    fn get_next_br(mut f : File, os : i64) -> io::BufReader<File> {
        let _ = f.seek(SeekFrom::Start(os as u64));
        let mut br = io::BufReader::with_capacity(600_000, f);
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
    
    fn boost(&self) -> i64 {
        //return 9999999999999;
        if self.get_target() == 0 {
            return 0;
        }
        self.activation() * *self.target.read().unwrap()
    }

    fn get_pid(&self) -> usize {
        self.id
    }

    fn set_target(&self, target : i64) {
        *self.target.write().unwrap() = target;
    }

    fn get_target(&self) -> i64 {
        let tar = *self.target.read().unwrap();
        if tar > 2000 {
            return 1000;
        }
        if self.activation() == 0 {
            return 0;
        }
        return tar;
    }

    fn activate(&self, mut batch_size : i64, thread_id : i64) {
        let lines = self.lines.lock().unwrap().pop();
        let (mut buf_reader, upper_bound) = match lines {
            Some(x) => x,
            None => {
		println!("HEI");
		return
	    }
        };

        // Read lines of current bufreader
        let mut current_pos = buf_reader.seek(SeekFrom::Current(0)).unwrap();

        let mut ws;
        let mut t0 = std::time::Instant::now();
        
        loop {
            ws = unsafe {
                if batch_size == 0 {
                    println!("ERROR : Batch Size is 0 on FileReader!");
                }
                (*self.outputs).reserve(batch_size as usize)
            };
            if ws.is_some() {
                break;
            }
            println!("HIHI fr, {}", self.get_target());
        }
        let mut t1 = t0.elapsed().as_nanos();
        unsafe{(*self.metrics).update_reserve_time(t1 as u64);}
        let mut wslice = ws.unwrap();

        t0 = std::time::Instant::now();
        let mut temp_batch_size = batch_size;
        while temp_batch_size > 0 && current_pos < upper_bound {
            let mut next_line = vec![];
            temp_batch_size -= 1;
            let mut bytes_read = 0;
            while current_pos < upper_bound && bytes_read < 2048 {
                let line_bytes = buf_reader.read_until(b'\n', &mut next_line).unwrap() as u64;
                bytes_read += line_bytes;
                current_pos += line_bytes;
            }
            unsafe{wslice.update(Some(Arc::new(next_line)))}
        } 
        t1 = t0.elapsed().as_nanos();
        //unsafe{(*self.metrics).update_read_time(t1 as u64);}

        unsafe{(*self.metrics).proc_metrics[self.id].update(batch_size, batch_size)}

        t0 = std::time::Instant::now();
        unsafe{wslice.commit()};
        t1 = t0.elapsed().as_nanos();
        //unsafe{(*self.metrics).update_commit_time(t1 as u64);}

        if buf_reader.seek(SeekFrom::Current(0)).unwrap() < upper_bound {
            self.lines.lock().unwrap().push((buf_reader, upper_bound));
        }
    }
}
