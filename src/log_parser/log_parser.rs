use crate::{process, fifo};
use std::sync::Mutex;
use crate::log_parser::file_reader;
use crate::log_parser::apply_regex;
use crate::log_parser::output_results;
use crate::log_parser::split_string;
use crate::metrics;
use std::sync::Arc;
use std::fs::File;


// NewType design in order to make
// raw pointer Send + Sync
//struct SendPtr<T> (*mut T);
//impl<T> SendPtr<T> {
//    pub fn get(self) -> *mut T {
//        return self.0;
//    }
//}
//unsafe impl<T> Send for SendPtr<T> {}
//unsafe impl<T> Sync for SendPtr<T> {}
//impl<T> Clone for SendPtr<T> {
//    fn clone(&self) -> Self { *self }
//}
//impl<T> Copy for SendPtr<T> {}

//pub fn read_file(iq : *mut fifo::Queue<String>, oq : *mut fifo::Queue<String>) {
//
//    let f_name = "test.txt";
//    let file = File::open(f_name).unwrap();
//    let map = UnsafeCell::new(unsafe{Mmap::map(&file).unwrap()});
//   
//    let file_size = unsafe{(*map.get()).len()};
//
//    let ptr = map.get();
//    
//    let to_split = unsafe{str::from_utf8(&(*ptr)[0..file_size])}.unwrap();
//    let splitted = to_split.split('\n').collect::<Vec<&str>>();
//    let total_lines = splitted.len() - 1;
////    println!("{}", total_lines);
//    let mut chunks : Vec<Vec<&str>> = Vec::new();
////    println!("{}", (total_lines / PRODUCERS) + 1);
//    let lines_per = if total_lines % PRODUCERS == 0 {total_lines / PRODUCERS} 
//                        else {(total_lines / PRODUCERS) + 1};
//    for chunk in splitted.chunks(lines_per) {
//        chunks.push(chunk.to_owned());
//    }
//
//    let mut threads = Vec::with_capacity(PRODUCERS);
//
//    for chunk in chunks {
//        let poq = SendPtr(oq);
//        // MUST RESERVE WRITE_SLICE_S slice size
//        let mut ws = unsafe{(*poq.get()).reserve(lines_per).unwrap()};
//        threads.push(thread::spawn(move || {
//            for line in chunk {
//                // to_owned will require copying under the hood, is it
//                // good if we work with str references instead?
//                unsafe{ws.update(line.to_owned())};
//            }
//            unsafe{ws.commit()};
//        }));
//    }
//
//    for th in threads {
//        let _ = th.join();
//    }
//}

pub fn run() {
    let pr = Box::leak(Box::new(process::ProcessRunner::new()));
    let q = Box::leak(Box::new(fifo::Queue{..Default::default()}));
    let q2 = Box::leak(Box::new(fifo::Queue{..Default::default()}));
    let q3 = Box::leak(Box::new(fifo::Queue{..Default::default()}));

    let f1 = "test0.txt".to_owned();
    let f2 = "test1.txt".to_owned();
    let f3 = "test2.txt".to_owned();
    let f4 = "test3.txt".to_owned();
    let f5 = "test4.txt".to_owned();
    let f6 = "test5.txt".to_owned();
    let fds = vec![f1, f2, f3, f4, f5, f6];

    let metrics = Arc::new(Box::leak(Box::new(metrics::Metrics{..Default::default()})));
//    let p1 = file_reader::FileReader::new_with_vector(q, q, fds);
    let p1 = file_reader::FileReader::new_with_single(q, q, "combined_texts.txt".to_owned(), 50);
    let metrics_c = metrics.clone();

    let p2 = split_string::SplitString::new(q, q2);

    let p3 = apply_regex::AppRegex::new(q2, q3, metrics);

    let p4 = output_results::Output::new(q3, q3, metrics_c);


    pr.add_process(Box::new(p1));
    pr.add_process(Box::new(p2));
    pr.add_process(Box::new(p3));
    pr.add_process(Box::new(p4));
    pr.start();

    loop {}
}
