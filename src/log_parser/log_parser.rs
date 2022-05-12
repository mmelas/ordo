use crate::{process, fifo};
use std::sync::Mutex;
use crate::log_parser::file_reader;
use crate::log_parser::apply_regex;
use crate::log_parser::output_results;
use crate::log_parser::split_string;
use crate::log_parser::clean_queue;
use crate::metrics;
use crate::metric::Metric;
use std::sync::Arc;
use std::fs::File;
use std::time::{Instant, Duration};
use std::thread;
use crate::params;

const PRODUCERS : i64 = params::PRODUCERS;

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

    let metrics = Box::leak(Box::new(metrics::Metrics{..Default::default()}));

    let m_0 = Box::leak(Box::new(Metric::new(0)));
    let m_1 = Box::leak(Box::new(Metric::new(1)));
    let m_2 = Box::leak(Box::new(Metric::new(2)));
    let m_3 = Box::leak(Box::new(Metric::new(3)));
    metrics.add_metric(m_0);
    metrics.add_metric(m_1);
    metrics.add_metric(m_2);
    metrics.add_metric(m_3);

 //   let metrics_arc = Arc::new(metrics);
//    let p1 = file_reader::FileReader::new_with_vector(q, q, fds);
    let p1 = file_reader::FileReader::new_with_single(0, q, q, "../c_texts.txt".to_owned(), PRODUCERS, metrics);
//    let metrics_c = metrics_arc.clone();
//    let metrics_c2 = metrics_arc.clone();

    let p2 = split_string::SplitString::new(1, q, q2, metrics);

    let p3 = apply_regex::AppRegex::new(2, q2, q3, metrics);

    let p4 = output_results::Output::new(3, q3, q3, metrics);

//    let p5 = clean_queue::CleanQueue::new(4, q2, q2, metrics);
//
//    let p6 = clean_queue::CleanQueue::new(5, q, q, metrics);

    let pr = Box::leak(Box::new(process::ProcessRunner::new(metrics)));
    pr.add_process(Box::leak(Box::new(p1)));
    pr.add_process(Box::leak(Box::new(p2)));
    pr.add_process(Box::leak(Box::new(p3)));
    pr.add_process(Box::leak(Box::new(p4)));
//    pr.add_process(Box::leak(Box::new(p5)));
//    pr.add_process(Box::leak(Box::new(p6)));
    pr.start();

    loop {
        thread::sleep(Duration::from_millis(500));
        metrics.print_metrics();
    }
}
