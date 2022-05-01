use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::collections::binary_heap::BinaryHeap;
use std::collections::HashMap;
use std::time::{Instant, Duration};
use crate::params;
use crate::metric::Metric;


pub struct Metrics<'a> {
    pub start_time : Instant,
    pub items_read : AtomicI64,
    pub hashtags_read : AtomicI64,
    pub splits_time : Mutex<Duration>,
    pub proc_throughput : Mutex<BinaryHeap<Metric>>,
    pub proc_metrics : Vec<&'a mut Metric>,
}

impl<'a> Default for Metrics<'a> {
    fn default() -> Self {
        Metrics {
            start_time : Instant::now(),
            items_read : AtomicI64::new(0),
            hashtags_read : AtomicI64::new(0),
            proc_throughput : Mutex::new(BinaryHeap::new()),
            splits_time : Mutex::new(Duration::new(0, 0)),
            proc_metrics : vec![]
        }
    }
}

impl<'a> Metrics<'a> {
    pub fn incr_hashtags(&self, amount : i64) {
        if self.hashtags_read.fetch_add(amount, Ordering::SeqCst) == 2879424 { //put as many hashtags as the files contain
            let total_time = self.start_time.elapsed();
            println!("Done reading all hashtags ({}).\n
                     Items read : {}, total time : {:?}",
                     self.hashtags_read.load(Ordering::SeqCst),
                     self.items_read.load(Ordering::SeqCst),
                     total_time
                     )
        }    
    }

    pub fn incr_items(&self, amount : usize) {
        self.items_read.fetch_add(amount as i64, Ordering::SeqCst);
    }

    pub fn time_elapsed(&self) {
        let total_time = self.start_time.elapsed();
        println!("Time elapsed {:?}", total_time);
    }

    pub fn add_metric(&mut self, metric : &'a mut Metric) {
        self.proc_metrics.push(metric);
    }

    pub fn update_s_duration(&mut self, d : Duration) {
//        *self.splits_time.lock().unwrap() += d;

//        if self.splits_time.lock().unwrap().as_millis() >= 500 {
//            println!("{:?}", *self.splits_time.lock().unwrap());
//            *self.splits_time.lock().unwrap() = Duration::new(0, 0);
//            println!{"yes"}
//        }
//        println!("Total duration for splitting {:?}", self.splits_time);
//        self.proc_metrics.push(metric);
    }

    pub fn print_metrics(&self) {
        for metric in &self.proc_metrics {
            println!("process {} inp_throughput : {:?}, out_throughput : {:?} (items/ ms), total_amount_in : {}",
                      metric.p_id, metric.inp_throughput.load(Ordering::SeqCst), metric.out_throughput.load(Ordering::SeqCst),
                      metric.total_amount_in.load(Ordering::SeqCst));
        }
        println!("---------------------------------------------");
    }

//    pub fn update(&mut self, id : usize, amount : i64) {
//        self.proc_metrics[id].incr_items(amount);
//    }
}
