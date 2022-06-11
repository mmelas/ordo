use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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
    pub proc_metrics : Vec<&'a mut Metric>,
    pub activation : AtomicI64,
    pub prev_act : i64,
    pub process : Mutex<Vec<i64>>,
    pub reserve_time : AtomicU64,
    pub commit_time : AtomicU64,
    pub read_time : AtomicU64,
}

impl<'a> Default for Metrics<'a> {
    fn default() -> Self {
        Metrics {
            start_time : Instant::now(),
            items_read : AtomicI64::new(0),
            hashtags_read : AtomicI64::new(0),
            splits_time : Mutex::new(Duration::new(0, 0)),
            proc_metrics : vec![],
            activation : AtomicI64::new(0),
            prev_act : 0,
            process : Mutex::new(vec![0; 4]),
            reserve_time : AtomicU64::new(0),
            commit_time : AtomicU64::new(0),
            read_time : AtomicU64::new(0),
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

    pub fn update_activation(&mut self, a : i64) {
        self.activation.fetch_add(a, Ordering::SeqCst);
    }

    pub fn update_process(&mut self, p : usize) {
        self.process.lock().unwrap()[p] += 1;
    }

    pub fn update_reserve_time(&mut self, t : u64) {
        self.reserve_time.fetch_add(t, Ordering::SeqCst);
    }

    pub fn update_commit_time(&mut self, t : u64) {
        self.commit_time.fetch_add(t, Ordering::SeqCst);
    }

    pub fn update_read_time(&mut self, t : u64) {
        self.read_time.fetch_add(t, Ordering::SeqCst);
    }

    pub fn print_metrics(&mut self) {
        let act = self.activation.load(Ordering::SeqCst);
        for metric in &self.proc_metrics {
            println!("process {} inp_throughput : {:?}, out_throughput : {:?} (items/ ms), total_amount_in: {}, total_activation_amount : {}, total_reserve_time : {:?}, total_commit_time : {:?}, total_read_time : {:?}, 
                     last extra required slices : {}, total extra required slices : {}, selectivity : {}, not_entered_cnt : {}",
                      metric.p_id, metric.inp_throughput.load(Ordering::SeqCst), metric.out_throughput.load(Ordering::SeqCst),
                      metric.total_amount_in.load(Ordering::SeqCst), act - self.prev_act, self.reserve_time.load(Ordering::SeqCst), 
                      self.commit_time.load(Ordering::SeqCst), self.read_time.load(Ordering::SeqCst),
                      metric.extra_slices.load(Ordering::SeqCst), metric.total_extra_slices.load(Ordering::SeqCst), metric.selectivity.load(Ordering::SeqCst), metric.not_entered_cnt.load(Ordering::SeqCst));
            metric.extra_slices.store(0, Ordering::SeqCst);
            metric.not_entered_cnt.store(0, Ordering::SeqCst);
            //(*self.process.lock().unwrap()).iter_mut().for_each(|x| println!("{}", x));
        }
        self.reserve_time.store(0, Ordering::SeqCst);
        self.commit_time.store(0, Ordering::SeqCst);
        self.read_time.store(0, Ordering::SeqCst);
        self.prev_act = act;
        println!("---------------------------------------------");
    }

//    pub fn update(&mut self, id : usize, amount : i64) {
//        self.proc_metrics[id].incr_items(amount);
//    }
}
