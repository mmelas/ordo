use crate::metrics::Metrics;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::cmp;
use std::time::Instant;
use crate::params;
use std::sync::Arc;
 
const PERIOD : i64 = params::PERIOD;

pub struct Metric {
    pub p_id : i64,
    pub tick : AtomicI64,
    pub inp_throughput : AtomicI64,
    pub out_throughput : AtomicI64,
    pub items_read : AtomicI64,
    pub items_written : AtomicI64,
    pub start_time : Instant,
    pub hashtags_read : AtomicI64,
    pub total_amount_in : AtomicI64,
}

impl Metric {
    pub fn new(p_id : i64) -> Self {
        Metric {p_id : p_id, tick : AtomicI64::new(0), inp_throughput : AtomicI64::new(0), 
                out_throughput : AtomicI64::new(0), items_read : AtomicI64::new(0), start_time : Instant::now(),
                items_written : AtomicI64::new(0), hashtags_read : AtomicI64::new(0), total_amount_in : AtomicI64::new(0)}
    }

    pub fn update(&mut self, amount_in : i64, amount_out : i64) {
        let curr_tick = self.tick.fetch_sub(amount_in, Ordering::SeqCst) - amount_in;
        let items_read = self.items_read.fetch_add(amount_in, Ordering::SeqCst) + amount_in;
        let items_written = self.items_written.fetch_add(amount_out, Ordering::SeqCst) + amount_out;
        self.total_amount_in.fetch_add(amount_in, Ordering::SeqCst);
        // period ticks passed, update inp_throughput
        if curr_tick <= 0 {
            let total_ms = self.start_time.elapsed().as_millis() as i64;
            let current_inp_throughput = items_read / cmp::max(1, total_ms);
            let current_out_throughput = items_written / cmp::max(1, total_ms);
            self.inp_throughput.store(current_inp_throughput, Ordering::SeqCst);
            self.out_throughput.store(current_out_throughput, Ordering::SeqCst);
            self.tick.store(PERIOD, Ordering::SeqCst);
        //    println!("Process : {} inp_throughput : {}, out_throughput : {} (items/ms)", self.p_id, current_inp_throughput, current_out_throughput);
        }
    }

    pub fn incr_items(&self, amount : i64) {
        self.items_read.fetch_add(amount, Ordering::SeqCst);
    }

    pub fn incr_hashtags(&self, amount : i64) {
        //println!("Hashtags num : {}", self.hashtags_read.load(Ordering::SeqCst) + amount);//456750000
        if self.hashtags_read.fetch_add(amount, Ordering::Relaxed) + amount == 154857248 { //put as many hashtags as the files contain
            let total_time = self.start_time.elapsed();
            println!("Done reading all hashtags ({}).\n
                     total time : {:?}",
                     self.hashtags_read.load(Ordering::SeqCst),
                     total_time
                     )
        }    
    }
}

impl Ord for Metric {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.inp_throughput.load(Ordering::SeqCst).cmp(&other.inp_throughput.load(Ordering::SeqCst))
    }
}

impl PartialOrd for Metric {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Metric {
    fn eq(&self, other: &Self) -> bool {
        self.inp_throughput.load(Ordering::SeqCst) == other.inp_throughput.load(Ordering::SeqCst)
    }
}

impl Eq for Metric {

}
