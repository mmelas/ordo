use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

pub struct Metrics {
    pub start_time : Instant,
    pub items_read : AtomicI64,
    pub hashtags_read : AtomicI64,
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics {
            start_time : Instant::now(),
            items_read : AtomicI64::new(0),
            hashtags_read : AtomicI64::new(0),
        }
    }
}

impl Metrics {
    pub fn incr_hashtags(&self) {
        if self.hashtags_read.fetch_add(1, Ordering::SeqCst) == 287 { //put as many hashtags as the files contain
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
}
