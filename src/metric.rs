use crate::metrics::Metrics;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use atomic_float::AtomicF64;
use std::cmp;
use std::time::Instant;
use crate::params;
use std::sync::Arc;
use std::process;
use std::fs::OpenOptions;
use std::io::Write;
use std::fmt;
use std::env;
 
const PERIOD : i64 = params::PERIOD;
const PRODUCERS : i64 = params::PRODUCERS;
const SELECTIVITY_ARR_LEN : usize = 20;

pub struct Metric {
    pub p_id : i64,
    pub tick : AtomicI64,
    pub inp_throughput : AtomicF64,
    pub out_throughput : AtomicI64,
    pub items_read : AtomicI64,
    pub items_written : AtomicI64,
    pub start_time : Instant,
    pub hashtags_read : AtomicI64,
    pub total_amount_in_per_run : AtomicI64,
    pub selectivity : AtomicF64,
    pub select_cnt : AtomicI64,
    pub safety_margin : f64, //TODO: use safety_margin and epsilon along with confidence interval for selectivity
    pub epsilon : AtomicI64,
    pub extra_slices : AtomicI64,
    pub total_extra_slices : AtomicI64,
    pub total_runs : AtomicI64,
    pub not_entered_cnt : AtomicU64,
    pub total_amount_in : AtomicF64,
    pub total_amount_out : AtomicF64,
    pub output_throughput_array : Mutex<Vec<i64>>
}

impl Metric {
    pub fn new(p_id : i64) -> Self {
        Metric {p_id : p_id, tick : AtomicI64::new(0), inp_throughput : AtomicF64::new(0.0), 
                out_throughput : AtomicI64::new(0), items_read : AtomicI64::new(0), start_time : Instant::now(),
                items_written : AtomicI64::new(0), hashtags_read : AtomicI64::new(0), total_amount_in_per_run : AtomicI64::new(0),
                selectivity : AtomicF64::new(1000.0), select_cnt : AtomicI64::new(0), safety_margin : 0.3, epsilon : AtomicI64::new(0), extra_slices : AtomicI64::new(0), total_extra_slices : AtomicI64::new(0), total_runs : AtomicI64::new(0), not_entered_cnt : AtomicU64::new(0), total_amount_in : AtomicF64::new(0.0), total_amount_out : AtomicF64::new(0.0), output_throughput_array : Mutex::new(Vec::new())}
    }

    pub fn update(&mut self, amount_in : i64, amount_out : i64) {
        // only needed for output process, maybe remove after debugging no longer needed
        // (when the call to this function is removed from the output process as well)
        if amount_in == 0 {
            return;
        }
        let curr_tick = self.tick.fetch_sub(amount_in, Ordering::SeqCst) - amount_in;
        let items_read = self.items_read.fetch_add(amount_in, Ordering::SeqCst) + amount_in;
        let items_written = self.items_written.fetch_add(amount_out, Ordering::SeqCst) + amount_out;
        self.total_runs.fetch_add(1, Ordering::SeqCst);

        // Update selectivity only if it is not an outlier (it is within 20% of the average
        // selectivity (of the last 20 items))
        let curr_amount_in = self.total_amount_in.fetch_add(amount_in as f64, Ordering::SeqCst) + amount_in as f64;
        let curr_amount_out = self.total_amount_out.fetch_add(amount_out as f64, Ordering::SeqCst) + amount_out as f64;
        let mut curr_throughput = (curr_amount_out / curr_amount_in) as f64;
        // In order to avoid overflows, whenever total throughput is higher than 100_000, simplify
        // the fraction
        if curr_throughput > 100000.0 {
            self.total_amount_in.store(curr_throughput, Ordering::SeqCst);
            self.total_amount_out.store(1.0, Ordering::SeqCst);
        }
        if curr_throughput == 0.0 {
            curr_throughput = 1.0;
        }
        self.selectivity.store(curr_throughput, Ordering::SeqCst);
        //if self.total_runs.load(Ordering::SeqCst) < 50 || (throughput < 1.2 * self.selectivity.load(Ordering::SeqCst) &&
         //                                                  throughput > 0.8 * self.selectivity.load(Ordering::SeqCst)){
            //println!("{}", throughput);
        //}
/*
        if self.p_id == 1 {
            //println!("{}", amount_out as f64/ amount_in as f64);
            println!("{} {} {} {} {}", self.selectivity_arr[0].load(Ordering::SeqCst), self.selectivity_arr[1].load(Ordering::SeqCst),
                      self.selectivity_arr[2].load(Ordering::SeqCst), self.selectivity_arr[3].load(Ordering::SeqCst), self.selectivity_arr[4].load(Ordering::SeqCst));
        }
*/

        self.total_amount_in_per_run.fetch_add(amount_in, Ordering::SeqCst);
        // period ticks passed, update inp_throughput
        //if curr_tick <= 0 {
            let total_ms = self.start_time.elapsed().as_millis() as i64;
            let current_inp_throughput = items_read as f64 / cmp::max(1, total_ms) as f64;
            let current_out_throughput = items_written / cmp::max(1, total_ms);
            self.inp_throughput.store(current_inp_throughput, Ordering::SeqCst);
            self.out_throughput.store(current_out_throughput, Ordering::SeqCst);
            self.tick.store(PERIOD, Ordering::SeqCst);
        //    println!("Process : {} inp_throughput : {}, out_throughput : {} (items/ms)", self.p_id, current_inp_throughput, current_out_throughput);
        //}
    }

    pub fn incr_items(&self, amount : i64) {
        self.items_read.fetch_add(amount, Ordering::SeqCst);
    }

    pub fn update_extra_slices(&self, amount : i64) {
        self.extra_slices.fetch_add(amount, Ordering::SeqCst);
        self.total_extra_slices.fetch_add(amount, Ordering::SeqCst);
    }

    pub fn update_not_entered_cnt(&mut self, t : u64) {
        self.not_entered_cnt.fetch_add(t, Ordering::SeqCst);
    }

    // RUNTIME 
//    pub fn incr_hashtags(&self, amount : i64) {
//        //println!("Hashtags num : {}", self.hashtags_read.load(Ordering::SeqCst) + amount);//456750000
//        if self.hashtags_read.fetch_add(amount, Ordering::Relaxed) + amount == 349019020 { //put as many hashtags as the files contain
//            let total_time = self.start_time.elapsed();
//            println!("Done reading all hashtags ({}).\n
//                     total time : {:?}",
//                     self.hashtags_read.load(Ordering::SeqCst),
//                     total_time
//                     );
//        let mut file = OpenOptions::new().append(true).create(true).open("runtime.txt").expect("Unable to open file");
//        let output_string = format!("Threads : {} running time : {:?}\n", PRODUCERS, total_time);
//        file.write_all(output_string.as_bytes()).expect("write failed");
//        println!("Data appended successfuly");
//
//	    process::exit(0);
//        }    
//    }

    // THROUGHPUT
//    pub fn incr_hashtags(&self, amount : i64) {
//        //println!("Hashtags num : {}", self.hashtags_read.load(Ordering::SeqCst) + amount);//456750000
//        if self.hashtags_read.fetch_add(amount, Ordering::Relaxed) + amount == 349019020 { //put as many hashtags as the files contain
//            let total_time = self.start_time.elapsed();
//            println!("Done reading all hashtags ({}).\n
//                     total time : {:?}",
//                     self.hashtags_read.load(Ordering::SeqCst),
//                     total_time
//                     );
//	let args: Vec<String> = env::args().collect();
//	let file_name = format!("throughput{}.txt", args[1]);
//        let mut file = OpenOptions::new().append(true).create(true).open(file_name).expect("Unable to open file");
//        for val in &*self.output_throughput_array.lock().unwrap() {
//            //file.write_all(val.()).expect("write failed");
//	    write!(file, "Run : {}, throughput value : {}\n", params::RUN, &val);
//        }
//        println!("Data appended successfuly");
//	    process::exit(0);
//        }    
//    }

    // AVG THROUGHPUT && RUNTIME
    pub fn incr_hashtags(&self, amount : i64) {
        //println!("Hashtags num : {}", self.hashtags_read.load(Ordering::SeqCst) + amount);//456750000
        if self.hashtags_read.fetch_add(amount, Ordering::Relaxed) + amount == 349019020 { //put as many hashtags as the files contain
            let total_time = self.start_time.elapsed();
            println!("Done reading all hashtags ({}).\n
                     total time : {:?}",
                     self.hashtags_read.load(Ordering::SeqCst),
                     total_time
                     );
	let args: Vec<String> = env::args().collect();
	let file_name = format!("/var/scratch/mmelas/both{}.txt", args[1]);
	let file_name2 = format!("/var/scratch/mmelas/throughputs{}.txt", args[1]);
        let mut file = OpenOptions::new().append(true).create(true).open(file_name).expect("Unable to open file");
        let mut file2 = OpenOptions::new().append(true).create(true).open(file_name2).expect("Unable to open file");
        let mut avg_throughput = 0;
        let mut cnt = 0;
        for val in &*self.output_throughput_array.lock().unwrap() {
            avg_throughput += val;
            cnt += 1;
	    write!(file2, "Run : {}, throughput value : {}\n", params::RUN, &val);
            //file.write_all(val.()).expect("write failed");
        }
        avg_throughput /= cnt;
	    write!(file, "Thread : {}, Run : {}, throughput avg : {}, runtime : {:?}\n", PRODUCERS, params::RUN, avg_throughput, total_time);
        println!("Data appended successfuly");
	    process::exit(0);
        }    
    }


    pub fn save_throughput(&self) {
        (*(self.output_throughput_array.lock().unwrap())).push(self.out_throughput.load(Ordering::SeqCst));
    }
}

//impl Ord for Metric {
//    fn cmp(&self, other: &Self) -> cmp::Ordering {
//        self.inp_throughput.load(Ordering::SeqCst).cmp(&other.inp_throughput.load(Ordering::SeqCst))
//    }
//}
//
//impl PartialOrd for Metric {
//    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
//        Some(self.cmp(other))
//    }
//}
//
//impl PartialEq for Metric {
//    fn eq(&self, other: &Self) -> bool {
//        self.inp_throughput.load(Ordering::SeqCst) == other.inp_throughput.load(Ordering::SeqCst)
//    }
//}
//
//impl Eq for Metric {
//
//}
