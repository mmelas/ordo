use std::sync::{Arc, Mutex};
use std::cell::UnsafeCell;
use std::thread;
use std::sync::atomic::{AtomicI64, Ordering, AtomicUsize};
use std::time::Instant;
use crate::fifo;

// NUM_ITEMS must be multiple of 8
const NUM_ITEMS : usize = 10_000;
const PRODUCERS : i64 = 4;
const CONSUMERS : i64 = 4;
const THREAD_ITEMS : usize = NUM_ITEMS / PRODUCERS as usize;

pub fn run_test() {
    let q = Arc::new(Mutex::new(fifo::Queue{..Default::default()}));
    /*
     * Each tread requires a different counter ref
     */
    let counter = Arc::new(AtomicI64::new(0));

    let mut prod_threads = vec![];

    let t0 = Instant::now();
    {
        /*
         * Producers
         */
        for _ in 0..PRODUCERS {
            let q_ptr_c = q.clone();
            let cnt_c = counter.clone();
            prod_threads.push(thread::spawn(move || {
                for _ in 0..THREAD_ITEMS {
                    let mut curr_q = q_ptr_c.lock().unwrap();
                    let ind = curr_q.w_ind;
                    curr_q.buffer[ind] = cnt_c.fetch_add(1, Ordering::SeqCst);
                    curr_q.w_ind += 1;
                    drop(curr_q);
                }
            }));
        }
    }

    let producers_time = t0.elapsed();
    println!("Producers time: {:.2?}", producers_time);

    /*
     * Consumers
     */
    let mut cons_threads = vec![];

    let rem_read = Arc::new(Mutex::new(NUM_ITEMS));
    let t0 = Instant::now();
    for _ in 0..CONSUMERS {
        let q_ptr_c = q.clone();
        let rem_c = rem_read.clone();
        cons_threads.push(thread::spawn(move || {
            for _ in 0..THREAD_ITEMS {
                let mut curr_q = q_ptr_c.lock().unwrap();
                let calculation = curr_q.buffer[curr_q.r_ind] + 1;
                let mut rem = rem_c.lock().unwrap();
                *rem -= 1;
                curr_q.r_ind += 1;
                if *rem == 0 {
                    let consumers_time = t0.elapsed();
                    println!("Consumers time: {:.2?}", consumers_time);
                    println!("Total time: {:.2?}", producers_time + consumers_time);
                }
            }
       }));

    }

    while (true) {
        //do nothing
    }
}
