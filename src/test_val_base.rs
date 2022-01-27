use std::sync::{Arc, Mutex};
use std::thread;
use std::sync::atomic::{AtomicI64, Ordering};
use std::collections::HashSet;
use std::sync::Condvar;
use std::time::Instant;

use crate::fifo;

const NUM_ITEMS : usize = 10_000;
const PRODUCERS : i64 = 1;
const CONSUMERS : i64 = 1;

pub struct Semaphore {
    mutex: Mutex<i64>,
    cvar: Condvar,
}

impl Semaphore {
    pub fn new(count: i64) -> Self {
        Semaphore {
            mutex: Mutex::new(count),
            cvar: Condvar::new(),
        }
    }

    pub fn dec(&self) {
        let mut lock = self.mutex.lock().unwrap();
        *lock -= 1;
        if *lock < 0 {
            let _ = self.cvar.wait(lock).unwrap();
        }
    }

    pub fn inc(&self) {
        let mut lock = self.mutex.lock().unwrap();
        *lock += 1;
        if *lock <= 0 {
            // maybe notify_all ? test performance of both
            self.cvar.notify_one();
        }
    }
}

unsafe impl Send for Semaphore {}
unsafe impl Sync for Semaphore {}

pub fn run_test() {
    let q = Arc::new(Mutex::new(fifo::Queue{..Default::default()}));
    /*
     * Each tread requires a different counter ref
     */
    let counter = Arc::new(AtomicI64::new(0));

    let mut prod_threads = vec![];

    let prod_sem = Arc::new(Semaphore::new(NUM_ITEMS as i64));
    let cons_sem = Arc::new(Semaphore::new(0));

    let t0 = Instant::now();
    {
        /*
         * Producers
         */
        for _ in 0..PRODUCERS {
            let q_ptr_c = q.clone();
            let cnt_c = counter.clone();
            let sem_p = prod_sem.clone();
            let sem_c = cons_sem.clone();
            prod_threads.push(thread::spawn(move || {
                loop {
                    sem_p.dec();
                    let mut curr_q = q_ptr_c.lock().unwrap();
                    let ind = curr_q.w_ind;
                    curr_q.buffer[ind] = cnt_c.fetch_add(1, Ordering::SeqCst);
                    curr_q.w_ind += 1;
                    curr_q.w_ind %= NUM_ITEMS;
                    sem_c.inc();
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

    let rem_read = Arc::new(Mutex::new(0));
    /*
     * The set will contain CONSUMERS-1 more values
     * because only one consumer will enter the equality condition on line 20
     * while the rest will fall negative
     */
    let nums_inserted = Arc::new(Mutex::new(HashSet::new()));
    let t0 = Instant::now();
    for _ in 0..CONSUMERS {
        let q_ptr_c = q.clone();
        let rem_c = rem_read.clone();
        let sem_p = prod_sem.clone();
        let sem_c = cons_sem.clone();
        let nums_inserted_c = nums_inserted.clone();
        cons_threads.push(thread::spawn(move || {
            loop {
                sem_c.dec();
                let mut curr_q = q_ptr_c.lock().unwrap();
                let mut s = nums_inserted_c.lock().unwrap();
                if s.contains(&(curr_q.buffer[curr_q.r_ind] + 1)) {
                    println!("Error, duplicate value {}", curr_q.buffer[curr_q.r_ind] + 1);
                };
                s.insert(curr_q.buffer[curr_q.r_ind] + 1);
                let mut rem = rem_c.lock().unwrap();
                *rem += 1;
                curr_q.r_ind += 1;
                curr_q.r_ind %= NUM_ITEMS;
                drop(curr_q);
//                if *rem <= 0 {
//                    let consumers_time = t0.elapsed();
//                    println!("Consumers time: {:.2?}", consumers_time);
//                    println!("Total time: {:.2?}", producers_time + consumers_time);
//                    break;
//                }
                sem_p.inc();
            }
       }));
    }

//    for th in cons_threads {
//        _ = th.join();
//    }
//
//    for i in 0..NUM_ITEMS as i64 + 4 {
//        if !nums_inserted.lock().unwrap().contains(&i) {
//            println!("Error : Didn't find {} in the hashmap", i);
//        }
//    }

    ctrlc::set_handler(move || {
        let consumers_time = t0.elapsed();
        let curr_reads = *rem_read.lock().unwrap();
        let mut cnt = 0;
        for i in 0..curr_reads as i64 {
            if !nums_inserted.lock().unwrap().contains(&(i + 1)) {
                println!("Error : Didn't find {} in the hashmap", i + 1);
                cnt += 1;
            }
        }
        println!("Count of missing items {}", cnt);
        println!("Consumers time: {:.2?}", consumers_time);
        println!("Total time: {:.2?}", producers_time + consumers_time);
        println!("Items read: {:.2?}", *rem_read.lock().unwrap());
        std::process::exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    while (true) {
        //do nothing
    }
}
