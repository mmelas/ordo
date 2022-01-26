use std::sync::{Arc, Mutex, Condvar};
use std::cell::UnsafeCell;
use std::thread;
use std::sync::atomic::{AtomicI64, Ordering, AtomicUsize};
use std::time::Instant;

use crate::fifo;

// NUM_ITEMS must make THREAD_ITEMS even num
const NUM_ITEMS : usize = 10_000;
const PRODUCERS : i64 = 1;
const CONSUMERS : i64 = 1;
const THREAD_ITEMS : usize = NUM_ITEMS / PRODUCERS as usize;

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
                for _ in 0..THREAD_ITEMS {
                    sem_p.dec();
                    let mut curr_q = q_ptr_c.lock().unwrap();
                    let ind = curr_q.w_ind;
                    curr_q.buffer[ind] = cnt_c.fetch_add(1, Ordering::SeqCst);
                    // wrap it around maybe
                    curr_q.w_ind += 1;
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

    let rem_read = Arc::new(Mutex::new(NUM_ITEMS));
    let t0 = Instant::now();
    for _ in 0..CONSUMERS {
        let q_ptr_c = q.clone();
        let rem_c = rem_read.clone();
        let sem_p = prod_sem.clone();
        let sem_c = cons_sem.clone();
        cons_threads.push(thread::spawn(move || {
            for _ in 0..THREAD_ITEMS {
                sem_c.dec();
                let mut curr_q = q_ptr_c.lock().unwrap();
                let calculation = curr_q.buffer[curr_q.r_ind] + 1;
                curr_q.r_ind += 1;
                drop(curr_q);
                let mut rem = rem_c.lock().unwrap();
                *rem -= 1;
                if *rem == 0 {
                    let consumers_time = t0.elapsed();
                    println!("Consumers time: {:.2?}", consumers_time);
                    println!("Total time: {:.2?}", producers_time + consumers_time);
                }
                sem_p.inc();
            }
       }));

    }

    while (true) {
        //do nothing
    }
}
