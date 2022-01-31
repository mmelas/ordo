use std::sync::{Arc, Mutex, Condvar};
use std::cell::UnsafeCell;
use std::{thread};
use std::sync::atomic::{AtomicI64, Ordering};
use std::collections::HashSet;
use std::time::{Instant, Duration};
use crate::fifo;

// NUM_ITEMS must be multiple of 8
const NUM_ITEMS : usize = 50_000;
const PRODUCERS : i64 = 3;
const CONSUMERS : i64 = 3;
const WRITE_SLICE_S : usize = 1000;
const READ_SLICE_S : usize = 1000;

// NewType design in order to make
// raw pointer Send + Sync
struct SendPtr<T> (*mut T);
impl<T> SendPtr<T> {
    pub fn get(self) -> *mut T {
        return self.0;
    }

}
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}
impl<T> Clone for SendPtr<T> {
    fn clone(&self) -> Self { *self }
}
impl<T> Copy for SendPtr<T> {}

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
    let q = UnsafeCell::new(fifo::Queue{..Default::default()});

    /*
     * Each thread requires a different pointer ref
     */
    let ptr_wslice = q.get();

    /*
     * Each tread requires a different counter ref
     */
    let counter = Arc::new(AtomicI64::new(0));

    let prod_sem = Arc::new(Semaphore::new(NUM_ITEMS as i64/ WRITE_SLICE_S as i64));
    let cons_sem = Arc::new(Semaphore::new(0));


    let t0 = Instant::now();
    {
        /*
         * Producers
         */
        let mut prod_threads = vec![];
        for _ in 0..PRODUCERS {
            let p = SendPtr(ptr_wslice);
            let cnt_clone = counter.clone();
            let sem_p = prod_sem.clone();
            let sem_c = cons_sem.clone();
            prod_threads.push(thread::spawn(move || {
                loop {
                    let wslice = unsafe{ (*p.get()).reserve(WRITE_SLICE_S) };
                    match wslice {
                        Some(mut x) => {
 //                           sem_p.dec();
                            for _ in 0..x.len {
                                let curr = cnt_clone.fetch_add(1, Ordering::SeqCst);
//                                println!("{}", curr);
                                unsafe {
                                     x.update(curr);
                                 }
                            }
                            unsafe {
                                x.commit();
                            }
                            if cnt_clone.load(Ordering::SeqCst) > NUM_ITEMS as i64 {
                                break;
                            }
//                            sem_c.inc();
                        },
                        None => {
//                            println!("error");
                        }
                    }
                }
            }));
        }
    }
    let producers_time = t0.elapsed();

    /*
     * Consumers
     */
    let mut cons_threads = vec![];

    let ptr_wslice = q.get();

    /*
     * Eventually the Set will contain 3*slice_size more values
     * because the producers do not stop appending values
     * and we have 4 threads reading simultaneously (so 3 threads more after 
     * the *rem = 0 one
     */
    let included_nums = Arc::new(Mutex::new(HashSet::new()));
    let rem_read = Arc::new(Mutex::new(NUM_ITEMS as i64));
    let t0 = Instant::now();
    for _ in 0..CONSUMERS as usize {
        let p = SendPtr(ptr_wslice);
        let sem_p = prod_sem.clone();
        let sem_c = cons_sem.clone();
        let rem_c = rem_read.clone();
        let included_nums_c = included_nums.clone();
        cons_threads.push(thread::spawn(move || {
            let mut cnt2 = 0;
            loop {
//                sem_c.dec();
                let slice = unsafe{ (*p.get()).dequeue_multiple(READ_SLICE_S as i64) };
                let offset = slice.offset;
//                println!("len : {}, offset : {}, duplicate values {}", slice.len, offset, cnt2);
                for i in 0..slice.len {
                    let ind = i + offset; // DO we need % here?
                    if included_nums_c.lock().unwrap().contains(&(slice.queue.buffer[ind] + 1)) {
                        println!("Error, duplicate value {}", slice.queue.buffer[ind] + 1);
                        cnt2 += 1;
                    };
//                    println!("Value {}", slice.queue.buffer[ind] + 1);
                    included_nums_c.lock().unwrap().insert(slice.queue.buffer[ind] + 1); 
                }
                let mut rem = rem_c.lock().unwrap();
                *rem -= slice.len as i64;
                if *rem <= 0 {
                    println!("cnt 2 {}", cnt2);
                    let consumers_time = t0.elapsed();
                    println!("Consumers time: {:.2?}", consumers_time);
                    println!("Producers time: {:.2?}", producers_time);
                    println!("Total time: {:.2?}", producers_time + consumers_time);
                    let mut cnt_missing = 0;
                    for i in 0..NUM_ITEMS as i64 {
                        if !included_nums_c.lock().unwrap().contains(&(i + 1)) {
//                            println!("{}, Error : Didn't find {} in the hashmap", *rem, i + 1);
                            cnt_missing += 1;
                        }
                    }
                    println!("{}, Number of missing values : {}", *rem, cnt_missing);
                    break;
                }
//                sem_p.inc();
            }
        }));
    }
    while true {
        //do nothing
    }

    println!("Nice");
}
