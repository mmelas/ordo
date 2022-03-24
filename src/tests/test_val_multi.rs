use std::sync::{Arc, Mutex};
use std::cell::UnsafeCell;
use std::{thread, time};
use std::sync::atomic::{AtomicI64, Ordering};
use std::collections::HashSet;
use std::time::Instant;
use crate::fifo;
use crate::params;

//const NUM_ITEMS : usize = params::new().NUM_ITEMS;
const NUM_ITEMS : usize = params::NUM_ITEMS;
const PRODUCERS : i64 = params::PRODUCERS;
const CONSUMERS : i64 = params::CONSUMERS;
const WRITE_SLICE_S : usize = params::WRITE_SLICE_S;
const READ_SLICE_S : usize = params::READ_SLICE_S;
const PRODUCERS_SLICES : usize = NUM_ITEMS / WRITE_SLICE_S;

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
    let slices_counter = Arc::new(AtomicI64::new(0));
    let producers_time = Arc::new(Mutex::new(time::Duration::new(0, 0)));

    let t0 = Instant::now();
    {
        /*
         * Producers
         */
        let mut prod_threads = vec![];
        for _ in 0..PRODUCERS {
            let p = SendPtr(ptr_wslice);
            let cnt_clone = counter.clone();
            let cnt_slices = slices_counter.clone();
            let time_c = producers_time.clone();
            prod_threads.push(thread::spawn(move || {
                loop {
                    let wslice = unsafe{
                        (*p.get()).reserve(WRITE_SLICE_S) 
                    };
                    match wslice {
                        Some(mut x) => {
                            let curr_cnt = cnt_slices.fetch_add(
                                1, Ordering::SeqCst
                            );
//                            println!("slice {}", curr_cnt);
                            let mut curr = 0;
                            for _ in 0..x.len {
                                curr = cnt_clone.fetch_add(
                                    1, Ordering::SeqCst
                                );
                                unsafe {
                                     x.update(curr);
                                 }
                            }
//                            if cnt_slices.load(Ordering::SeqCst) < 5000 {
//                                println!("{}", curr);
//                            }
                            unsafe {
                                x.commit();
                            }
                            if curr_cnt >= PRODUCERS_SLICES as i64 {
                                *time_c.lock().unwrap() = t0.elapsed();
//                                println!("slice {}", cnt_slices.load(Ordering::SeqCst));
                                break;
                            }
                        },
                        None => {
//                            println!("error");
                        }
                    }
                }
            }));
        }
    }

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
        let rem_c = rem_read.clone();
        let included_nums_c = included_nums.clone();
        let prod_time_c = producers_time.clone();
        cons_threads.push(thread::spawn(move || {
            let mut cnt2 = 0;
            loop {
                let mut slice = unsafe{
                    (*p.get()).dequeue_multiple(READ_SLICE_S as i64) 
                };
                let offset = slice.offset;
                for i in 0..slice.len {
                    let ind = (i + offset) % params::QUEUE_SIZE; // DO we need % here?
                    if included_nums_c.lock().unwrap().contains(
                        &(slice.queue.buffer[ind] + 1)
                    ) {
//                        println!("Error, duplicate value {}", slice.queue.buffer[ind] + 1);
                        cnt2 += 1;
                    };
//                    println!("Value {}", slice.queue.buffer[ind] + 1);
                    included_nums_c.lock().unwrap().insert(
                        slice.queue.buffer[ind] + 1
                    ); 
                }
                slice.commit();
//                println!("len : {}, offset : {}, duplicate values {}", slice.len, offset, cnt2);
                let mut rem = rem_c.lock().unwrap();
                *rem -= slice.len as i64;
                if *rem <= 0 {
                    println!(
                        "Count of duplicates: {}", cnt2
                    );
                    let consumers_time = t0.elapsed();
                    println!(
                        "Consumers time: {:.2?}", consumers_time
                    );
                    println!(
                        "Producers time: {:.2?}", *prod_time_c.lock().unwrap()
                    );
                    println!(
                        "Total time: {:.2?}", *prod_time_c.lock().unwrap() + consumers_time
                    );
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
            }
        }));
    }
    while true {
        //do nothing
    }

    println!("Nice");
}
