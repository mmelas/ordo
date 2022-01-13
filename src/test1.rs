use std::sync::{Arc, Mutex};
use std::cell::UnsafeCell;
use std::thread;
use std::sync::atomic::{AtomicI64, Ordering};
use std::collections::HashMap;
use crate::fifo;

pub fn run_test() {
    let q = UnsafeCell::new(fifo::Queue{..Default::default()});

    
    let ptr_wslice = q.get();
    let wslice = unsafe{ (*ptr_wslice).reserve(250) };
    let ptr_wslice2 = q.get();
    let wslice2 = unsafe{ (*ptr_wslice2).reserve(200) };
    let ptr_wslice3 = q.get();
    let wslice3 = unsafe{ (*ptr_wslice3).reserve(200) };
    let ptr_wslice4 = q.get();
    let wslice4 = unsafe{ (*ptr_wslice4).reserve(200) };
    let ptr_wslice5 = q.get();
    let wslice5 = unsafe{ (*ptr_wslice5).reserve(149) };

    /*
     * Producers
     */
    let counter = Arc::new(AtomicI64::new(0));
    let cnt_clone = counter.clone();
    let cnt_clone2 = counter.clone();
    let cnt_clone3 = counter.clone();
    let cnt_clone4 = counter.clone();
    let cnt_clone5 = counter.clone();

    let mut prod_threads = vec![];
    prod_threads.push(thread::spawn(move || {
        match wslice {
            Some(mut x) => {
                for _ in 0..x.len {
                    let curr = cnt_clone.fetch_add(1, Ordering::SeqCst);
                    unsafe {
                        x.update(0, curr);
                    }
                }
                unsafe {
                    x.commit();
                }
            },
            None => {
                println!("error");
            }
        }
    }));

    prod_threads.push(thread::spawn(move || {
        match wslice2 {
            Some(mut x) => {
                for _ in 0..x.len {
                    let curr = cnt_clone2.fetch_add(1, Ordering::SeqCst);
                    unsafe {
                        x.update(0, curr);
                    }
                }
                unsafe {
                    x.commit();
                }
            },
            None => {
                println!("error");
            }
        }
    }));

    prod_threads.push(thread::spawn(move || {
        match wslice3 {
            Some(mut x) => {
                for _ in 0..x.len {
                    let curr = cnt_clone3.fetch_add(1, Ordering::SeqCst);
                    unsafe {
                        x.update(0, curr);
                    }
                }
                unsafe {
                    x.commit();
                }
            },
            None => {
                println!("error");
            }
        }
    }));

    prod_threads.push(thread::spawn(move || {
        match wslice4 {
            Some(mut x) => {
                for _ in 0..x.len {
                    let curr = cnt_clone4.fetch_add(1, Ordering::SeqCst);
                    unsafe {
                        x.update(0, curr);
                    }
                }
                unsafe {
                    x.commit();
                }
            },
            None => {
                println!("error");
            }
        }
    }));

    prod_threads.push(thread::spawn(move || {
        match wslice5 {
            Some(mut x) => {
                for _ in 0..x.len {
                    let curr = cnt_clone5.fetch_add(1, Ordering::SeqCst);
                    unsafe {
                        x.update(0, curr);
                    }
                }
                unsafe {
                    x.commit();
                }
            },
            None => {
                println!("error");
            }
        }
    }));


    for th in prod_threads {
        let _ = th.join();
    }
    /*
     * Consumer
     */

    let mut cons_threads = vec![];
    let contains = Arc::new(Mutex::new(HashMap::new()));
    let contains2 = contains.clone();
    let contains3 = contains.clone();
    let contains4 = contains.clone();
    let contains5 = contains.clone();
    let ptr_slice = q.get();
    let mut slice = unsafe{ (*ptr_slice).dequeue_multiple(200) };
    let mut slice2 = unsafe{ (*ptr_slice).dequeue_multiple(200) };
    let mut slice3 = unsafe{ (*ptr_slice).dequeue_multiple(200) };
    let mut slice4 = unsafe{ (*ptr_slice).dequeue_multiple(399) };

    cons_threads.push(thread::spawn(move || {
        let offset = slice.offset;
        println!("{}", offset);
        for i in 0..slice.len {
            let mut contains = contains2.lock().unwrap();
            contains.insert(slice.queue.buffer[i + offset] + 1, true);
    //        println!("Iteration : {}, Item : {}", i, slice.queue.buffer[i + offset]);
        }
        slice.commit();
    }));

    cons_threads.push(thread::spawn(move || {
        let offset = slice2.offset;
        println!("{}", offset);
        for i in 0..slice2.len {
            let mut contains = contains3.lock().unwrap();
            contains.insert(slice2.queue.buffer[i + offset] + 1, true);
    //        println!("Iteration : {}, Item : {}", i, slice2.queue.buffer[i +  offset]);
        }
        slice2.commit();
    }));

    cons_threads.push(thread::spawn(move || {
        let offset = slice3.offset;
        println!("{}", offset);
        for i in 0..slice3.len {
            let mut contains = contains4.lock().unwrap();
            contains.insert(slice3.queue.buffer[i + offset] + 1, true);
     //       println!("Iteration : {}, Item : {}", i, slice3.queue.buffer[i + offset]);
        }
        slice3.commit();
    }));

    cons_threads.push(thread::spawn(move || {
        let offset = slice4.offset;
        println!("{}", offset);
        for i in 0..slice4.len {
            let mut contains = contains5.lock().unwrap();
            contains.insert(slice4.queue.buffer[i + offset] + 1, true);
    //        println!("Iteration : {}, Item : {}", i, slice4.queue.buffer[i + offset]);
        }
        slice4.commit();
    }));

    for th in cons_threads {
        let _ = th.join();
    }

    for i in 0..999 {
        let contains = contains.lock().unwrap();
        if contains.get(&(i + 1)) != Some(&true) {
            println!("Error : Didn't find {} in the hashmap", i);
        }
    }
    println!("Nice");
}
