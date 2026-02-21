use hel::channel::{
    bounded
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64,Ordering},
    },
    thread,
};

fn main() {
    let (tx, rx) = bounded::<u64, 128>();
    let total = Arc::new(AtomicU64::new(0));

    // 4 producers
    let producers: Vec<_> = (0..4u64).map(|p| {
        let tx = tx.clone();
        thread::spawn(move || {
            for i in 0..1000u64 {
                tx.send(p * 1000 + i).unwrap();
            }
        })
    }).collect();
    drop(tx); // важно: дропаем оригинал

    // 4 consumers
    let consumers: Vec<_> = (0..4).map(|_| {
        let rx = rx.clone();
        let total = total.clone();
        thread::spawn(move || {
            let sum: u64 = rx.iter().sum();
            total.fetch_add(sum, Ordering::Relaxed);
        })
    }).collect();

    for h in producers { h.join().unwrap(); }
    for h in consumers { h.join().unwrap(); }

    println!("total: {}", total.load(Ordering::Relaxed));
}