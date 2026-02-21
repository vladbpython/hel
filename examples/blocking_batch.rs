use hel::channel::{
    bounded
};
use std::thread;


fn main() {
    let (tx, rx) = bounded::<u64, 256>();

    thread::spawn(move || {
        for i in 0..10_000u64 { tx.send(i).unwrap(); }
    });

    let mut buf = Vec::with_capacity(64);
    let mut total = 0u64;

    loop {
        let (n, disconnected) = rx.recv_batch(&mut buf, 64);
        for v in buf.drain(..) { total += v; }
        if disconnected && n == 0 { break; }
        if n == 0 { std::hint::spin_loop(); } // буфер временно пуст
    }
    println!("batch total: {total}");
}