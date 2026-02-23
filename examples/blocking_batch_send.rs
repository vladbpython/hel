use hel::channel::{
    bounded
};
use std::{
    thread,
    time::Duration,
};


fn main() {
    let (tx, rx) = bounded::<u64, 256>();
    // Consumer
    let h = thread::spawn(move || {
        let mut buf = Vec::with_capacity(64);
        loop {
            let (count, disc) = rx.recv_batch(&mut buf, 64);
            println!("received {} items", count);
            buf.clear();
            if count == 0 || disc { break; }
        }
    });

    // Простая отправка батча
    let mut buf: Vec<u64> = (0..1000).collect();
    let sent = tx.send_batch(&mut buf);
    println!("sent {}, remaining {}", sent, buf.len());
    // С таймаутом — если буфер полон, ждём не дольше 100ms
    let mut buf: Vec<u64> = (0..1000).collect();
    let sent = tx.send_batch_timeout(&mut buf, Duration::from_millis(100));
    println!("sent {}, remaining {}", sent, buf.len());
    if !buf.is_empty() {
        println!("timeout: sent {}, unsent {} items remain in buf", sent, buf.len());
        // buf содержит неотправленные элементы в исходном порядке — можно retry
    }
    drop(tx);
    h.join().unwrap();
}