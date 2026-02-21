use hel::channel::{
    bounded
};

#[tokio::main]
async fn main() {
    let (tx, rx) = bounded::<u64, 256>();

    tokio::spawn(async move {
        for i in 0..10_000u64 { tx.send_async(i).await.unwrap(); }
    });

    let mut buf = Vec::with_capacity(64);
    let mut total = 0u64;

    loop {
        // Ждём первый item, потом дренируем burst без лишних await
        let (n, disconnected) = rx.recv_batch_async(&mut buf, 64).await;
        for v in buf.drain(..) { total += v; }
        if disconnected && n == 0 { break; }
    }
    println!("async batch total: {total}");
}