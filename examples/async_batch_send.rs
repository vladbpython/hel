use hel::channel::{
    bounded
};

#[tokio::main]
async fn main() {
    let (tx, rx) = bounded::<u64, 256>();
    // Consumer
    let handle = tokio::spawn(async move {
        let mut buf = Vec::with_capacity(64);
        loop {
            let (count, disc) = rx.recv_batch_async(&mut buf, 64).await;
            buf.clear();
            if count == 0 || disc { break; }
        }
    });

    // Отправляем батч целиком
    let mut buf: Vec<u64> = (0..1000).collect();
    let sent = tx.send_async_batch(&mut buf).await;
    println!("sent {}", sent);
    // Retry loop при Disconnected
    let mut buf: Vec<u64> = (1000..2000).collect();
    while !buf.is_empty() {
        let sent = tx.send_async_batch(&mut buf).await;
        println!("sent {}",sent);
        if sent == 0 {
            println!("disconnected, {} items unsent", buf.len());
            break;
        }
    }
    drop(tx);
    handle.await.unwrap();
}