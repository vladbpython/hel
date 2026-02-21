use hel::channel::{
        bounded
    };

#[tokio::main]
async fn main() {
    let (tx1, rx1) = bounded::<&str, 8>();
    let (tx2, rx2) = bounded::<u64, 8>();

    tokio::spawn(async move { tx1.send_async("event").await.unwrap(); });
    tokio::spawn(async move { tx2.send_async(42u64).await.unwrap(); });

    tokio::select! {
        Ok(msg) = rx1.recv_async() => println!("string: {msg}"),
        Ok(num) = rx2.recv_async() => println!("number: {num}"),
    }
}