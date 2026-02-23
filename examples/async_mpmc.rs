use hel::{
    channel::{
        bounded
    },
    errors::AsyncRecvError,
};

#[tokio::main]
async fn main() {
    let (tx, rx) = bounded::<u64, 128>();
    let tx2 = tx.clone();

    tokio::spawn(async move {
        for i in 0..500u64 { tx.send_async(i).await.unwrap(); }
    });
    tokio::spawn(async move {
        for i in 500..1000u64 { tx2.send_async(i).await.unwrap(); }
    });

    let mut sum = 0u64;
    loop {
        match rx.recv_async().await {
            Ok(v) => sum += v,
            Err(AsyncRecvError::Disconnected) => break,
        }
    }
    println!("async sum: {sum}");
}