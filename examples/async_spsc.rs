use hel::{
    channel::{
        scsp_bounded,
    },
    errors::AsyncRecvError,
};

#[tokio::main]
async fn main() {
    let (tx, rx) = scsp_bounded::<u64, 128>();

    tokio::spawn(async move {
        for i in 0..100_000u64 { tx.send_async(i).await.unwrap(); }
    });

    let mut sum = 0u64;
    loop {
        match rx.recv_async().await {
            Ok(v) => sum += v,
            Err(AsyncRecvError::Disconnected) => break,
        }
    }
    println!("spsc async sum: {sum}");
}