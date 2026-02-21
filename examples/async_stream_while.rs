use hel::channel::{
    bounded
};
use futures::StreamExt;

#[tokio::main]
async fn main() {
    let (tx, rx) = bounded::<String, 32>();

    tokio::spawn(async move {
        for i in 0..5u64 {
            tx.send_async(format!("msg-{i}")).await.unwrap();
        }
    });

    let mut stream = std::pin::pin!(rx.stream());
    while let Some(msg) = stream.next().await {
        println!("{msg}");
    }
}