use bytes::Bytes;
use mini_redis::{client, Result};
use tokio::sync::mpsc;

#[derive(Debug)]
enum Command {
    Get { key: String },
    Set { key: String, val: Bytes },
}

#[tokio::main]
async fn main() -> Result<()> {
    let (tx, mut rx) = mpsc::channel(32);
    let tx2 = tx.clone();

    // Spawn a task which Gets
    let t1 = tokio::spawn(async move {
        let cmd = Command::Get {
            key: "foo".to_string(),
        };
        tx.send(cmd).await.unwrap();
    });

    // Spawn a task which Sets
    let t2 = tokio::spawn(async move {
        let cmd = Command::Set {
            key: "foo".to_string(),
            val: "bar".into(),
        };
        tx2.send(cmd).await.unwrap();
    });

    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Get { key } => {
                    let ret = client.get(&key).await.unwrap();
                    println!("{:?}", ret);
                }
                Command::Set { key, val } => {
                    client.set(&key, val).await.unwrap();
                }
            }
        }
    });

    // Join
    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();

    Ok(())
}
