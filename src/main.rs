use bytes::Bytes;
use mini_redis::Command::{self, Get, Set};
use mini_redis::{Connection, Frame};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

type SharededDb = Arc<Vec<Mutex<HashMap<String, Bytes>>>>;
const DB_SIZE: usize = 8;

fn key_to_shard_index<T>(obj: T) -> usize
where
    T: std::hash::Hash,
{
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    obj.hash(&mut hasher);
    hasher.finish() as usize % DB_SIZE
}

#[tokio::main]
async fn main() {
    let db = Arc::new({
        let mut r = Vec::new();
        for _ in 0..DB_SIZE {
            r.push(Mutex::new(HashMap::new()));
        }
        r
    });

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let db = db.clone();
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: SharededDb) {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let idx = key_to_shard_index(cmd.key());
                let mut shard = db[idx].lock().unwrap();
                println!("{:?}", cmd);
                shard.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let idx = key_to_shard_index(cmd.key());
                let shard = db[idx].lock().unwrap();
                if let Some(value) = shard.get(cmd.key()) {
                    println!("{:?}", cmd);
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => unimplemented!("{:?}", cmd),
        };
        connection.write_frame(&response).await.unwrap();
    }
}
