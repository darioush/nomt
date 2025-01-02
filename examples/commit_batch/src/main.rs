use std::os::unix::net::{UnixStream, UnixListener};
use prost::Message;
use std::io::{Read, Write};
use nomt::{Nomt, Session, Blake3Hasher, Options, KeyReadWrite};
use sha2::Digest;
use clap::Parser;

// Import the generated types
mod database_interface {
    include!(concat!(env!("OUT_DIR"), "/database_interface.rs"));
}
use database_interface::{Request, Response, RootResponse, GetResponse, PrefetchResponse, UpdateResponse, CloseResponse};

fn handle_client(mut stream: UnixStream, nomt: &Nomt<Blake3Hasher>, session: Session) -> Session {
    let mut next_session = session;
    let mut buffer = vec![0; 1024*1024]; 

    loop {
        // Read the length of the incoming message
        let mut length_buffer = [0u8; 4];
        if stream.read_exact(&mut length_buffer).is_err() {
            break;
        }
        let message_length = u32::from_be_bytes(length_buffer) as usize;

        // Read the message based on the length
        if message_length > buffer.len() {
            buffer.resize(message_length, 0);
        }
        if stream.read_exact(&mut buffer[..message_length]).is_err() {
            break;
        }
        // Deserialize the request
        let request = Request::decode(&buffer[..message_length]).unwrap();

        // Example processing logic
        let response = match request.request.unwrap() {
            database_interface::request::Request::Root(_) => {
                Response {
                    err_code: 0,
                    response: Some(database_interface::response::Response::Root(RootResponse {
                        root: nomt.root().to_vec(),
                    })),
                }
            }
            database_interface::request::Request::Get(req) => {
                let key_path = sha2::Sha256::digest(&req.key).into();
                let value = next_session.read(key_path).unwrap();
                match value {
                    Some(value) => {
                        Response {
                            err_code: 0,
                            response: Some(database_interface::response::Response::Get(GetResponse {
                                value: value,
                            })),
                        }
                    }
                    None => {
                        Response {
                            err_code: 1,
                            response: None,
                        }
                    }
                }
            }
            database_interface::request::Request::Prefetch(req) => {
                let key_path = sha2::Sha256::digest(&req.key).into();
                next_session.warm_up(key_path);
                Response {
                    err_code: 0,
                    response: Some(database_interface::response::Response::Prefetch(PrefetchResponse {})),
                }
            }
            database_interface::request::Request::Update(req) => {
                let mut actual_access: Vec<_> = req.items.into_iter().map(|item| {
                    let key_path = sha2::Sha256::digest(&item.key).into();
                    let write_val = match item.value.len() {
                        0 => None,
                        _ => Some(item.value),
                    };
                    (key_path, KeyReadWrite::Write(write_val))
                }).collect();
                actual_access.sort_by_key(|(k, _)| *k);

                let root= nomt.commit(next_session, actual_access).unwrap();
                next_session = nomt.begin_session();
                Response {
                    err_code: 0,
                    response: Some(database_interface::response::Response::Update(UpdateResponse {
                        root: root.to_vec(),
                    })),
                }
            }
            database_interface::request::Request::Close(_) => {
                Response {
                    err_code: 0,
                    response: Some(database_interface::response::Response::Close(CloseResponse {})),
                }
            }
        };

        // Serialize the response
        let mut response_buffer = Vec::new();
        response.encode(&mut response_buffer).unwrap();
        stream.write_all(&response_buffer).unwrap();
    };
    

    // Return the session to be used in the next iteration
    next_session
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to the unix socket
    #[arg(short, long, default_value = "/tmp/rust_socket")]
    socket_path: String,

    /// Path to the database
    #[arg(short, long, default_value = "nomt_db")]
    path: String,

    /// Number of io workers
    #[arg(short, long, default_value = "4")]
    io_workers: usize,

    /// Number of commit workers
    #[arg(short, long, default_value = "1")]
    commit_concurrency: usize,


    // Number of hashtable buckets
    #[arg(short, long, default_value = "64000")]
    hashtable_buckets: u32,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let socket_path = &args.socket_path;
    if std::path::Path::new(socket_path).exists() {
        std::fs::remove_file(socket_path)?;
    }

    let listener = UnixListener::bind(socket_path)?;
    println!("Server listening on {}", socket_path);


    let mut opts = Options::new();
    opts.bitbox_seed([42; 16]); // Reproducibility
    opts.io_workers(args.io_workers);
    opts.commit_concurrency(args.commit_concurrency);
    opts.path(args.path);
    opts.hashtable_buckets(hashtable_buckets);

    let nomt = Nomt::<Blake3Hasher>::open(opts)?;
    let mut session = nomt.begin_session();

    // Main loop to handle client connections
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                session =  handle_client(stream, &nomt, session);
            },
            Err(err) => {
                eprintln!("Connection failed: {}", err);
            }
        }
    }

    Ok(())
}
