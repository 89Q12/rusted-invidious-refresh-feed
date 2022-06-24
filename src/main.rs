use axum::{extract::Extension, http::StatusCode, routing::post, Json, Router};
use rdkafka::{ClientConfig, producer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Deserialize;
use tokio::join;
use youtubei_rs::query::{next_video_id, player};
use youtubei_rs::types::error::RequestError;
use std::time::Duration;
use std::{collections::HashMap, net::SocketAddr};
use std::{
    sync::{mpsc, Arc, RwLock},
    thread, time,
};
use tokio::runtime::Handle;

use youtubei_rs::{
    query::browse_id, types::misc::TabRendererContent, utils::default_client_config,
};

#[tokio::main]
async fn main() {
    let producer: FutureProducer =ClientConfig::new()
    .set("bootstrap.servers", "localhost:9092")
    .set("message.timeout.ms", "5000")
    .create::<FutureProducer>().unwrap();
    // initialize tracing
    tracing_subscriber::fmt::init();
    let tk_rt_handle: Arc<RwLock<Handle>> = Arc::new(RwLock::new(Handle::current()));
    let state = Arc::new(RwLock::new(State {
        channels_video_ids: HashMap::new(),
        channels_to_refresh: Vec::new(),
        current_channel_stack: Vec::new(),
    }));
    let (sender, receiver) = mpsc::channel::<String>();
    let handle_fill = {
        let state = state.clone();
        thread::spawn(move || {
            fill_stack(state);
        })
    };
    let handle_exec = {
        let state = state.clone();
        thread::spawn(move || {
            executer_thread(state, tk_rt_handle, receiver,Arc::new(producer));
        })
    };
    let app = Router::new()
        .route("/add_channel", post(add_channel))
        .route("/remove_channel", post(remove_channel))
        .layer(Extension(state));
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    println!("Sending term signal to");
    sender.send("term".to_string()).unwrap();
    handle_exec.join().unwrap();
    handle_fill.join().unwrap();
    println!("lol");
}
fn fill_stack(state: Arc<RwLock<State>>) {
    loop {
        println!("Filling stack");
        let lock = state.read().unwrap();
        let mut v: Vec<String> = Vec::new();
        for channel in lock.channels_to_refresh.iter() {
            v.push(channel.to_owned());
        }
        drop(lock);
        let mut lock = state.write().unwrap();
        v.iter()
            .for_each(|id| lock.current_channel_stack.push(id.to_owned()));
        drop(lock);
        println!("Waiting for 60 seconds...");
        thread::sleep(time::Duration::from_secs(10));
    }
}
fn executer_thread(
    state: Arc<RwLock<State>>,
    handle: Arc<RwLock<Handle>>,
    rx: mpsc::Receiver<String>,
    kafka_producer: Arc<FutureProducer>
) {
    loop {
        let mut lock = state.write().unwrap();
        match lock.current_channel_stack.pop() {
            Some(cid) => {
                let s = state.clone();
                let kafka_producer = kafka_producer.clone();
                Some(handle.read().unwrap().spawn(async move {
                    let vid = query_channel(cid.clone()).await;
                    if !vid.is_empty() && s.read().unwrap().channels_video_ids.get(&cid).unwrap() != &vid {
                        println!("New video {}", vid);
                        s.write().unwrap().channels_video_ids.insert(cid.clone(), vid.clone());
                        //Database::fetch_and_insert(vid.clone(), cid.clone()).await;
                        // TODO: Notify the notification manager about the new video
                        kafka_producer.send(FutureRecord::to("video_feed")
                        .payload(&format!(r#"{{"channel_id"={}, "video_id={} }}"#, &cid, &vid))
                        .key(""), Duration::from_secs(0)).await.unwrap();
                        return;
                    }
                    println!("No new video");
                }))
            }
            None => None,
        };
        drop(lock); // Drop the lock to unlock the state
        match rx.try_recv() {
            Ok(msg) => match msg.as_str() {
                "term" => {
                    println!("Terminating.");
                    break;
                }
                _ => continue,
            },
            Err(_) => continue,
        }
    }
    return;
}
async fn query_channel(channel_id: String) -> String {
    println!("updated {}", channel_id);
    let browse_data = browse_id(channel_id, "".to_string(), &default_client_config()).await;
    match browse_data {
        Ok(data) => match data.contents.unwrap().two_column_browse_results_renderer.unwrap().tabs.get(0).unwrap().tab_renderer.as_ref().unwrap().content.as_ref().unwrap(){
            TabRendererContent::SectionListRenderer(content) => for item in content.contents.iter(){
                match item{
                    youtubei_rs::types::misc::ItemSectionRendererContents::ItemSectionRenderer(item) => for item in item.contents.iter() {
                        match item {
                            youtubei_rs::types::misc::ItemSectionRendererContents::ShelfRenderer(shelf) => 
                            if shelf.title.runs.as_ref().unwrap().get(0).unwrap().text == "Uploads" {
                                    match shelf.content.horizontal_list_renderer.as_ref().unwrap().items.get(0).unwrap() {
                                        youtubei_rs::types::misc::ItemSectionRendererContents::GridVideoRenderer(vid) => return vid.video_id.clone(),
                                        _ => return String::from(""),
                                    };
                            }else{
                                    continue;
                            },
                            _ => continue,
                        };
                    },
                    _ => unreachable!(),
                };
            }
            _ => unreachable!()
        },
        Err(_) => return String::from(""),
    };
    return String::from("")
}

async fn add_channel(
    Json(payload): Json<Channel>,
    Extension(state): Extension<Arc<RwLock<State>>>,
) -> StatusCode {
    println!("added {}", payload.id);
    state.write().unwrap().channels_to_refresh.push(payload.id.clone());
    state.write().unwrap().channels_video_ids.insert(payload.id, payload.newest_video_id);
    StatusCode::CREATED
}

async fn remove_channel(
    Json(payload): Json<Channel>,
    Extension(state): Extension<Arc<RwLock<State>>>,
) -> StatusCode {
    println!("removed {}", payload.id);
    let mut lock = state.write().unwrap();
    for i in 0..lock.channels_to_refresh.iter().len() - 1 {
        if lock.channels_to_refresh[i] == payload.id {
            lock.channels_to_refresh.remove(i);
            break;
        }
    }

    StatusCode::OK
}
#[derive(Deserialize)]
struct Channel {
    id: String,
    newest_video_id: String,
}

struct State {
    // <Channel_id, Video_id>
    channels_video_ids: HashMap<String, String>,
    channels_to_refresh: Vec<String>,
    current_channel_stack: Vec<String>,
}

struct Database{

}
impl Database {
    async fn fetch_and_insert(video_id:String,channel_id: String){
        let client = default_client_config();
        let next = next_video_id(video_id.clone(), "".to_string(), &client);
        let player = player(video_id.clone(), "".to_string(), &client);
        let (vid_next, vid_player) = join!(next,player);
        println!("{}",vid_player.unwrap().video_details.title);
    }
}