use axum::{
    extract::Extension,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use std::{collections::HashMap, net::SocketAddr};
use std::{
    sync::{mpsc, Arc, RwLock},
    thread, time,
};
use tokio::{join, runtime::Handle};
use youtubei_rs::{
    query::browse_id,
    types::{client::ClientConfig, misc::TabRendererContent},
    utils::default_client_config,
};

#[tokio::main]
async fn main() {
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
            executer_thread(state, tk_rt_handle, receiver);
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
        thread::sleep(time::Duration::from_secs(60));
    }
}
fn executer_thread(
    state: Arc<RwLock<State>>,
    handle: Arc<RwLock<Handle>>,
    rx: mpsc::Receiver<String>,
) {
    loop {
        let mut lock = state.write().unwrap();
        match lock.current_channel_stack.pop() {
            Some(v) => Some(
                handle
                    .read()
                    .unwrap()
                    .spawn(async move { println!("{}",query_channel(v.clone()).await) }),
            ),
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
            TabRendererContent::SectionListRenderer(content) => match content.contents.get(2).unwrap(){
                youtubei_rs::types::misc::ItemSectionRendererContents::ItemSectionRenderer(content) => match content.contents.get(0).unwrap(){
                        youtubei_rs::types::misc::ItemSectionRendererContents::ShelfRenderer(renderer) => match renderer.content.horizontal_list_renderer.as_ref().unwrap().items.get(0).unwrap(){
                            youtubei_rs::types::misc::ItemSectionRendererContents::GridVideoRenderer(video) => video.video_id.clone(),
                            _ => unreachable!()
                        }
                        _ => unreachable!()
                },
                _ => unreachable!(),

            },
            _ => unreachable!()
        },
        Err(_) => return String::from(""),
    }
}

async fn add_channel(
    Json(payload): Json<Channel>,
    Extension(state): Extension<Arc<RwLock<State>>>,
) -> StatusCode {
    println!("added {}", payload.id);
    state.write().unwrap().channels_to_refresh.push(payload.id);
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
}

struct State {
    // <Channel_id, Video_id>
    channels_video_ids: HashMap<String, String>,
    channels_to_refresh: Vec<String>,
    current_channel_stack: Vec<String>,
}
