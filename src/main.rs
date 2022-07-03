use axum::{extract::Extension, http::StatusCode, routing::post, Json, Router};
use chrono::Duration;
use quick_xml::{events::Event, Reader};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};

use rusted_invidious_types::database::{db_manager::DbManager, models::video::Video};
use scylla::frame::value::Timestamp;
use serde::Deserialize;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{mpsc, Arc, RwLock},
    thread, time,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{join, runtime::Handle, sync::Mutex};
use tracing::Level;
use youtubei_rs::{
    query::{next_video_id, player},
    types::{client::ClientConfig as YTClientConfig, query_results::NextResult},
    utils::default_client_config,
};

#[tokio::main]
async fn main() {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create::<FutureProducer>()
        .unwrap();
    // initialize tracing
    tracing_subscriber::fmt::init();
    let tk_rt_handle: Arc<RwLock<Handle>> = Arc::new(RwLock::new(Handle::current()));
    let syncstate = Arc::new(RwLock::new(State {
        channels_video_ids: HashMap::new(),
        channels_to_refresh: Vec::new(),
        current_channel_stack: Vec::new(),
    }));
    let asyncstate = Arc::new(Mutex::new(default_client_config()));
    let (sender, receiver) = mpsc::channel::<String>();
    let handle_fill = {
        let state = syncstate.clone();
        thread::spawn(move || {
            fill_stack(state);
        })
    };
    let db = DbManager::new("192.168.100.100:19042", None).await.unwrap();
    let handle_exec = {
        let state = syncstate.clone();
        thread::spawn(move || {
            executer_thread(
                state,
                tk_rt_handle,
                receiver,
                Arc::new(producer),
                Arc::new(db),
                asyncstate,
            );
        })
    };
    let app = Router::new()
        .route("/add_channel", post(add_channel))
        .route("/remove_channel", post(remove_channel))
        .layer(Extension(syncstate));
    let addr = SocketAddr::from(([127, 0, 0, 1], 3001));

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
    kafka_producer: Arc<FutureProducer>,
    db_manager: Arc<DbManager>,
    async_state: Arc<Mutex<YTClientConfig>>,
) {
    loop {
        let mut lock = state.write().unwrap();
        match lock.current_channel_stack.pop() {
            Some(cid) => {
                let s = state.clone();
                let kafka_producer = kafka_producer.clone();
                let db_manager = db_manager.clone();
                let async_state = async_state.clone();
                Some(handle.read().unwrap().spawn(async move {
                    let vid = query_channel(cid.clone(), async_state).await;
                    if !vid.is_empty()
                        && s.read().unwrap().channels_video_ids.get(&cid).unwrap() != &vid
                    {
                        println!("New video {}", vid);
                        s.write()
                            .unwrap()
                            .channels_video_ids
                            .insert(cid.clone(), vid.clone());
                        let dbcall = fetch_video(vid.clone(), cid.clone());
                        let payload =
                            format!(r#"{{"channel_id":"{}", "video_id":"{}" }}"#, &cid, &vid);
                        // TODO: Notify the notification manager about the new video
                        let kafkacall = kafka_producer.send(
                            FutureRecord::to("video_feed").payload(&payload).key(""),
                            std::time::Duration::from_secs(0),
                        );
                        let (reskafka, video) = join!(kafkacall, dbcall); // TODO: Add tracing here to track errors
                        reskafka.unwrap();
                        db_manager.insert_video(video).await;
                        return;
                    }
                    println!("No new video: {}", vid);
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
async fn query_channel(channel_id: String, async_state: Arc<Mutex<YTClientConfig>>) -> String {
    println!("updated {}", channel_id);
    let uri = format!(
        "https://www.youtube.com/feeds/videos.xml?channel_id={}",
        channel_id
    );
    let res = async_state.lock().await.http_client.get(uri).send().await;
    match res {
        Ok(res) => {
            return extract_xml(&res.text().await.unwrap())
                .get(0)
                .unwrap()
                .clone();
            // for now just take the first but should be changed to check if it contains current video_id if not return all ids or ids up to current video_id
        }
        Err(e) => tracing::event!(target:"yt", Level::ERROR, "{}", e.to_string()),
    };
    todo!()
}

fn extract_xml(xml: &String) -> Vec<String> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);
    let mut buf = Vec::new();
    let mut txt = Vec::new();
    // The `Reader` does not implement `Iterator` because it outputs borrowed data (`Cow`s)
    loop {
        match reader.read_event(&mut buf) {
            // for triggering namespaced events, use this instead:
            // match reader.read_namespaced_event(&mut buf) {
            // unescape and decode the text event using the reader encoding
            Ok(Event::Start(ref e)) if e.name().as_ref() == b"yt:videoId" => {
                txt.push(
                    reader
                        .read_text(b"yt:videoId", &mut Vec::new())
                        .expect("Cannot decode text value"),
                );
            }
            Ok(Event::Eof) => break, // exits the loop when reaching end of file
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            _ => (), // There are several other `Event`s we do not consider here
        }

        // if we don't keep a borrow elsewhere, we can clear the buffer to keep memory usage low
        buf.clear();
    }
    txt
}

fn get_video_from_shelf(shelf: &youtubei_rs::types::misc::ShelfRenderer) -> String {
    println!(
        "{}",
        shelf.title.runs.as_ref().unwrap().get(0).unwrap().text
    );
    match shelf
        .content
        .horizontal_list_renderer
        .as_ref()
        .unwrap()
        .items
        .get(0)
        .unwrap()
    {
        youtubei_rs::types::misc::ItemSectionRendererContents::GridVideoRenderer(vid) => {
            return vid.video_id.clone()
        }
        _ => return String::from(""),
    };
}

async fn add_channel(
    Json(payload): Json<Channel>,
    Extension(state): Extension<Arc<RwLock<State>>>,
) -> StatusCode {
    println!("added {}", payload.channel_id);
    state
        .write()
        .unwrap()
        .channels_to_refresh
        .push(payload.channel_id.clone());
    state
        .write()
        .unwrap()
        .channels_video_ids
        .insert(payload.channel_id, payload.newest_video_id);
    StatusCode::CREATED
}

async fn remove_channel(
    Json(payload): Json<Channel>,
    Extension(state): Extension<Arc<RwLock<State>>>,
) -> StatusCode {
    println!("removed {}", payload.channel_id);
    let mut lock = state.write().unwrap();
    for i in 0..lock.channels_to_refresh.iter().len() - 1 {
        if lock.channels_to_refresh[i] == payload.channel_id {
            lock.channels_to_refresh.remove(i);
            break;
        }
    }

    StatusCode::OK
}
#[derive(Deserialize)]
struct Channel {
    channel_id: String,
    newest_video_id: String,
}

struct State {
    // <Channel_id, Video_id>
    channels_video_ids: HashMap<String, String>,
    channels_to_refresh: Vec<String>,
    current_channel_stack: Vec<String>,
}

async fn fetch_video(video_id: String, channel_id: String) -> Video {
    let client = default_client_config();
    let next = next_video_id(video_id.clone(), "".to_string(), &client);
    let player = player(video_id.clone(), "".to_string(), &client);
    let (vid_next, vid_player) = join!(next, player);
    let vid_next = vid_next.unwrap();
    let vid_player = vid_player.unwrap();
    let video = Video {
        video_id,
        updated_at: Timestamp(Duration::seconds(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .try_into()
                .unwrap(),
        )),
        channel_id,
        title: vid_player.video_details.title,
        likes: get_likes(&vid_next),
        view_count: vid_player.video_details.view_count.parse().unwrap(),
        length_in_seconds: vid_player.video_details.length_seconds.parse().unwrap(),
        description: get_description(&vid_next),
        genre: vid_player.microformat.player_microformat_renderer.category,
        genre_url: String::from(""),
        license: String::from(""),
        author_verified: get_author_verified(&vid_next),
        subcriber_count: get_subcribe_count(&vid_next),
        author_name: vid_player.video_details.author,
        author_thumbnail_url: get_owner_thumbnail(&vid_next),
        is_famliy_safe: vid_player
            .microformat
            .player_microformat_renderer
            .is_family_safe,
        publish_date: vid_player
            .microformat
            .player_microformat_renderer
            .publish_date,
        formats: String::from(""),
        storyboard_spec_url: vid_player.storyboards.player_storyboard_spec_renderer.spec,
        continuation_comments: get_continuation_comments(&vid_next),
        continuation_related: get_continuation_related(&vid_next),
    };
    video
}

fn get_likes(video: &NextResult) -> String {
    match video
        .contents
        .as_ref()
        .unwrap()
        .two_column_watch_next_results
        .as_ref()
        .unwrap()
        .results
        .results
        .contents
        .get(0)
        .unwrap()
    {
        youtubei_rs::types::misc::NextContents::VideoPrimaryInfoRenderer(vpir) => match vpir
            .video_actions
            .menu_renderer
            .as_ref()
            .unwrap()
            .top_level_buttons
            .as_ref()
            .unwrap()
            .get(0)
            .unwrap()
        {
            youtubei_rs::types::misc::TopLevelButtons::ButtonRenderer(_) => unreachable!(),
            youtubei_rs::types::misc::TopLevelButtons::ToggleButtonRenderer(btn) => btn
                .default_text
                .accessibility
                .as_ref()
                .unwrap()
                .accessibility_data
                .label
                .clone(),
        },
        _ => unreachable!(),
    }
}

fn get_description(video: &NextResult) -> String {
    let mut desc = String::from("");
    match video
        .contents
        .as_ref()
        .unwrap()
        .two_column_watch_next_results
        .as_ref()
        .unwrap()
        .results
        .results
        .contents
        .get(1)
        .unwrap()
    {
        youtubei_rs::types::misc::NextContents::VideoSecondaryInfoRenderer(vsir) => {
            for text in vsir.description.runs.iter() {
                desc.push_str(text.text.clone().as_str());
            }
        }
        _ => unreachable!(),
    }
    desc
}

fn get_author_verified(video: &NextResult) -> bool {
    match video
        .contents
        .as_ref()
        .unwrap()
        .two_column_watch_next_results
        .as_ref()
        .unwrap()
        .results
        .results
        .contents
        .get(1)
        .unwrap()
    {
        youtubei_rs::types::misc::NextContents::VideoSecondaryInfoRenderer(vsir) => {
            match &vsir.owner.video_owner_renderer.badges {
                Some(badges) => match badges
                    .get(0)
                    .unwrap()
                    .metadata_badge_renderer
                    .icon
                    .as_ref()
                    .unwrap()
                    .icon_type
                    .as_str()
                {
                    "OFFICIAL_ARTIST_BADGE" => true,
                    "CHECK_CIRCLE_THICK" => true,
                    _ => false,
                },
                None => false,
            }
        }
        _ => unreachable!(),
    }
}

fn get_subcribe_count(video: &NextResult) -> String {
    match video
        .contents
        .as_ref()
        .unwrap()
        .two_column_watch_next_results
        .as_ref()
        .unwrap()
        .results
        .results
        .contents
        .get(1)
        .unwrap()
    {
        youtubei_rs::types::misc::NextContents::VideoSecondaryInfoRenderer(vsir) => {
            match &vsir.owner.video_owner_renderer.subscriber_count_text {
                Some(text) => text.simple_text.clone(),
                None => String::from(""),
            }
        }
        _ => unreachable!(),
    }
}

fn get_owner_thumbnail(video: &NextResult) -> String {
    match video
        .contents
        .as_ref()
        .unwrap()
        .two_column_watch_next_results
        .as_ref()
        .unwrap()
        .results
        .results
        .contents
        .get(1)
        .unwrap()
    {
        youtubei_rs::types::misc::NextContents::VideoSecondaryInfoRenderer(vsir) => vsir
            .owner
            .video_owner_renderer
            .thumbnail
            .thumbnails
            .get(0)
            .unwrap()
            .url
            .clone(),
        _ => unreachable!(),
    }
}
fn get_continuation_comments(video: &NextResult) -> Option<String> {
    match video
        .contents
        .as_ref()
        .unwrap()
        .two_column_watch_next_results
        .as_ref()
        .unwrap()
        .results
        .results
        .contents
        .last()
        .unwrap()
    {
        youtubei_rs::types::misc::NextContents::ContinuationItemRenderer(cir) => {
            match &cir.continuation_endpoint.continuation_command {
                Some(cmd) => Some(cmd.token.clone()),
                None => None,
            }
        }
        _ => None,
    }
}
fn get_continuation_related(video: &NextResult) -> Option<String> {
    match video
        .contents
        .as_ref()
        .unwrap()
        .two_column_watch_next_results
        .as_ref()
        .unwrap()
        .results
        .results
        .contents
        .last()
        .unwrap()
    {
        youtubei_rs::types::misc::NextContents::ContinuationItemRenderer(cir) => {
            match &cir.continuation_endpoint.continuation_command {
                Some(cmd) => Some(cmd.token.clone()),
                None => None,
            }
        }
        _ => None,
    }
}
