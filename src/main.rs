use axum::{extract::Extension, http::StatusCode, routing::post, Json, Router};
use chrono::Duration;
use rdkafka::{ClientConfig, producer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use scylla::transport::errors::NewSessionError;
use scylla::{Session, SessionBuilder};
use scylla::prepared_statement::PreparedStatement;
use serde::Deserialize;
use tokio::join;
use tracing::Level;
use tracing_subscriber::registry::Data;
use youtubei_rs::query::{next_video_id, player};
use youtubei_rs::types::error::RequestError;
use youtubei_rs::types::query_results::NextResult;
use std::borrow::Cow;
use std::fmt::Debug;
use std::time::{SystemTime, UNIX_EPOCH};
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
    let db = Database::new("192.168.100.100:19042", None).await.unwrap();
    let handle_exec = {
        let state = state.clone();
        thread::spawn(move || {
            executer_thread(state, tk_rt_handle, receiver,Arc::new(producer), Arc::new(db));
        })
    };
    let app = Router::new()
        .route("/add_channel", post(add_channel))
        .route("/remove_channel", post(remove_channel))
        .layer(Extension(state));
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
    db_manager: Arc<Database>
) {
    loop {
        let mut lock = state.write().unwrap();
        match lock.current_channel_stack.pop() {
            Some(cid) => {
                let s = state.clone();
                let kafka_producer = kafka_producer.clone();
                let db_manager = db_manager.clone();
                Some(handle.read().unwrap().spawn(async move {
                    let vid = query_channel(cid.clone()).await;
                    if !vid.is_empty() && s.read().unwrap().channels_video_ids.get(&cid).unwrap() != &vid {
                        println!("New video {}", vid);
                        s.write().unwrap().channels_video_ids.insert(cid.clone(), vid.clone());
                        let dbcall= db_manager.fetch_and_insert(vid.clone(), cid.clone());
                        let payload = format!(r#"{{"channel_id"={}, "video_id={} }}"#, &cid, &vid);
                        // TODO: Notify the notification manager about the new video
                        let kafkacall= kafka_producer.send(FutureRecord::to("video_feed")
                        .payload(&payload)
                        .key(""), std::time::Duration::from_secs(0));
                        join!(kafkacall,dbcall).0.unwrap(); // TODO: Add tracing here to track errors
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
                            match shelf.title.runs.as_ref().unwrap().get(0).unwrap().text.as_str(){
                                "Uploads"=> return get_video_from_shelf(shelf),
                                "latest and greatest" => return get_video_from_shelf(shelf),
                                _ => continue,
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

fn get_video_from_shelf(shelf: &youtubei_rs::types::misc::ShelfRenderer) -> String {
    println!("{}",shelf.title.runs.as_ref().unwrap().get(0).unwrap().text);
    match shelf.content.horizontal_list_renderer.as_ref().unwrap().items.get(0).unwrap() {
            youtubei_rs::types::misc::ItemSectionRendererContents::GridVideoRenderer(vid) => return vid.video_id.clone(),
            _ => return String::from(""),
    };
}

async fn add_channel(
    Json(payload): Json<Channel>,
    Extension(state): Extension<Arc<RwLock<State>>>,
) -> StatusCode {
    println!("added {}", payload.channel_id);
    state.write().unwrap().channels_to_refresh.push(payload.channel_id.clone());
    state.write().unwrap().channels_video_ids.insert(payload.channel_id, payload.newest_video_id);
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

struct Database{
    session: Session,
    prepared_statement: PreparedStatement,
}
impl Database {
    /// Initializes a new DbManager struct and creates the database session
    async fn new(uri: &str, known_hosts: Option<Vec<String>>) -> Result<Self, NewSessionError>{
        let session_builder;
        if known_hosts.is_some(){
            session_builder = SessionBuilder::new().known_node(uri).known_nodes(&known_hosts.unwrap());
        }else{
            session_builder = SessionBuilder::new().known_node(uri);
        }
        
        match session_builder.build().await {
            Ok(session) => {
                match session.use_keyspace("rusted_invidious", false).await{
                    Ok(_) => tracing::event!(target:"db", Level::DEBUG, "Successfully set keyspace"),
                    Err(_) => panic!("KESPACE NOT FOUND EXISTING...."),
                }
                let prepared_statement = session.prepare("INSERT INTO videos (video_id, updated_at, channel_id, title, likes, view_count,\
                    description, length_in_seconds, genere, genre_url, license, author_verified, subcriber_count, author_name, author_thumbnail_url, is_famliy_safe, publish_date, formats, storyboard_spec_url, continuation_related, continuation_comments ) \
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").await.unwrap();
                Ok(Self { session, prepared_statement})
            },
            Err(err) => Err(err),
        }
    }
    async fn fetch_and_insert(&self,video_id:String,channel_id: String){
        let client = default_client_config();
        let next = next_video_id(video_id.clone(), "".to_string(), &client);
        let player = player(video_id.clone(), "".to_string(), &client);
        let (vid_next, vid_player) = join!(next,player);
        let vid_next = vid_next.unwrap();
        let vid_player = vid_player.unwrap();
        let video = Video{
            video_id,
            updated_at: Timestamp(Duration::seconds(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap().as_secs().try_into().unwrap())),
            channel_id,
            title: vid_player.video_details.title,
            likes: get_likes(&vid_next),
            view_count: vid_player.video_details.view_count.parse().unwrap(),
            length_in_seconds: vid_player.video_details.length_seconds.parse().unwrap(),
            description: get_description(&vid_next),
            genre: vid_player.microformat.player_microformat_renderer.category,
            genre_url: String::from(""),
            license: String::from(""),
            author_verified:get_author_verified(&vid_next),
            subcriber_count: get_subcribe_count(&vid_next),
            author_name: vid_player.video_details.author,
            author_thumbnail_url: get_owner_thumbnail(&vid_next),
            is_famliy_safe:vid_player.microformat.player_microformat_renderer.is_family_safe,
            publish_date: vid_player.microformat.player_microformat_renderer.publish_date,
            formats:String::from(""),
            storyboard_spec_url: vid_player.storyboards.player_storyboard_spec_renderer.spec,
            continuation_comments: get_continuation_comments(&vid_next),
            continuation_related: get_continuation_related(&vid_next),
        };
        print!("{}", video.title);
        let res = self.insert_video(video).await;
        if !res {
            panic!("Failed to insert video: {}", res);
        }

    }
    pub async fn insert_video(&self, video: Video) -> bool{
        match self.session.execute(&self.prepared_statement,
        video ).await{
            Ok(_) =>  true,
            Err(e) => {
                println!("Failed to insert video: {}", e);
                false
            },
        }
    }
}


use scylla::macros::{IntoUserType,FromUserType,FromRow};
use scylla::cql_to_rust::FromCqlVal;
use scylla::frame::value::{Timestamp, ValueList, SerializedResult, SerializedValues};

/// Temporary needs to be in serpart crate aka rusted-invidious-types
/// Represents a video queried from the database
#[derive(Debug,IntoUserType, FromUserType,FromRow)]
pub struct Video{
    pub video_id: String, // Primary Key -> partition key
    pub updated_at: Timestamp,
    pub channel_id: String,
    pub title: String,
    pub likes: String,
    pub view_count: i64,
    pub length_in_seconds: i64,
    pub description: String,
    pub genre: String,
    pub genre_url: String,
    pub license: String,
    pub author_verified: bool,
    pub subcriber_count: String,
    pub author_name: String,
    pub author_thumbnail_url: String,
    pub is_famliy_safe: bool,
    pub publish_date: String,
    pub formats:String, // This string contains json that holds all formats for the video. Could be stored in own table I think
    pub storyboard_spec_url: String,
    pub continuation_comments: Option<String>,
    pub continuation_related: Option<String>,
}
impl ValueList for Video {
    fn serialized(&self) -> SerializedResult {
        let mut result = SerializedValues::with_capacity(2);
        result.add_value(&self.video_id)?;
        result.add_value(&self.updated_at)?;
        result.add_value(&self.channel_id)?;
        result.add_value(&self.title)?;
        result.add_value(&self.likes)?;
        result.add_value(&self.view_count)?;
        result.add_value(&self.length_in_seconds)?;
        result.add_value(&self.description)?;
        result.add_value(&self.genre)?;
        result.add_value(&self.genre_url)?;
        result.add_value(&self.license)?;
        result.add_value(&self.author_verified)?;
        result.add_value(&self.subcriber_count)?;
        result.add_value(&self.author_name)?;
        result.add_value(&self.author_thumbnail_url)?;
        result.add_value(&self.is_famliy_safe)?;
        result.add_value(&self.publish_date)?;
        result.add_value(&self.formats)?;
        result.add_value(&self.storyboard_spec_url)?;
        result.add_value(&self.continuation_comments)?;
        result.add_value(&self.continuation_related)?;

        Ok(Cow::Owned(result))
    }
}
fn get_likes(video: &NextResult) -> String{
    match video.contents.as_ref().unwrap().two_column_watch_next_results.as_ref().unwrap().results.results.contents.get(0).unwrap() {
        youtubei_rs::types::misc::NextContents::VideoPrimaryInfoRenderer(vpir) => match vpir.video_actions.menu_renderer.as_ref().unwrap().top_level_buttons.as_ref().unwrap().get(0).unwrap(){
            youtubei_rs::types::misc::TopLevelButtons::ButtonRenderer(_) => unreachable!(),
            youtubei_rs::types::misc::TopLevelButtons::ToggleButtonRenderer(btn) => btn.default_text.accessibility.as_ref().unwrap().accessibility_data.label.clone(),
        },
        _ => unreachable!()
    }
}

fn get_description(video: &NextResult) -> String{
    let mut desc = String::from("");
    match video.contents.as_ref().unwrap().two_column_watch_next_results.as_ref().unwrap().results.results.contents.get(1).unwrap() {
        youtubei_rs::types::misc::NextContents::VideoSecondaryInfoRenderer(vsir) => 
        for text in vsir.description.runs.iter(){
            desc.push_str(text.text.clone().as_str());
        },
        _ => unreachable!()
    }
    desc
}

fn get_author_verified(video: &NextResult) -> bool{
    match video.contents.as_ref().unwrap().two_column_watch_next_results.as_ref().unwrap().results.results.contents.get(1).unwrap() {
        youtubei_rs::types::misc::NextContents::VideoSecondaryInfoRenderer(vsir) => match &vsir.owner.video_owner_renderer.badges{
            Some(badges) => match badges.get(0).unwrap().metadata_badge_renderer.icon.as_ref().unwrap().icon_type.as_str(){
                "OFFICIAL_ARTIST_BADGE" => true,
                "CHECK_CIRCLE_THICK"  => true,
                _ => false
            },
            None => false,
        },
        _ => unreachable!()
    }
}

fn get_subcribe_count(video: &NextResult) -> String{
    match video.contents.as_ref().unwrap().two_column_watch_next_results.as_ref().unwrap().results.results.contents.get(1).unwrap() {
        youtubei_rs::types::misc::NextContents::VideoSecondaryInfoRenderer(vsir) => match &vsir.owner.video_owner_renderer.subscriber_count_text{
            Some(text) => text.simple_text.clone(),
            None => String::from(""),
        },
        _ => unreachable!()
    }
}

fn get_owner_thumbnail(video: &NextResult) -> String{
    match video.contents.as_ref().unwrap().two_column_watch_next_results.as_ref().unwrap().results.results.contents.get(1).unwrap() {
        youtubei_rs::types::misc::NextContents::VideoSecondaryInfoRenderer(vsir) => vsir.owner.video_owner_renderer.thumbnail.thumbnails.get(0).unwrap().url.clone(),
        _ => unreachable!()
    }
}
fn get_continuation_comments(video: &NextResult) -> Option<String>{
    match video.contents.as_ref().unwrap().two_column_watch_next_results.as_ref().unwrap().results.results.contents.last().unwrap() {
        youtubei_rs::types::misc::NextContents::ContinuationItemRenderer(cir) => match &cir.continuation_endpoint.continuation_command{
            Some(cmd) => Some(cmd.token.clone()),
            None => None,
        },
        _ => None
    }
}
fn get_continuation_related(video: &NextResult) -> Option<String>{
    match video.contents.as_ref().unwrap().two_column_watch_next_results.as_ref().unwrap().results.results.contents.last().unwrap()  {
        youtubei_rs::types::misc::NextContents::ContinuationItemRenderer(cir) => match &cir.continuation_endpoint.continuation_command{
            Some(cmd) => Some(cmd.token.clone()),
            None => None,
        },
        _ =>None
    }
}