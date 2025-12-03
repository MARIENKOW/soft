use dotenvy::dotenv;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::{
    collections::HashSet,
    env,
    sync::Arc,
    time::Instant,
};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tokio::net::TcpStream;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = futures_util::stream::SplitSink<WsStream, Message>;
type WsStreamSplit = futures_util::stream::SplitStream<WsStream>;

#[derive(Clone)]
struct Config {
    access_token: String,
    min_amount: i64,
    max_amount: i64,
    take_orders: bool,
    timeout_ms: u64,
}

#[derive(Default)]
struct Stats {
    total: u64,
    filtered: u64,
    taken: u64,
    failed: u64,
    timeouts: u64,
}

struct Bot {
    config: Config,
    processed: Arc<Mutex<HashSet<String>>>,
    stats: Arc<Mutex<Stats>>,
    client: Client,
}

impl Bot {
    fn new(config: Config) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .expect("Failed to build reqwest client");

        Self {
            config,
            processed: Arc::new(Mutex::new(HashSet::new())),
            stats: Arc::new(Mutex::new(Stats::default())),
            client,
        }
    }

    async fn run(self: Arc<Self>) {
        loop {
            println!("🔌 Connecting to WebSocket...");
            // URL
            let url = "wss://app.cr.bot/internal/v1/p2c-socket/?EIO=4&transport=websocket";

            // connect
            let ws_res = connect_async(url).await;
            let (ws_stream, _) = match ws_res {
                Ok(pair) => pair,
                Err(e) => {
                    eprintln!("❌ WebSocket connect error: {}", e);
                    sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };

            println!("✅ WS connected");

            let (write_raw, mut read): (WsSink, WsStreamSplit) = ws_stream.split();

            let write = Arc::new(Mutex::new(write_raw));

            // send Socket.IO handshake sequence (spawned tasks so delays don't block)
            {
                let w = write.clone();
                tokio::spawn(async move {
                    let steps = vec![
                        (10u64, "0"),
                        (50u64, "40"),
                        (100u64, r#"42["list:initialize"]"#),
                    ];
                    for (delay, msg) in steps {
                        sleep(Duration::from_millis(delay)).await;
                        let mut sink = w.lock().await;
                        let _ = sink.send(Message::Text(msg.to_string())).await;
                        // ignore result; reconnect logic will handle failures
                    }
                });
            }

            // spawn a lightweight ping task (optional)
            {
                let w = write.clone();
                tokio::spawn(async move {
                    loop {
                        sleep(Duration::from_secs(20)).await;
                        let mut sink = w.lock().await;
                        let _ = sink.send(Message::Text("3".to_string())).await; // pong? send as keepalive
                    }
                });
            }

            // read loop
            while let Some(msg_res) = read.next().await {
                let msg = match msg_res {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("WS read error: {}", e);
                        break;
                    }
                };

                // only care about text messages
                let text = match msg {
                    Message::Text(t) => t,
                    Message::Binary(_) => continue,
                    Message::Ping(_) => continue,
                    Message::Pong(_) => continue,
                    Message::Close(_) => {
                        println!("WS closed by server");
                        break;
                    }
                };

                // handle socket.io heartbeats and prefixes
                if text == "2" {
                    // server ping -> send pong "3"
                    let mut sink = write.lock().await;
                    let _ = sink.send(Message::Text("3".to_string())).await;
                    continue;
                }
                if text == "3" { continue; }
                if text.starts_with("40") { continue; }
                if text.starts_with("0") {
                    // sometimes contains handshake info; ignore
                    continue;
                }

                if text.starts_with(r#"42["list:snapshot""#) {
                    println!("✅ SNAPSHOT received, waiting orders...");
                    continue;
                }

                if text.starts_with(r#"42["list:update""#) {
                    let me = self.clone();
                    // handle update in background to not block read loop
                    tokio::spawn(async move {
                        me.handle_list_update(&text).await;
                    });
                    continue;
                }

                // other 42 events
                if text.starts_with("42") {
                    // optional: parse general events
                    if let Ok(payload) = serde_json::from_str::<Value>(&text[2..]) {
                        println!("📨 Other event: {}", payload);
                    }
                }
            }

            println!("🔌 WS disconnected, reconnect in 3s...");
            sleep(Duration::from_secs(3)).await;
        }
    }

    async fn handle_list_update(self: Arc<Self>, text: &str) {
        // increment total
        {
            let mut s = self.stats.lock().await;
            s.total += 1;
        }

        // fast search for "data":{...}
        let data_pos = match text.find("\"data\":") {
            Some(p) => p + 7,
            None => return,
        };

        // find first '{' after data_pos
        let slice = &text[data_pos..];
        let start_rel = match slice.find('{') {
            Some(i) => i,
            None => return,
        };
        let mut bracket = 0isize;
        let mut end_rel = None;
        for (i, ch) in slice[start_rel..].char_indices() {
            match ch {
                '{' => bracket += 1,
                '}' => {
                    bracket -= 1;
                    if bracket == 0 {
                        end_rel = Some(start_rel + i + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        let end_rel = match end_rel {
            Some(v) => v,
            None => return,
        };

        let json_str = &slice[start_rel..end_rel];

        let order: Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(_) => return,
        };

        let id = match order.get("id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => return,
        };

        // dedupe
        {
            let mut processed = self.processed.lock().await;
            if processed.contains(&id) {
                return;
            }
            processed.insert(id.clone());
        }

        // validate
        let amount = order.get("in_amount").and_then(|v| v.as_f64()).unwrap_or(0.0).trunc() as i64;
        let asset = order.get("in_asset").and_then(|v| v.as_str()).unwrap_or("");

        if amount < self.config.min_amount || amount > self.config.max_amount || asset != "RUB" {
            return;
        }

        {
            let mut s = self.stats.lock().await;
            s.filtered += 1;
        }

        if self.config.take_orders {
            let me = self.clone();
            tokio::spawn(async move {
                let ok = me.take_order(&id).await;
                if ok {
                    // taken increment done inside take_order
                }
            });
        }
    }

    async fn take_order(self: Arc<Self>, id: &str) -> bool {
        let url = format!("https://app.cr.bot/internal/v1/p2c/payments/take/{}", id);
        let cookie = format!("access_token={}", self.config.access_token);
        let start = Instant::now();

        let req = self.client.post(&url)
            .header("Cookie", cookie)
            .header("Origin", "https://app.cr.bot")
            .header("Referer", "https://app.cr.bot/p2c")
            .header("User-Agent", "Mozilla/5.0 (compatible)");

        // use tokio timeout wrapper around the send
        match timeout(Duration::from_millis(self.config.timeout_ms), req.send()).await {
            Err(_) => {
                let mut s = self.stats.lock().await;
                s.timeouts += 1;
                println!("⏰ TAKE timeout for id {}", id);
                return false;
            }
            Ok(Err(e)) => {
                let mut s = self.stats.lock().await;
                s.failed += 1;
                eprintln!("❌ TAKE request error: {}", e);
                return false;
            }
            Ok(Ok(resp)) => {
                let elapsed = start.elapsed().as_millis();
                println!("⚡ RTT {} ms for id {}", elapsed, id);

                if resp.status().as_u16() == 200 {
                    let mut s = self.stats.lock().await;
                    s.taken += 1;
                    println!("✅ ORDER TAKEN {}", id);
                    // optionally print link if response contains data
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(order_data) = json.get("data") {
                            if let Some(order_id) = order_data.get("id").and_then(|v| v.as_str()) {
                                println!("Payment link -> https://app.cr.bot/p2c/orders/{}?back=payments", order_id);
                            }
                        }
                    }
                    return true;
                } else {
                    let mut s = self.stats.lock().await;
                    s.failed += 1;
                    // try to parse error
                    if let Ok(j) = resp.json::<Value>().await {
                        if j.get("error").and_then(|v| v.as_str()) == Some("ActiveOrderExists") {
                            println!("❌ ActiveOrderExists - need to pay old order");
                        } else {
                            println!("❌ Not taken: status {}", resp.status());
                        }
                    } else {
                        println!("❌ Not taken: status {}", resp.status());
                    }
                }
            }
        }

        false
    }
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    let cfg = Config {
        access_token: env::var("ACCESS_TOKEN").unwrap_or_default(),
        min_amount: env::var("MIN_AMOUNT").unwrap_or("500".into()).parse().unwrap_or(500),
        max_amount: env::var("MAX_AMOUNT").unwrap_or("7500".into()).parse().unwrap_or(7500),
        take_orders: env::var("TAKE_ORDERS").unwrap_or("true".into()) == "true",
        timeout_ms: env::var("REQUEST_TIMEOUT").unwrap_or("15000".into()).parse().unwrap_or(15000),
    };

    println!("ACCESS_TOKEN prefix: {}", &cfg.access_token.chars().take(8).collect::<String>());

    let bot = Arc::new(Bot::new(cfg.clone()));

    // stats printer
    {
        let stats = bot.stats.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(15)).await;
                let s = stats.lock().await;
                println!("📈 STATS: total={}, filtered={}, taken={}, failed={}, timeouts={}",
                    s.total, s.filtered, s.taken, s.failed, s.timeouts);
            }
        });
    }

    // run main loop
    bot.run().await;
}
