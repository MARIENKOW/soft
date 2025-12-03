use futures_util::{StreamExt, SinkExt};
use serde_json::Value;
use tokio_tungstenite::connect_async;
use tungstenite::protocol::Message;
use reqwest::Client;
use dotenv::dotenv;
use std::{
    env,
    collections::HashSet,
    time::Instant,
};
use tokio::time::{sleep, Duration};

#[derive(Clone)]
struct Config {
    access_token: String,
    min_amount: i64,
    max_amount: i64,
    take_orders: bool,
    timeout: u64,
}

struct Bot {
    config: Config,
    processed: HashSet<String>,
    stats: Stats,
    client: Client,
}

#[derive(Default)]
struct Stats {
    total: u64,
    filtered: u64,
    taken: u64,
    failed: u64,
    timeouts: u64,
}

impl Bot {
    fn new(config: Config) -> Self {
        Self {
            client: Client::builder()
                .cookie_store(true)
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap(),
            config,
            processed: HashSet::new(),
            stats: Stats::default(),
        }
    }

    async fn run(&mut self) {
        loop {
            println!("🔌 Connecting to WebSocket...");

            let url = "wss://app.cr.bot/internal/v1/p2c-socket/?EIO=4&transport=websocket";
            let header = format!("access_token={}", self.config.access_token);

            let (ws_stream, _) = connect_async(
                url,
            ).await.expect("WS connect failed");

            println!("✅ WS connected!");

            let (mut write, mut read) = ws_stream.split();

            // Socket.IO handshake
            let init_steps = vec![
                ("0", 10),
                ("40", 40),
                (r#"42["list:initialize"]"#, 70),
            ];

            for (msg, delay) in init_steps {
                let m = Message::Text(msg.to_string());
                tokio::spawn({
                    let mut w = write.clone();
                    async move {
                        sleep(Duration::from_millis(delay)).await;
                        let _ = w.send(m).await;
                    }
                });
            }

            while let Some(msg) = read.next().await {
                let msg = match msg {
                    Ok(Message::Text(t)) => t,
                    _ => continue,
                };

                if msg == "2" {
                    let _ = write.send(Message::Text("3".into())).await;
                    continue;
                }
                if msg.starts_with("40") { continue; }
                if msg.starts_with("0") { continue; }

                if msg.starts_with(r#"42["list:snapshot""#) {
                    println!("🔥 READY — P2C ORDERS COMING");
                    continue;
                }

                if msg.starts_with(r#"42["list:update""#) {
                    self.handle_order_msg(&msg).await;
                }
            }

            println!("❌ WS disconnected, reconnecting...");
            sleep(Duration::from_secs(3)).await;
        }
    }

    async fn handle_order_msg(&mut self, msg: &str) {
        self.stats.total += 1;

        let idx = match msg.find("\"data\":") {
            Some(i) => i + 7,
            None => return,
        };

        let json_slice = match msg[idx..].find('{') {
            Some(start) => &msg[idx + start..],
            None => return,
        };

        let mut brackets = 0usize;
        let mut end_pos = None;

        for (i, c) in json_slice.char_indices() {
            match c {
                '{' => brackets += 1,
                '}' => {
                    brackets -= 1;
                    if brackets == 0 {
                        end_pos = Some(i + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        let end_pos = match end_pos {
            Some(p) => p,
            None => return,
        };

        let raw = &json_slice[..end_pos];
        let order: Value = match serde_json::from_str(raw) {
            Ok(v) => v,
            Err(_) => return,
        };

        let id = order["id"].as_str().unwrap_or("").to_string();
        if self.processed.contains(&id) {
            return;
        }

        let amount = order["in_amount"].as_f64().unwrap_or(0.0) as i64;
        let asset = order["in_asset"].as_str().unwrap_or("");

        if amount < self.config.min_amount
            || amount > self.config.max_amount
            || asset != "RUB" {

            return;
        }

        self.stats.filtered += 1;
        self.processed.insert(id.clone());

        if self.config.take_orders {
            let _ = self.take_order(&id).await;
        }
    }

    async fn take_order(&mut self, id: &str) -> bool {
        let start = Instant::now();

        let url = format!(
            "https://app.cr.bot/internal/v1/p2c/payments/take/{}",
            id
        );

        let resp = tokio::time::timeout(
            Duration::from_millis(self.config.timeout),
            self.client.post(url)
                .header("Cookie", format!("access_token={}", self.config.access_token))
                .header("Origin", "https://app.cr.bot")
                .header("Referer", "https://app.cr.bot/p2c")
                .send(),
        ).await;

        match resp {
            Err(_) => {
                println!("⏰ timeout");
                self.stats.timeouts += 1;
                return false;
            }
            Ok(Err(e)) => {
                println!("❌ req error: {}", e);
                self.stats.failed += 1;
                return false;
            }
            Ok(Ok(r)) => {
                let status = r.status().as_u16();

                println!("⚡ RTT: {} ms", start.elapsed().as_millis());

                if status == 200 {
                    self.stats.taken += 1;
                    println!("✅ TAKEN!");
                    return true;
                } else {
                    self.stats.failed += 1;
                    println!("❌ NOT TAKEN ({})", status);
                }
            }
        }

        false
    }
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let config = Config {
        access_token: env::var("ACCESS_TOKEN").unwrap(),
        min_amount: env::var("MIN_AMOUNT").unwrap_or("500".into()).parse().unwrap(),
        max_amount: env::var("MAX_AMOUNT").unwrap_or("7500".into()).parse().unwrap(),
        take_orders: env::var("TAKE_ORDERS").unwrap_or("true".into()) == "true",
        timeout: env::var("REQUEST_TIMEOUT").unwrap_or("15000".into()).parse().unwrap(),
    };

    let mut bot = Bot::new(config);

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            println!(
                "📊 STATS: total={}, filtered={}, taken={}, failed={}",
                bot.stats.total,
                bot.stats.filtered,
                bot.stats.taken,
                bot.stats.failed
            );
        }
    });

    bot.run().await;
}
