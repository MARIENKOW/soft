use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time;
use tungstenite::protocol::Message;
use url::Url;
use serde::{Deserialize, Serialize};
use dotenv::dotenv;
use reqwest::Client;
use std::process;
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream};
use tokio::net::TcpStream;
use futures_util::{SinkExt, StreamExt};
use log::{info, error, warn};
use futures_util::stream::SplitSink;

#[derive(Debug, Clone)]
struct Config {
    access_token: String,
    min_amount: i32,
    max_amount: i32,
    take_orders: bool,
    request_timeout: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: String,
    in_amount: f64,
    in_asset: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TakeOrderResponse {
    data: Option<OrderData>,
    error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OrderData {
    id: String,
}

struct OptimizedP2POrderSnatcher {
    config: Config,
    processed_orders: Arc<Mutex<HashSet<String>>>,
    stats: Arc<Mutex<Stats>>,
    client: Client,
    is_running: Arc<Mutex<bool>>,
}

#[derive(Debug, Default)]
struct Stats {
    total: u64,
    filtered: u64,
    taken: u64,
    failed: u64,
    timeouts: u64,
}

impl OptimizedP2POrderSnatcher {
    fn new(config: Config) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.request_timeout))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            config,
            processed_orders: Arc::new(Mutex::new(HashSet::new())),
            stats: Arc::new(Mutex::new(Stats::default())),
            client,
            is_running: Arc::new(Mutex::new(true)),
        }
    }

    async fn start(&self) {
        info!("🚀 Запуск оптимизированного бота...");
        info!("🎯 Мин сумма поиска → {}", self.config.min_amount);
        info!("🎯 Макс сумма поиска → {}", self.config.max_amount);

        self.connect_websocket_with_retry().await;
    }

    async fn connect_websocket_with_retry(&self) {
        loop {
            if let Err(e) = self.try_connect_websocket().await {
                error!("❌ Ошибка подключения WebSocket: {}", e);
                warn!("🔌 Переподключаемся через 3 секунды...");
                time::sleep(Duration::from_secs(3)).await;
            } else {
                info!("✅ WebSocket соединение установлено и работает");
                time::sleep(Duration::from_secs(1)).await;
            }
            
            if !*self.is_running.lock().await {
                break;
            }
        }
    }

    async fn try_connect_websocket(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ws_url = "wss://app.cr.bot/internal/v1/p2c-socket/?EIO=4&transport=websocket";
        
        info!("🔌 Подключаемся к WebSocket...");

        // Простое подключение без заголовков (они не нужны для начального подключения)
        let (ws_stream, _) = connect_async(ws_url).await?;
        info!("✅ WebSocket подключен");
        
        let (write, mut read) = ws_stream.split();
        
        let write_arc = Arc::new(Mutex::new(write));
        
        // Отправка handshake
        self.send_socketio_handshake(write_arc.clone()).await?;

        // Клонируем данные
        let processed_orders_clone = self.processed_orders.clone();
        let stats_clone = self.stats.clone();
        let config_clone = self.config.clone();
        let client_clone = self.client.clone();
        let is_running_clone = self.is_running.clone();

        // Обработчик сообщений
        tokio::spawn(async move {
            while *is_running_clone.lock().await {
                match read.next().await {
                    Some(Ok(message)) => {
                        let start_time = Instant::now();
                        Self::process_websocket_message(
                            message,
                            &processed_orders_clone,
                            &stats_clone,
                            &config_clone,
                            &client_clone,
                            start_time,
                        ).await;
                    }
                    Some(Err(e)) => {
                        error!("❌ WebSocket ошибка: {}", e);
                        break;
                    }
                    None => {
                        warn!("📭 WebSocket поток закрыт");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    async fn send_socketio_handshake(
        &self, 
        write: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let handshake_sequence = vec![
            (Duration::from_millis(10), "0".to_string()),
            (Duration::from_millis(50), "40".to_string()),
            (Duration::from_millis(100), r#"42["list:initialize"]"#.to_string()),
        ];

        for (delay, message) in handshake_sequence {
            time::sleep(delay).await;
            let mut write_lock = write.lock().await;
            write_lock.send(Message::Text(message.clone())).await?;
            info!("📤 Отправляем: {}", message);
        }
        
        Ok(())
    }

    async fn process_websocket_message(
        message: Message,
        processed_orders: &Arc<Mutex<HashSet<String>>>,
        stats: &Arc<Mutex<Stats>>,
        config: &Config,
        client: &Client,
        start_time: Instant,
    ) {
        match message {
            Message::Text(text) => {
                // Ping-pong handling
                if text == "2" {
                    // Автоматически обрабатывается
                    return;
                }
                
                if text == "3" {
                    return;
                }
                
                if text.starts_with("40") {
                    return;
                }
                
                if text.starts_with('0') {
                    return;
                }

                if text.contains("list:snapshot") {
                    info!("✅ ВСЁ ПОДКЛЮЧЕНО УСПЕШНО → ОЖИДАЕМ ЗАКАЗЫ ✅ ");
                    info!("----------------------------------------------");
                    return;
                }

                if text.contains("list:update") {
                    Self::handle_list_update(text, processed_orders, stats, config, client, start_time).await;
                    return;
                }
            }
            Message::Ping(_) => {
                // Автоматически отвечает
            }
            Message::Close(_) => {
                warn!("📭 Получен Close фрейм");
            }
            _ => {}
        }
    }

    async fn handle_list_update(
        text: String,
        processed_orders: &Arc<Mutex<HashSet<String>>>,
        stats: &Arc<Mutex<Stats>>,
        config: &Config,
        client: &Client,
        start_time: Instant,
    ) {
        if let Some(data_start) = text.find("\"data\":") {
            let mut bracket_count = 0;
            let mut data_start_index = None;
            let mut data_end_index = None;
            
            let chars: Vec<char> = text.chars().collect();
            
            for i in data_start + 7..chars.len() {
                if chars[i] == '{' && data_start_index.is_none() {
                    data_start_index = Some(i);
                    bracket_count = 1;
                } else if chars[i] == '{' {
                    bracket_count += 1;
                } else if chars[i] == '}' {
                    bracket_count -= 1;
                    if bracket_count == 0 {
                        data_end_index = Some(i + 1);
                        break;
                    }
                }
            }
            
            if let (Some(start), Some(end)) = (data_start_index, data_end_index) {
                let order_json = &text[start..end];
                if let Ok(order) = serde_json::from_str::<Order>(order_json) {
                    Self::handle_new_order(order, processed_orders, stats, config, client, start_time).await;
                }
            }
        }
    }

    async fn handle_new_order(
        order: Order,
        processed_orders: &Arc<Mutex<HashSet<String>>>,
        stats: &Arc<Mutex<Stats>>,
        config: &Config,
        client: &Client,
        start_time: Instant,
    ) {
        let mut stats_lock = stats.lock().await;
        stats_lock.total += 1;
        drop(stats_lock);

        {
            let mut processed = processed_orders.lock().await;
            if processed.contains(&order.id) {
                return;
            }
            processed.insert(order.id.clone());
        }

        if !Self::is_valid_order(&order, config) {
            return;
        }

        {
            let mut stats_lock = stats.lock().await;
            stats_lock.filtered += 1;
        }

        info!("⚡ ЗАКАЗ {} {}", order.in_amount, order.in_asset);

        if config.take_orders {
            Self::take_order(&order.id, config, client, start_time, stats).await;
        }
    }

    fn is_valid_order(order: &Order, config: &Config) -> bool {
        let amount = order.in_amount as i32;
        
        if amount < config.min_amount || 
           order.in_asset != "RUB" || 
           amount > config.max_amount {
            return false;
        }
        
        true
    }

    async fn take_order(
        order_id: &str,
        config: &Config,
        client: &Client,
        start_time: Instant,
        stats: &Arc<Mutex<Stats>>,
    ) {
        let url = format!("https://app.cr.bot/internal/v1/p2c/payments/take/{}", order_id);
        
        let response = client.post(&url)
            .header("Cookie", format!("access_token={}", config.access_token))
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .header("Origin", "https://app.cr.bot")
            .header("Referer", "https://app.cr.bot/p2c")
            .header("Accept", "application/json, text/plain, */*")
            .header("Accept-Language", "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7")
            .header("Content-Type", "application/json")
            .send()
            .await;

        let elapsed = start_time.elapsed();
        info!("Время обработки: {} мс", elapsed.as_millis());

        match response {
            Ok(resp) => {
                let status = resp.status();
                let response_text = resp.text().await.unwrap_or_default();
                
                if status == 200 {
                    let mut stats_lock = stats.lock().await;
                    stats_lock.taken += 1;
                    info!("✅ ЗАКАЗ ВЗЯТ УСПЕШНО!");

                    if let Ok(parsed) = serde_json::from_str::<TakeOrderResponse>(&response_text) {
                        if let Some(order_data) = parsed.data {
                            let payment_link = format!("https://app.cr.bot/p2c/orders/{}?back=payments", order_data.id);
                            info!("Ссылка для оплаты - {}", payment_link);
                        }
                    }
                } else {
                    let mut stats_lock = stats.lock().await;
                    stats_lock.failed += 1;
                    
                    if response_text.contains("ActiveOrderExists") {
                        info!("❌ Нужно оплатить старый заказ.");
                    } else {
                        info!("❌ Не успели взять, статус: {}", status);
                    }
                }
            }
            Err(e) => {
                let mut stats_lock = stats.lock().await;
                if e.is_timeout() {
                    stats_lock.timeouts += 1;
                    info!("⏰ Таймаут...");
                } else {
                    stats_lock.failed += 1;
                    error!("❌ Ошибка: {}", e);
                }
            }
        }
    }

    async fn start_monitoring(&self) {
        let stats_clone = self.stats.clone();
        
        tokio::spawn(async move {
            loop {
                time::sleep(Duration::from_secs(15)).await;
                let stats = stats_clone.lock().await;
                info!(
                    "📈 СТАТИСТИКА: Всего {}, Подходят {}, Взято {}, Не взяли {}",
                    stats.total, stats.filtered, stats.taken, stats.failed
                );
            }
        });
    }

    async fn stop(&self) {
        let mut is_running = self.is_running.lock().await;
        *is_running = false;
        info!("🛑 Бот остановлен");
    }
}

impl Clone for OptimizedP2POrderSnatcher {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            processed_orders: self.processed_orders.clone(),
            stats: self.stats.clone(),
            client: self.client.clone(),
            is_running: self.is_running.clone(),
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();
    
    dotenv().ok();

    let config = Config {
        access_token: env::var("ACCESS_TOKEN").unwrap_or_default("SPbUyWmYQArPG1e6JDq2XPKzE-jl-IY7hLs1C98St30ejoDJ5uYLGQ74WpMnEat5.C%2BOi8FLYjP8rZDkPVg2wwfezBxJaz%2FkY1r3FZ%2Fel0%2B0"),
        min_amount: env::var("MIN_AMOUNT")
            .unwrap_or_else(|_| "500".to_string())
            .parse()
            .unwrap_or(500),
        max_amount: env::var("MAX_AMOUNT")
            .unwrap_or_else(|_| "7500".to_string())
            .parse()
            .unwrap_or(7500),
        take_orders: true,
        request_timeout: env::var("REQUEST_TIMEOUT")
            .unwrap_or_else(|_| "15000".to_string())
            .parse()
            .unwrap_or(15000),
    };

    let token_display = if config.access_token.len() > 10 {
        format!("{}...", &config.access_token[..10])
    } else {
        config.access_token.clone()
    };
    info!("Access Token: {}", token_display);

    let bot = OptimizedP2POrderSnatcher::new(config);
    let bot_for_signal = Arc::new(bot);
    let bot_clone = bot_for_signal.clone();
    
    ctrlc::set_handler(move || {
        println!("\n🛑 Получен сигнал остановки...");
        let bot = bot_clone.clone();
        
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                bot.stop().await;
                process::exit(0);
            });
        });
    }).expect("Ошибка установки обработчика сигнала");

    bot_for_signal.start_monitoring().await;
    bot_for_signal.start().await;

    loop {
        time::sleep(Duration::from_secs(60)).await;
    }
}