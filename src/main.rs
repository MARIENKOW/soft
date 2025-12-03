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
use tokio_tungstenite::{connect_async, WebSocketStream};
use futures_util::{SinkExt, StreamExt};
use log::{info, error, warn};
use futures_util::stream::SplitSink;
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;

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

        self.connect_websocket().await;
    }

    async fn connect_websocket(&self) {
        let ws_url = "wss://app.cr.bot/internal/v1/p2c-socket/?EIO=4&transport=websocket";
        
        info!("🔌 Подключаемся к WebSocket...");

        match connect_async(Url::parse(ws_url).unwrap()).await {
            Ok((ws_stream, _)) => {
                info!("✅ WebSocket подключен");
                let (write, read) = ws_stream.split();
                
                // Сохраняем write часть для отправки сообщений
                let write_arc = Arc::new(Mutex::new(write));
                
                // Отправка handshake
                self.send_socketio_handshake(write_arc.clone()).await;

                // Клонируем необходимые данные для обработчика сообщений
                let processed_orders_clone = self.processed_orders.clone();
                let stats_clone = self.stats.clone();
                let config_clone = self.config.clone();
                let client_clone = self.client.clone();
                let is_running_clone = self.is_running.clone();
                let write_clone = write_arc.clone();

                // Запускаем обработчик входящих сообщений
                tokio::spawn(async move {
                    Self::handle_websocket_messages(
                        read,
                        write_clone,
                        processed_orders_clone,
                        stats_clone,
                        config_clone,
                        client_clone,
                        is_running_clone,
                    ).await;
                });

                // Запускаем ping-pong
                self.start_ping_pong(write_arc.clone()).await;
            }
            Err(e) => {
                error!("❌ Ошибка подключения WebSocket: {}", e);
                warn!("🔌 Переподключаемся через 3 секунды...");
                
                // Используем Box для рекурсии
                let self_clone = self.clone();
                tokio::spawn(async move {
                    time::sleep(Duration::from_secs(3)).await;
                    self_clone.reconnect_websocket().await;
                });
            }
        }
    }

    // Отдельный метод для переподключения
    async fn reconnect_websocket(&self) {
        warn!("🔄 Выполняем переподключение...");
        self.connect_websocket().await;
    }

    async fn send_socketio_handshake(&self, write: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>) {
        let handshake_sequence = vec![
            (Duration::from_millis(10), "0".to_string()),
            (Duration::from_millis(50), "40".to_string()),
            (Duration::from_millis(100), r#"42["list:initialize"]"#.to_string()),
        ];

        for (delay, message) in handshake_sequence {
            time::sleep(delay).await;
            let mut write_lock = write.lock().await;
            if let Err(e) = write_lock.send(Message::Text(message.clone())).await {
                error!("❌ Ошибка отправки handshake: {}", e);
                break;
            }
            info!("📤 Отправляем: {}", message);
        }
    }

    async fn start_ping_pong(&self, write: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>) {
        let write_clone = write.clone();
        
        tokio::spawn(async move {
            loop {
                time::sleep(Duration::from_secs(25)).await;
                let mut write_lock = write_clone.lock().await;
                if let Err(e) = write_lock.send(Message::Text("2".to_string())).await {
                    error!("❌ Ошибка отправки ping: {}", e);
                    break;
                }
            }
        });
    }

    async fn handle_websocket_messages(
        mut read: futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        write: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
        processed_orders: Arc<Mutex<HashSet<String>>>,
        stats: Arc<Mutex<Stats>>,
        config: Config,
        client: Client,
        is_running: Arc<Mutex<bool>>,
    ) {
        while *is_running.lock().await {
            match read.next().await {
                Some(Ok(message)) => {
                    let start_time = Instant::now();
                    Self::process_websocket_message(
                        message,
                        &write,
                        &processed_orders,
                        &stats,
                        &config,
                        &client,
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
        
        // При разрыве соединения пытаемся переподключиться
        if *is_running.lock().await {
            warn!("🔄 Соединение разорвано, планируем переподключение...");
            // Здесь можно добавить логику переподключения, если нужно
        }
    }

    async fn process_websocket_message(
        message: Message,
        write: &Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
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
                    let mut write_lock = write.lock().await;
                    if let Err(e) = write_lock.send(Message::Text("3".to_string())).await {
                        error!("❌ Ошибка отправки pong: {}", e);
                    }
                    return;
                }
                
                if text == "3" {
                    return; // Pong ответ
                }
                
                if text.starts_with("40") {
                    return;
                }
                
                if text.starts_with('0') {
                    return;
                }

                // List snapshot
                if text.contains("list:snapshot") {
                    info!("✅ ВСЁ ПОДКЛЮЧЕНО УСПЕШНО → ОЖИДАЕМ ЗАКАЗЫ ✅ ");
                    info!("----------------------------------------------");
                    return;
                }

                if text.contains("list:update") {
                    Self::handle_list_update(text, processed_orders, stats, config, client, start_time).await;
                    return;
                }

                if text.starts_with("42") {
                    info!("📨 Другое событие: {}", text);
                }
            }
            Message::Ping(_) => {
                let mut write_lock = write.lock().await;
                if let Err(e) = write_lock.send(Message::Pong(vec![])).await {
                    error!("❌ Ошибка отправки pong: {}", e);
                }
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
        // Быстрый парсинг данных
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
                } else {
                    error!("❌ Ошибка парсинга ордера: {}", order_json);
                }
            }
        } else {
            warn!("⚠️ Не удалось найти данные ордера в сообщении");
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

        // Проверка дубликатов
        {
            let mut processed = processed_orders.lock().await;
            if processed.contains(&order.id) {
                return;
            }
            processed.insert(order.id.clone());
        }

        // Валидация
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

// Реализуем Clone для структуры
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
    // Инициализация логгера
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();
    
    // Загрузка переменных окружения
    dotenv().ok();

    let config = Config {
        access_token: env::var("ACCESS_TOKEN").unwrap_or_default(),
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

    // Вывод токена для проверки (только первые 10 символов)
    let token_display = if config.access_token.len() > 10 {
        format!("{}...", &config.access_token[..10])
    } else {
        config.access_token.clone()
    };
    info!("Access Token: {}", token_display);

    let bot = OptimizedP2POrderSnatcher::new(config);

    // Обработка сигналов завершения
    let bot_for_signal = Arc::new(bot);
    let bot_clone = bot_for_signal.clone();
    
    ctrlc::set_handler(move || {
        println!("\n🛑 Получен сигнал остановки...");
        let bot = bot_clone.clone();
        tokio::spawn(async move {
            bot.stop().await;
            process::exit(0);
        });
    }).expect("Ошибка установки обработчика сигнала");

    // Запуск мониторинга статистики
    bot_for_signal.start_monitoring().await;
    
    // Запуск бота
    bot_for_signal.start().await;

    // Бесконечное ожидание
    loop {
        time::sleep(Duration::from_secs(60)).await;
    }
}