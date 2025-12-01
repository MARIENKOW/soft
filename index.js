require("dotenv").config();
const WebSocket = require("ws");
const axios = require("axios");
const { performance } = require("perf_hooks");

const CONFIG = {
    ACCESS_TOKEN: process.env.ACCESS_TOKEN || "",
    MIN_AMOUNT: parseInt(process.env.MIN_AMOUNT) || 500,
    MAX_AMOUNT: parseInt(process.env.MAX_AMOUNT) || 7500,
    TAKE_ORDERS: true,
    REQUEST_TIMEOUT: parseInt(process.env.REQUEST_TIMEOUT) || 8000, // Уменьшен таймаут
    CONCURRENT_REQUESTS: 3, // Количество параллельных запросов
};

let startTime = 0

class OptimizedP2POrderSnatcher {
    constructor(config) {
        this.config = config;
        this.ws = null;
        this.isRunning = false;
        this.processedOrders = new Map(); // Используем Map для быстрого удаления старых записей
        this.requestQueue = [];
        this.activeRequests = 0;
        this.orderCache = new Set(); // Кэш для быстрой проверки

        // Оптимизированная статистика
        this.stats = {
            total: 0,
            filtered: 0,
            taken: 0,
            failed: 0,
            timeouts: 0,
            avgProcessTime: 0,
            lastProcessTime: 0,
        };

        this.startTime = performance.now();
        this.cleanupInterval = null;
    }

    async start() {
        console.log("🚀 Запуск гипер-оптимизированного бота...");
        console.log(
            `🎯 Диапазон суммы: ${this.config.MIN_AMOUNT} - ${this.config.MAX_AMOUNT} RUB`
        );
        console.log(
            `⚡ Параллельных запросов: ${this.config.CONCURRENT_REQUESTS}`
        );

        await this.connectWebSocket();
        this.isRunning = true;
        this.startCleanup();
        this.startMonitoring();
    }

    async connectWebSocket() {
        const wsUrl =
            "wss://app.cr.bot/internal/v1/p2c-socket/?EIO=4&transport=websocket";

        console.log("🔌 Подключаемся к WebSocket...");

        this.ws = new WebSocket(wsUrl, {
            headers: {
                Cookie: `access_token=${this.config.ACCESS_TOKEN}`,
                Origin: "https://app.cr.bot",
            },
            perMessageDeflate: false, // Отключаем сжатие для скорости
        });

        this.ws.on("open", () => {
            console.log("✅ WebSocket подключен");
            this.sendOptimizedHandshake();
        });

        this.ws.on("message", (data) => {
            startTime = performance.now()
            this.processWebSocketMessage(data.toString());
        });

        this.ws.on("error", (error) => {
            console.log("❌ WebSocket ошибка:", error.message);
        });

        this.ws.on("close", (code) => {
            console.log(`🔌 WebSocket отключен: ${code}`);
            if (this.isRunning) {
                console.log("🔄 Переподключаемся через 1 секунду...");
                setTimeout(() => this.connectWebSocket(), 1000);
            }
        });
    }

    sendOptimizedHandshake() {
        // Более быстрая последовательность подключения
        const handshakeSequence = [
            { delay: 5, message: "0" },
            { delay: 20, message: "40" },
            { delay: 40, message: '42["list:initialize"]' },
        ];

        handshakeSequence.forEach((step) => {
            setTimeout(() => {
                if (this.ws?.readyState === WebSocket.OPEN) {
                    this.ws.send(step.message);
                }
            }, step.delay);
        });
    }

    processWebSocketMessage(message) {
        // Ультра-быстрая обработка с минимальными проверками
        if (message === "2") {
            this.ws?.readyState === WebSocket.OPEN && this.ws.send("3");
            return;
        }
        if (message === "3" || message.startsWith("40")) return;

        if (message.startsWith('42["list:update"')) {
            this.handleListUpdateOptimized(message);
            return;
        }

        if (message.startsWith('42["list:snapshot"')) {
            console.log("✅ ВСЁ ПОДКЛЮЧЕНО → ОЖИДАЕМ ЗАКАЗЫ ✅");
            return;
        }
    }

    handleListUpdateOptimized(message) {
        const startTime = performance.now();

        try {
            // Быстрый поиск данных ордера
            const dataIndex = message.indexOf('"data":');
            if (dataIndex === -1) return;

            // Находим начало JSON объекта ордера
            const start = message.indexOf("{", dataIndex);
            if (start === -1) return;

            let braceCount = 0;
            let end = -1;

            for (let i = start; i < message.length; i++) {
                if (message[i] === "{") braceCount++;
                if (message[i] === "}") {
                    braceCount--;
                    if (braceCount === 0) {
                        end = i + 1;
                        break;
                    }
                }
            }

            if (end > start) {
                const orderJson = message.substring(start, end);
                // Парсим асинхронно, чтобы не блокировать поток
                setImmediate(() => {
                    try {
                        const order = JSON.parse(orderJson);
                        this.handleNewOrderOptimized(order);
                    } catch (e) {
                        // Игнорируем ошибки парсинга
                    }
                });
            }
        } catch (error) {
            // Пропускаем ошибки
        }

        this.stats.lastProcessTime = performance.now() - startTime;
    }

    async handleNewOrderOptimized(order) {
        this.stats.total++;

        // Супер-быстрая проверка дубликатов через Bloom filter эмуляцию
        if (this.orderCache.has(order.id)) return;
        this.orderCache.add(order.id);

        // Быстрая валидация без лишних проверок
        const amount = Math.trunc(order.in_amount);
        if (
            amount < this.config.MIN_AMOUNT ||
            amount > this.config.MAX_AMOUNT ||
            order.in_asset !== "RUB"
        ) {
            return;
        }

        this.stats.filtered++;

        // console.log(`⚡ ЗАКАЗ ${amount} RUB`); // Минимальный лог для скорости

        if (this.config.TAKE_ORDERS) {
            this.enqueueOrderRequest(order.id);
        }
    }

    enqueueOrderRequest(orderId) {
        this.requestQueue.push(orderId);
        this.processQueue();
    }

    async processQueue() {
        // Ограничиваем количество параллельных запросов
        while (
            this.activeRequests < this.config.CONCURRENT_REQUESTS &&
            this.requestQueue.length > 0
        ) {
            const orderId = this.requestQueue.shift();
            this.activeRequests++;

            // Запускаем асинхронно без await чтобы не блокировать
            this.takeOrderFast(orderId).finally(() => {
                this.activeRequests--;
                // Рекурсивно продолжаем обработку очереди
                setImmediate(() => this.processQueue());
            });
        }
    }

    async takeOrderFast(orderId) {
        const controller = new AbortController();
        const timeout = setTimeout(
            () => controller.abort(),
            this.config.REQUEST_TIMEOUT
        );

        try {

            let response = fetch(
                `https://app.cr.bot/internal/v1/p2c/payments/take/${orderId}`,
                {
                    method: "POST",
                    headers: {
                        Cookie: `access_token=${this.config.ACCESS_TOKEN}`,
                        "User-Agent":
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        Origin: "https://app.cr.bot",
                        Referer: "https://app.cr.bot/p2c",
                        Accept: "application/json, text/plain, */*",
                        "Accept-Language":
                            "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                        "Content-Type": "application/json",
                        // Добавляем все оригинальные заголовки
                        "Accept-Encoding": "gzip, deflate, br",
                        Connection: "keep-alive",
                        "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "cors",
                        "Sec-Fetch-Site": "same-origin",
                    },
                    body: null, // Empty body - это важно! Сохраняем как в оригинале
                    signal: controller.signal,
                    // Дополнительные параметры для точного соответствия
                    redirect: "follow",
                    credentials: "include", // Важно для cookies
                    mode: "cors",
                    cache: "no-cache",
                    referrerPolicy: "strict-origin-when-cross-origin",
                }
            );

            const processTime = performance.now() - startTime;
            console.log("время запроса: ", processTime);
            response = await response;
            this.stats.avgProcessTime =
                this.stats.avgProcessTime * 0.7 + processTime * 0.3;

            // Получаем текст ответа для обработки
            const responseText = await response.text();

            if (response.status === 200) {
                this.stats.taken++;
                console.log(
                    `✅ ЗАКАЗ ВЗЯТ УСПЕШНО! (${Math.round(processTime)}ms)`
                );

                try {
                    // Парсим JSON только если ответ не пустой
                    if (responseText.trim()) {
                        const responseData = JSON.parse(responseText);
                        if (responseData.data) {
                            const orderData = responseData.data;
                            const paymentLink = `https://app.cr.bot/p2c/orders/${orderData.id}?back=payments`;
                            console.log("Ссылка для оплаты - ", paymentLink);
                        }
                    }
                } catch (parseError) {
                    console.log(
                        "⚠️ Ответ получен, но не удалось распарсить JSON"
                    );
                }
            } else {
                this.stats.failed++;

                try {
                    // Пытаемся получить текст ошибки
                    if (responseText.trim()) {
                        const errorData = JSON.parse(responseText);
                        console.log(
                            errorData.error === "ActiveOrderExists"
                                ? "❌ Нужно оплатить старый заказ."
                                : `❌ Не успели взять: ${
                                      errorData.error || "Неизвестная ошибка"
                                  }`
                        );
                    } else {
                        console.log(
                            `❌ Не успели взять (статус: ${response.status})`
                        );
                    }
                } catch (e) {
                    console.log(
                        `❌ Ошибка при обработке ответа (статус: ${response.status})`
                    );
                }
            }
        } catch (error) {
            if (error.name === "AbortError") {
                this.stats.timeouts++;
                console.log("⏰ Таймаут...");
            } else if (
                error.name === "TypeError" &&
                error.message.includes("fetch")
            ) {
                this.stats.failed++;
                console.log("❌ Ошибка сети");
            } else {
                this.stats.failed++;
                console.log("❌ Ошибка:", error.message);
            }
            return false;
        } finally {
            clearTimeout(timeout);
        }
    }

    startCleanup() {
        // Очищаем кэш каждые 30 секунд чтобы не накапливать мусор
        this.cleanupInterval = setInterval(() => {
            // Очищаем старые записи (больше 1 минуты)
            const now = Date.now();
            for (const [id, timestamp] of this.processedOrders) {
                if (now - timestamp > 60000) {
                    this.processedOrders.delete(id);
                    this.orderCache.delete(id);
                }
            }

            // Очищаем очередь если она слишком большая
            if (this.requestQueue.length > 100) {
                this.requestQueue = this.requestQueue.slice(-50);
            }
        }, 30000);
    }

    startMonitoring() {
        setInterval(() => {
            const uptime = (
                (performance.now() - this.startTime) /
                1000
            ).toFixed(0);
            console.log(
                `📈 СТАТИСТИКА за ${uptime}с:\n` +
                    `   Всего: ${this.stats.total} | Подходят: ${this.stats.filtered}\n` +
                    `   Взято: ${this.stats.taken} | Не взято: ${this.stats.failed}\n` +
                    `   В очереди: ${this.requestQueue.length} | Активных: ${this.activeRequests}\n` +
                    `   Среднее время: ${this.stats.avgProcessTime.toFixed(
                        1
                    )}ms`
            );
        }, 10000);
    }

    async stop() {
        this.isRunning = false;
        if (this.cleanupInterval) clearInterval(this.cleanupInterval);
        if (this.ws) this.ws.close();
        console.log("🛑 Бот остановлен");
        process.exit(0);
    }
}

// 🎯 ЗАПУСК БОТА
const bot = new OptimizedP2POrderSnatcher(CONFIG);

// Обработка graceful shutdown
process.on("SIGINT", async () => {
    console.log("\n🛑 Получен сигнал остановки...");
    await bot.stop();
});

process.on("SIGTERM", async () => {
    console.log("\n🛑 Получен сигнал завершения...");
    await bot.stop();
});

// Запуск бота
bot.start().catch((error) => {
    console.error("❌ Критическая ошибка запуска:", error);
    process.exit(1);
});
