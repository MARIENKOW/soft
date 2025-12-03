require("dotenv").config();
const WebSocket = require("ws");
const axios = require("axios");

const CONFIG = {
    ACCESS_TOKEN: process.env.ACCESS_TOKEN || "",
    MIN_AMOUNT: parseInt(process.env.MIN_AMOUNT) || 500,
    MAX_AMOUNT: parseInt(process.env.MAX_AMOUNT) || 7500,
    TAKE_ORDERS: true,
    REQUEST_TIMEOUT: parseInt(process.env.REQUEST_TIMEOUT) || 15000,
};

console.log(process.env.ACCESS_TOKEN);

class OptimizedP2POrderSnatcher {
    constructor(config) {
        this.config = config;
        this.ws = null;
        this.isRunning = false;
        this.processedOrders = new Set();
        this.pingInterval = null;

        // Статистика и мониторинг производительности
        this.stats = {
            total: 0,
            filtered: 0,
            taken: 0,
            failed: 0,
            timeouts: 0,
        };
    }

    async start() {
        console.log("🚀 Запуск оптимизированного бота...");
        console.log(`🎯 Мин сумма поиска → ${this.config.MIN_AMOUNT}`);
        console.log(`🎯 Макс сумма поиска → ${this.config.MAX_AMOUNT}`);

        await this.connectWebSocket();
        this.isRunning = true;
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
        });

        this.ws.on("open", (e, t) => {
            console.log("✅ WebSocket подключен");
            this.sendSocketIOHandshake();
        });

        this.ws.on("message", (data) => {
            this.processWebSocketMessage(data.toString());
        });

        this.ws.on("error", (error) => {
            console.log("❌ WebSocket ошибка:", error.message);
        });

        this.ws.on("close", (code, reason) => {
            console.log(`🔌 WebSocket отключен: ${code}`);
            console.log(reason);
            if (this.isRunning) {
                console.log("🔄 Переподключаемся через 3 секунды...");
                setTimeout(() => this.connectWebSocket(), 3000);
            }
        });
    }

    sendSocketIOHandshake() {
        // Правильная последовательность Socket.IO v4
        const handshakeSequence = [
            { delay: 10, message: "0" }, // Инициализация
            { delay: 50, message: "40" }, // Подключение к namespace
            { delay: 100, message: '42["list:initialize"]' }, // Инициализация списка
        ];

        handshakeSequence.forEach((step, index) => {
            setTimeout(() => {
                if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                    console.log(`📤 Отправляем: ${step.message}`);
                    this.ws.send(step.message);
                }
            }, step.delay);
        });
    }

    processWebSocketMessage(message) {
        try {
            // Ping-pong handling
            if (message === "2") {
                if (this.ws.readyState === WebSocket.OPEN) {
                    this.ws.send("3"); // pong
                }
                return;
            }

            if (message === "3") return;
            if (message.startsWith("40")) return;

            if (message.startsWith("0")) {
                JSON.parse(message.substring(1));
                return;
            }

            // List snapshot
            if (message.startsWith('42["list:snapshot"')) {
                console.log("✅ ВСЁ ПОДКЛЮЧЕНО УСПЕШНО → ОЖИДАЕМ ЗАКАЗЫ ✅ ");
                console.log("----------------------------------------------");
                return;
            }

            if (message.startsWith('42["list:update"')) {
                this.handleListUpdate(message);
                return;
            }

            // Other events
            if (message.startsWith("42")) {
                const payload = JSON.parse(message.substring(2));
                console.log("📨 Другое событие:", payload[0]);
                return;
            }
        } catch (error) {}
    }

    handleListUpdate(message) {
        try {
            // Быстрый парсинг только нужной части сообщения
            const dataStart = message.indexOf('"data":');
            if (dataStart === -1) return;

            // Ищем начало данных ордера
            let bracketCount = 0;
            let dataStartIndex = -1;
            let dataEndIndex = -1;

            for (let i = dataStart + 7; i < message.length; i++) {
                if (message[i] === "{" && dataStartIndex === -1) {
                    dataStartIndex = i;
                    bracketCount = 1;
                } else if (message[i] === "{") {
                    bracketCount++;
                } else if (message[i] === "}") {
                    bracketCount--;
                    if (bracketCount === 0) {
                        dataEndIndex = i + 1;
                        break;
                    }
                }
            }

            if (dataStartIndex !== -1 && dataEndIndex !== -1) {
                const orderJson = message.substring(
                    dataStartIndex,
                    dataEndIndex
                );
                const order = JSON.parse(orderJson);
                this.handleNewOrder(order);
            }
        } catch (error) {
            // Пропускаем ошибки парсинга для скорости
        }
    }

    async handleNewOrder(order) {
        this.stats.total++;

        // Быстрая проверка дубликатов
        if (this.processedOrders.has(order.id)) return;

        // Быстрая валидация
        if (!this.isValidOrder(order)) {
            return;
        }

        this.processedOrders.add(order.id);
        this.stats.filtered++;

        // console.log(`⚡ ЗАКАЗ ${order.in_amount} ${order.in_asset}`)

        if (this.config.TAKE_ORDERS) await this.takeOrder(order.id);
    }

    isValidOrder(order) {
        const amount = Math.trunc(order.in_amount);
        if (
            amount < this.config.MIN_AMOUNT ||
            order.in_asset !== "RUB" ||
            amount > this.config.MAX_AMOUNT
        ) {
            return false;
        }

        return true;
    }

    async takeOrder(orderId) {
        try {
            let response = await axios.post(
                `https://app.cr.bot/internal/v1/p2c/payments/take/${orderId}`,
                null,
                {
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
                    },
                    timeout: this.config.REQUEST_TIMEOUT,
                    validateStatus: null,
                }
            );

            if (response.status === 200) {
                this.stats.taken++;
                console.log("✅ ЗАКАЗ ВЗЯТ УСПЕШНО!");

                if (response.data.data) {
                    const orderData = response.data.data;
                    const paymentLink = `https://app.cr.bot/p2c/orders/${orderData.id}?back=payments`;
                    console.log("Ссылка для оплаты - ", paymentLink);
                }
            } else {
                this.stats.failed++;
                console.log(
                    response.data.error === "ActiveOrderExists"
                        ? "❌ Нужно оплатить старый заказ."
                        : "❌ Не успели взять"
                );
            }
        } catch (error) {
            if (error.name === "AbortError") {
                this.stats.timeouts++;
                console.log("⏰ Таймаут...");
            } else {
                this.stats.failed++;
                console.log("❌ Ошибка:", error.message);
            }
            return false;
        }
    }

    startMonitoring() {
        // Статистика каждые 15 секунд
        setInterval(() => {
            console.log(
                `📈 СТАТИСТИКА: Всего ${this.stats.total}, ` +
                    `Подходят ${this.stats.filtered}, ` +
                    `Взято ${this.stats.taken}, ` +
                    `Не взяли ${this.stats.failed}`
            );
        }, 15000);
    }

    async stop() {
        this.isRunning = false;
        if (this.ws) {
            this.ws.close();
        }
        if (this.pingInterval) {
            clearInterval(this.pingInterval);
        }
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
