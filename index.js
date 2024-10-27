import express from 'express';
import dotenv from 'dotenv';
import { createClient } from "redis";
import { v4 as uuidv4 } from 'uuid';

dotenv.config();
const app = express();
app.use(express.json());

const client = createClient();
const subscriber = createClient();

client.on('error', (err) => console.error('Redis Client Error:', err));

async function queueTask(task) {
    console.log('inside', task);
    await client.lPush('taskQueue', JSON.stringify(task));
}

const handlePubSubWithTimeout = (uid, timeoutMs = 3000) => {
    return new Promise((resolve, reject) => {
        const channel = `response.${uid}`;

        const timeout = setTimeout(() => {
            subscriber.unsubscribe(channel);
            reject(new Error("Response timed out"));
        }, timeoutMs);

        subscriber.subscribe(channel, (data) => {
            clearTimeout(timeout);
            subscriber.unsubscribe(channel);
            resolve(data);
        });
    });
};

app.post("/user/create/:userId", async (req, res) => {
    const id = req.params.userId;
    const uid = uuidv4();
    const promisified = handlePubSubWithTimeout(uid);
    await queueTask({
        type: 'createUser',
        data: { id },
        uid: uid
    });
    try {
        let data = await promisified
        data = JSON.parse(data)
        if (data.error) {
            res.status(404).json({ msg: data.msg });
        } else {
            res.status(200).json({ msg: data.msg })
        }
    } catch (e) {
        res.status(500).json({ error: e })
    }
});

app.post("/symbol/create/:stockSymbol", async (req, res) => {
    const stockSymbol = req.params.stockSymbol;
    const uid = uuidv4();
    const promisified = handlePubSubWithTimeout(uid);
    await queueTask({
        type: 'createSymbol',
        data: { stockSymbol },
        uid: uid
    });
    try {
        let data = await promisified
        data = JSON.parse(data)
        if (data.error) {
            res.status(404).json({ msg: data.msg });
        } else {
            res.status(200).json({ msg: JSON.parse(data.msg) });
        }
    } catch (e) {
        res.status(500).json({ error: e })
    }
});

app.get("/balance/inr/:userId", async (req, res) => {
    const id = req.params.userId;
    const uid = uuidv4();
    const promisified = handlePubSubWithTimeout(uid);
    await queueTask({
        type: 'getBalance',
        data: { id },
        uid: uid
    });
    try {
        let data = await promisified
        data = JSON.parse(data)
        if (data.error) {
            res.status(404).json({ msg: data.msg });
        } else {
            res.status(200).json({ msg: JSON.parse(data.msg) });
        }
    } catch (e) {
        res.status(500).json({ error: e })
    }
});

app.get("/balances/stock", async (req, res) => {
    const uid = uuidv4();
    const promisified = handlePubSubWithTimeout(uid);
    await queueTask({
        type: 'getStockBalance',
        uid: uid
    });
    try {
        let data = await promisified
        data = JSON.parse(data)
        if (data.error) {
            res.status(404).json({ msg: data.msg });
        } else {
            res.status(200).json({ msg: JSON.parse(data.msg) });
        }
    } catch (e) {
        res.status(500).json({ error: e })
    }
})
app.get("/orderbook/:stockSymbol", async (req, res) => {
    const stockSymbol = req.params.stockSymbol;
    const uid = uuidv4();
    const promisified = handlePubSubWithTimeout(uid);
    await queueTask({
        type: 'getParticularStockBalance',
        data: { stockSymbol },
        uid: uid
    });
    try {
        let data = await promisified
        data = JSON.parse(data)
        if (data.error) {
            res.status(404).json({ msg: data.msg });
        } else {
            res.status(200).json({ msg: JSON.parse(data.msg) });
        }
    } catch (e) {
        res.status(500).json({ error: e })
    }
})
app.get("/orderbook", async (req, res) => {
    const uid = uuidv4();
    const promisified = handlePubSubWithTimeout(uid);
    await queueTask({
        type: 'getOrderbook',
        uid: uid
    });
    try {
        let data = await promisified
        data = JSON.parse(data)
        if (data.error) {
            res.status(404).json({ msg: data.msg });
        } else {
            res.status(200).json({ msg: data.msg });
        }
    } catch (e) {
        res.status(500).json({ error: e })
    }
})

app.get("/balances/inr", async (req, res) => {
    const uid = uuidv4();
    const promisified = handlePubSubWithTimeout(uid);
    await queueTask({
        type: 'getbalanceINR',
        uid: uid
    });
    try {
        let data = await promisified
        data = JSON.parse(data)
        if (data.error) {
            res.status(404).json({ msg: data.msg });
        } else {
            res.status(200).json({ msg: JSON.parse(data.msg) });
        }
    } catch (e) {
        res.status(500).json({ error: e })
    }
})

app.get("/balance/stock/:userId", async (req, res) => {
    const id = req.params.userId;
    const uid = uuidv4();
    const promisified = handlePubSubWithTimeout(uid);
    await queueTask({
        type: 'getUserStockBalance',
        data: { id },
        uid: uid
    });
    try {
        let data = await promisified
        data = JSON.parse(data)
        if (data.error) {
            res.status(404).json({ msg: data.msg });
        } else {
            res.status(200).json({ msg: data.msg });
        }
    } catch (e) {
        res.status(500).json({ error: e })
    }
});

app.post("/onramp/inr", async (req, res) => {
    const { userId, amount } = req.body;
    const uid = uuidv4();
    const promisified = handlePubSubWithTimeout(uid);
    await queueTask({
        type: 'doOnRamp',
        data: { userId, amount },
        uid: uid
    });
    try {
        let data = await promisified
        data = JSON.parse(data)
        if (data.error) {
            res.status(404).json({ msg: data.msg });
        } else {
            res.status(200).json(JSON.parse(data.msg))
        }
    } catch (e) {
        res.status(500).json({ error: e })
    }
});

app.post("/order/buy", async (req, res) => {
    const { userId, stockSymbol, quantity, price, stockType } = req.body;
    const uid = uuidv4();
    const promisified = handlePubSubWithTimeout(uid);
    await queueTask({
        type: 'placeBuy',
        data: { userId, stockSymbol, quantity, price, stockType },
        uid: uid
    });
    try {
        let data = await promisified
        data = JSON.parse(data)
        if (data.error) {
            res.status(404).json({ msg: data.msg });
        } else {
            res.status(200).json({ msg: JSON.parse(data.msg) });
        }
    } catch (e) {
        res.status(500).json({ error: e })
    }
});

app.post("/order/sell", async (req, res) => {
    const { userId, stockSymbol, quantity, price, stockType } = req.body;
    const uid = uuidv4();
    const promisified = handlePubSubWithTimeout(uid);
    await queueTask({
        type: 'placeSell',
        data: { userId, stockSymbol, quantity, price, stockType },
        uid: uid
    });
    try {
        let data = await promisified
        data = JSON.parse(data)
        if (data.error) {
            res.status(404).json({ msg: data.msg });
        } else {
            res.status(200).json({ msg: data.msg });
        }
    } catch (e) {
        res.status(500).json({ error: e })
    }
});

app.post("/reset", async (req, res) => {
    const uid = uuidv4();
    const promisified = handlePubSubWithTimeout(uid);
    await queueTask({
        type: 'resetAll',
        data: {},
        uid: uid
    });
    try {
        let data = await promisified
        data = JSON.parse(data)
        if (data.error) {
            res.status(404).json({ msg: data.msg });
        } else {
            res.status(200).json({ msg: data.msg });
        }
    } catch (e) {
        res.status(500).json({ error: e })
    }
})


export default app;

async function startServer() {
    try {
        await client.connect();
        await subscriber.connect()
        console.log(`connected to redis`);
        app.listen(3000, () => {
            console.log(`express server started on 3000...`);
        })
    } catch (error) {
        console.log(`failed to connect to redis`, error);
    }
}
startServer();
