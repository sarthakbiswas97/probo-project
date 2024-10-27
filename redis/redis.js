import { createClient } from "redis";
const client = createClient({
    url: 'redis://localhost:6379'
});
const publisher = client.duplicate();

//////////////////////
export let INR_BALANCES = {};
export let ORDERBOOK = {}
export let STOCK_BALANCES = {}
/////////////////////

export function resetAll() {
    INR_BALANCES={}
    ORDERBOOK={}
    STOCK_BALANCES={}
}

async function processTask(task) {
    const { type, data, uid } = JSON.parse(task)

    switch (type) {
        case 'createUser':
            const userId = data.id;
            if (INR_BALANCES.hasOwnProperty(userId))
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: "User already exists" }));
            else {
                INR_BALANCES[userId] = {
                    balance: 0,
                    locked: 0
                }
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: false, msg: `User ${userId} created successfully` }));
            }
            break;

        case 'createSymbol':
            const stockSymbol = data.stockSymbol;
            if(ORDERBOOK.hasOwnProperty(stockSymbol)){
                await publisher.publish(`response.${uid}`,
                JSON.stringify({
                    error: true,
                    msg: "Stock already exists"
                })
                )
            } else{
                ORDERBOOK[stockSymbol]={
                    "yes":{},
                    "no": {}
                }
                await publisher.publish(`response.${uid}`,
                    JSON.stringify({
                        error: false,
                        msg: JSON.stringify(ORDERBOOK)
                    })
                    )
            }

            break;

        case 'getParticularStockBalance':
            const particularStockSymbol = data.stockSymbol;
            if(!ORDERBOOK.hasOwnProperty(particularStockSymbol)){
                await publisher.publish(`response.${uid}`,
                JSON.stringify({
                    error: true,
                    msg: "Stock symbol doesn't exists"
                }) )
            }else{
                await publisher.publish(`response.${uid}`,
                    JSON.stringify({
                        error: false,
                        msg: JSON.stringify(ORDERBOOK[particularStockSymbol])
                    })
                    )
            }
        break;

        case 'getBalance':
            const userID = data.id;
            if (!INR_BALANCES.hasOwnProperty(userID)){
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: `${userID} doesn't exist` }));
                console.log("here");}
            else {
                await publisher.publish(`response.${uid}`, JSON.stringify({ 
                    error: false, 
                    msg: JSON.stringify(INR_BALANCES[userID]) }));
            }
            break;

        case 'getStockBalance':
            const resultStockBalance = STOCK_BALANCES;
            await publisher.publish(`response.${uid}`, JSON.stringify({
                error: false,
                msg: JSON.stringify(resultStockBalance)
            }));

            break;

        case 'getOrderbook':
            await publisher.publish(`response.${uid}`, JSON.stringify({
                error: false,
                msg: JSON.stringify(ORDERBOOK)
            }));

        case 'getbalanceINR':
            const balanceResult = INR_BALANCES;

            await publisher.publish(`response.${uid}`, JSON.stringify({
                error: false,
                msg: JSON.stringify(balanceResult)
            }));
            break;

        case 'getUserStockBalance':
            const userIdForStockBalance = data.id;
            if (!STOCK_BALANCES.hasOwnProperty(userIdForStockBalance))
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: `${userIdForStockBalance} doesn't exist` }));
            else {
                const userStockBalance = STOCK_BALANCES[userIdForStockBalance];
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: false, msg: JSON.stringify(userStockBalance) }));
            }

            break;

        case 'doOnRamp':
            const onrampUserId = data.userId;
            const onrampAmount = data.amount;
            if (!INR_BALANCES.hasOwnProperty(onrampUserId)){
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: `${onrampUserId} doesn't exist` }));
            }
            else {
                console.log(onrampUserId);
                INR_BALANCES[onrampUserId].balance += onrampAmount;
                await publisher.publish(`response.${uid}`, JSON.stringify({
                    error: false,
                    msg: JSON.stringify({msg: INR_BALANCES[onrampUserId]})
                }));
            }
            break;

        case 'placeBuy':
            const buyerID = data.userId;
            const buyerStockSymbol = data.stockSymbol;
            const buyerQuantity = data.quantity;
            const buyerPrice = data.price;
            const buyerStockType = data.stockType;
            const buyerPriceInRs = buyerPrice / 100;
            const amountNeeded = buyerQuantity * buyerPriceInRs;

            if (!INR_BALANCES.hasOwnProperty(buyerID)) {
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: "USER NOT FOUND" }));
                break;
            }

            if (INR_BALANCES[buyerID].balance < amountNeeded) {
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: "INSUFFICIENT BALANCE" }));
                break;
            }

            if (!ORDERBOOK.hasOwnProperty(buyerStockSymbol)) {
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: "STOCK NOT AVAILABLE" }));
                break;
            }

            const particularStock = ORDERBOOK[buyerStockSymbol];
            let totalTradeQuantity = buyerQuantity;
            let totalTradeCost = 0;

            if (particularStock[buyerStockType].hasOwnProperty(buyerPriceInRs.toString())) {
                const priceCategory = particularStock[buyerStockType][buyerPriceInRs];

                for (let sellerId in priceCategory.orders) {
                    if (totalTradeQuantity === 0) break;

                    const sellerQuantity = priceCategory.orders[sellerId];
                    const tradeQuantity = Math.min(sellerQuantity, totalTradeQuantity);
                    const tradeCost = tradeQuantity * buyerPriceInRs;

                    INR_BALANCES[buyerID].balance -= tradeCost;
                    INR_BALANCES[sellerId].balance += tradeCost;
                    INR_BALANCES[buyerID].locked += tradeCost;
                    INR_BALANCES[sellerId].locked -= tradeCost;

                    if (!STOCK_BALANCES[buyerID].hasOwnProperty(buyerStockSymbol)) {
                        STOCK_BALANCES[buyerID][buyerStockSymbol] = {};
                    }
                    if (!STOCK_BALANCES[buyerID][buyerStockSymbol].hasOwnProperty(buyerStockType)) {
                        STOCK_BALANCES[buyerID][buyerStockSymbol][buyerStockType] = { quantity: 0, locked: 0 };
                    }
                    STOCK_BALANCES[buyerID][buyerStockSymbol][buyerStockType].quantity += tradeQuantity;

                    if (!STOCK_BALANCES[sellerId].hasOwnProperty(buyerStockSymbol)) {
                        STOCK_BALANCES[sellerId][buyerStockSymbol] = {};
                    }
                    if (!STOCK_BALANCES[sellerId][buyerStockSymbol].hasOwnProperty(buyerStockType)) {
                        STOCK_BALANCES[sellerId][buyerStockSymbol][buyerStockType] = { quantity: 0, locked: 0 };
                    }
                    STOCK_BALANCES[sellerId][buyerStockSymbol][buyerStockType].quantity -= tradeQuantity;
                    STOCK_BALANCES[sellerId][buyerStockSymbol][buyerStockType].locked += tradeQuantity;

                    priceCategory.orders[sellerId] -= tradeQuantity;
                    totalTradeQuantity -= tradeQuantity;
                    totalTradeCost += tradeCost;

                    if (priceCategory.orders[sellerId] === 0) {
                        delete priceCategory.orders[sellerId];
                    }
                }

                priceCategory.total -= buyerQuantity - totalTradeQuantity;

                if (totalTradeQuantity > 0) {
                    priceCategory.orders[buyerID] = (priceCategory.orders[buyerID] || 0) + totalTradeQuantity;
                    priceCategory.total += totalTradeQuantity;
                    STOCK_BALANCES[buyerID][buyerStockSymbol][buyerStockType].locked += totalTradeQuantity;
                }

                await publisher.publish(`response.${uid}`, JSON.stringify({
                    error: false,
                    msg: JSON.stringify(particularStock[buyerStockType][buyerPriceInRs])
                }));

                await publisher.publish(buyerStockSymbol,  JSON.stringify(particularStock[buyerStockType]));
            } else {
                const reverseStockType = buyerStockType === "yes" ? "no" : "yes";
                const reversePrice = (10 - buyerPriceInRs).toFixed(2);

                if (!particularStock[reverseStockType].hasOwnProperty(reversePrice)) {
                    particularStock[reverseStockType][reversePrice] = {
                        total: 0,
                        orders: {}
                    };
                }

                INR_BALANCES[buyerID].balance -= amountNeeded;
                INR_BALANCES[buyerID].locked += amountNeeded;

                particularStock[reverseStockType][reversePrice].total += buyerQuantity;

                if (particularStock[reverseStockType][reversePrice].orders.hasOwnProperty(buyerID)) {
                    particularStock[reverseStockType][reversePrice].orders[buyerID] += buyerQuantity;
                } else {
                    particularStock[reverseStockType][reversePrice].orders[buyerID] = buyerQuantity;
                }

                if (!STOCK_BALANCES[buyerID].hasOwnProperty(buyerStockSymbol)) {
                    STOCK_BALANCES[buyerID][buyerStockSymbol] = {};
                }
                if (!STOCK_BALANCES[buyerID][buyerStockSymbol].hasOwnProperty(reverseStockType)) {
                    STOCK_BALANCES[buyerID][buyerStockSymbol][reverseStockType] = { quantity: 0, locked: 0 };
                }

                STOCK_BALANCES[buyerID][buyerStockSymbol][reverseStockType].locked += buyerQuantity;

                await publisher.publish(`response.${uid}`, JSON.stringify({
                    error: false,
                    msg: JSON.stringify(particularStock[reverseStockType])
                }));

                await publisher.publish(buyerStockSymbol, JSON.stringify(particularStock[buyerStockType]));
            }

            break;

        case 'placeSell':
            const sellerID = data.userId;
            const sellerStockSymbol = data.stockSymbol;
            const sellerQuantity = data.quantity;
            const sellerPrice = data.price;
            const sellerStockType = data.stockType;
            const sellerPriceInRs = sellerPrice / 100;

            if (!STOCK_BALANCES.hasOwnProperty(sellerID)) {
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: `User ${sellerID} doesn't exist in STOCK_BALANCES` }));
                break;
            }

            if (!STOCK_BALANCES[sellerID].hasOwnProperty(sellerStockSymbol)) {
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: `User ${sellerID} doesn't have ${sellerStockSymbol} in their stock balance` }));
                break;
            }

            if (!STOCK_BALANCES[sellerID][sellerStockSymbol].hasOwnProperty(sellerStockType)) {
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: `User ${sellerID} doesn't have ${sellerStockType} type for ${sellerStockSymbol}` }));
                break;
            }

            if (STOCK_BALANCES[sellerID][sellerStockSymbol][sellerStockType].quantity < sellerQuantity) {
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: `INSUFFICIENT BALANCE: User ${sellerID} has ${STOCK_BALANCES[sellerID][sellerStockSymbol][sellerStockType].quantity} ${sellerStockType} ${sellerStockSymbol}, but wants to sell ${sellerQuantity}` }));
                break;
            }

            // after all the checks place sell order
            STOCK_BALANCES[sellerID][sellerStockSymbol][sellerStockType].locked += sellerQuantity;
            STOCK_BALANCES[sellerID][sellerStockSymbol][sellerStockType].quantity -= sellerQuantity;

            // orderbook structure check
            if (!ORDERBOOK.hasOwnProperty(sellerStockSymbol)) {
                ORDERBOOK[sellerStockSymbol] = {};
            }
            if (!ORDERBOOK[sellerStockSymbol].hasOwnProperty(sellerStockType)) {
                ORDERBOOK[sellerStockSymbol][sellerStockType] = {};
            }
            if (!ORDERBOOK[sellerStockSymbol][sellerStockType].hasOwnProperty(sellerPriceInRs)) {
                ORDERBOOK[sellerStockSymbol][sellerStockType][sellerPriceInRs] = {
                    total: 0,
                    orders: {}
                };
            }

            // Updating orderbook
            ORDERBOOK[sellerStockSymbol][sellerStockType][sellerPriceInRs].total += sellerQuantity;
            if (!ORDERBOOK[sellerStockSymbol][sellerStockType][sellerPriceInRs].orders.hasOwnProperty(sellerID)) {
                ORDERBOOK[sellerStockSymbol][sellerStockType][sellerPriceInRs].orders[sellerID] = 0;
            }
            ORDERBOOK[sellerStockSymbol][sellerStockType][sellerPriceInRs].orders[sellerID] += sellerQuantity;

            await publisher.publish(`response.${uid}`, JSON.stringify({
                error: false,
                msg: JSON.stringify(ORDERBOOK)
            }));

            await publisher.publish(sellerStockSymbol, JSON.stringify({ ORDERBOOK }));

            break;

        case 'resetAll':
            INR_BALANCES={}
            ORDERBOOK={}
            STOCK_BALANCES={}
            await publisher.publish(`response.${uid}`, JSON.stringify({
                error: false,
                msg: `all reset done`
            }));

        break;

        default:
            console.log(`Unknown task type: ${type}`);

    }
}

async function startWorker() {
    try {
        await client.connect();
        await publisher.connect();
        console.log("Worker connected to Redis...");

        while (true) {
            try {
                const result = await client.brPop('taskQueue', 0);
                console.log("result from redis.js StartWorker function", result);
                if (result) {
                    await processTask(result.element);
                }
            } catch (error) {
                console.error("Error processing task in redis.js startWorker function", error);
            }
        }
    } catch (error) {
        console.error("Failed to connect to Redis in redis.js", error);
    }
}

startWorker();
