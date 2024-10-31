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
    INR_BALANCES = {}
    ORDERBOOK = {}
    STOCK_BALANCES = {}
}

async function processTask(task) {
    const { type, data, uid } = JSON.parse(task)

    switch (type) {
        case 'createUser': {
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
        }
            break;

        case 'createSymbol':
            const stockSymbol = data.stockSymbol;
            if (ORDERBOOK.hasOwnProperty(stockSymbol)) {
                await publisher.publish(`response.${uid}`,
                    JSON.stringify({
                        error: true,
                        msg: "Stock already exists"
                    })
                )
            } else {
                ORDERBOOK[stockSymbol] = {
                    "yes": {},
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
            if (!ORDERBOOK.hasOwnProperty(particularStockSymbol)) {
                await publisher.publish(`response.${uid}`,
                    JSON.stringify({
                        error: true,
                        msg: "Stock symbol doesn't exists"
                    }))
            } else {
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
            if (!INR_BALANCES.hasOwnProperty(userID)) {
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: `${userID} doesn't exist` }));
                console.log("here");
            }
            else {
                await publisher.publish(`response.${uid}`, JSON.stringify({
                    error: false,
                    msg: JSON.stringify(INR_BALANCES[userID])
                }));
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
                await publisher.publish(`response.${uid}`,
                    JSON.stringify({
                        error: false,
                        msg: JSON.stringify(STOCK_BALANCES[userIdForStockBalance])
                    }));
            }

            break;

        case 'doOnRamp':
            const onrampUserId = data.userId;
            const onrampAmount = data.amount;
            if (!INR_BALANCES.hasOwnProperty(onrampUserId)) {
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: `${onrampUserId} doesn't exist` }));
            }
            else {
                console.log(onrampUserId);
                INR_BALANCES[onrampUserId].balance += onrampAmount;
                await publisher.publish(`response.${uid}`, JSON.stringify({
                    error: false,
                    msg: JSON.stringify({ msg: INR_BALANCES[onrampUserId] })
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
            const amountNeeded = buyerQuantity * buyerPrice;

            if (buyerQuantity === 0) {
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: "Can't place order for 0 quantity" }));
                break;
            }
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
            if (!ORDERBOOK[buyerStockSymbol].hasOwnProperty(buyerStockType)) {
                await publisher.publish(`response.${uid}`, JSON.stringify({ error: true, msg: "STOCK TYPE NOT AVAILABLE" }));
                break;
            }

            const particularStock = ORDERBOOK[buyerStockSymbol];
            let totalTradeQuantity = buyerQuantity;
            let totalTradeCost = 0;
            console.log("buyer price in rs", buyerPriceInRs);
            if (particularStock[buyerStockType].hasOwnProperty(buyerPriceInRs.toString())) {

                const priceCategory = particularStock[buyerStockType][buyerPriceInRs];

                for (let sellerId in priceCategory.orders) {
                    if (totalTradeQuantity === 0) break;

                    const sellerQuantity = priceCategory.orders[sellerId];
                    const tradeQuantity = Math.min(sellerQuantity, totalTradeQuantity);
                    const tradeCost = tradeQuantity * buyerPrice;

                    INR_BALANCES[buyerID].balance -= tradeCost;
                    INR_BALANCES[sellerId].balance += tradeCost;
                    INR_BALANCES[sellerId].locked -= tradeCost;

                    if (!STOCK_BALANCES.hasOwnProperty(buyerID)) {
                        STOCK_BALANCES[buyerID] = {};
                        STOCK_BALANCES[buyerID][buyerStockSymbol] = {
                            yes: { quantity: 0, locked: 0 },
                            no: { quantity: 0, locked: 0 }
                        };
                    }

                    STOCK_BALANCES[buyerID][buyerStockSymbol][buyerStockType].quantity += tradeQuantity;
                    STOCK_BALANCES[sellerId][buyerStockSymbol][buyerStockType].locked -= tradeQuantity;

                    if (!particularStock[buyerStockType][buyerPriceInRs].orders) {
                        particularStock[buyerStockType][buyerPriceInRs].orders = {};
                    }

                    priceCategory.orders[sellerId] -= tradeQuantity;
                    totalTradeQuantity -= tradeQuantity;
                    totalTradeCost += tradeCost;

                    if (priceCategory.orders[sellerId] === 0) {
                        delete priceCategory.orders[sellerId];
                    }
                }

                priceCategory.total -= buyerQuantity - totalTradeQuantity;

                // for structure even if empty
                if (priceCategory.total <= 0) {
                    particularStock[buyerStockType] = {};
                }

                if (totalTradeQuantity > 0) {
                    const reverseStockType = buyerStockType === "yes" ? "no" : "yes";
                    const reversePrice = (10 - buyerPriceInRs).toFixed(1);

                    if (!particularStock[reverseStockType][reversePrice]) {
                        particularStock[reverseStockType][reversePrice] = {
                            total: 0,
                            orders: {}
                        };
                        // particularStock[reverseStockType][reversePrice].orders = {};
                    }

                    particularStock[reverseStockType][reversePrice].total += totalTradeQuantity;

                    if (!particularStock[reverseStockType][reversePrice].orders) {
                        particularStock[reverseStockType][reversePrice].orders = {};
                    }

                    particularStock[reverseStockType][reversePrice].orders[buyerID] =
                        (particularStock[reverseStockType][reversePrice].orders[buyerID] || 0) + totalTradeQuantity;

                    if (!STOCK_BALANCES[buyerID].hasOwnProperty(buyerStockSymbol)) {
                        STOCK_BALANCES[buyerID][buyerStockSymbol] = {};
                        STOCK_BALANCES[buyerID][buyerStockSymbol][reverseStockType] = { quantity: 0, locked: 0 };
                    }

                    STOCK_BALANCES[buyerID][buyerStockSymbol][reverseStockType].quantity += totalTradeQuantity;

                    await publisher.publish(`response.${uid}`, JSON.stringify({
                        error: false,
                        msg: JSON.stringify(ORDERBOOK[buyerStockSymbol][reverseStockType][reversePrice])
                    }));
                    //**//** */ */
                    await publisher.publish(buyerStockSymbol, JSON.stringify({
                        event: "event_orderbook_update",
                        message: JSON.stringify({
                            [reverseStockType]: {
                                [reversePrice]: {
                                    total: totalTradeQuantity,
                                    orders: {
                                        [buyerID]: {
                                            type: "reverted",
                                            quantity: totalTradeQuantity
                                        }
                                    }
                                }
                            }
                        })
                    }));
                    break;
                }

                const orderUpdate = particularStock[buyerStockType][buyerPriceInRs] || {
                    total: 0,
                    orders: {}
                };

                await publisher.publish(`response.${uid}`, JSON.stringify({
                    error: false,
                    msg: JSON.stringify(STOCK_BALANCES[buyerID])
                }));

                await publisher.publish(buyerStockSymbol, JSON.stringify({
                    event: "event_orderbook_update",
                    msg: {
                        [buyerStockType]: {
                            [buyerPriceInRs]: orderUpdate
                        }
                    }
                })
                );

            } else {
                const reverseStockType = buyerStockType === "yes" ? "no" : "yes";
                const reversePrice = (10 - buyerPriceInRs).toFixed(1);

                if (!particularStock[reverseStockType].hasOwnProperty(reversePrice)) {
                    particularStock[reverseStockType][reversePrice] = {
                        total: 0,
                        orders: {}
                    };
                }

                particularStock[reverseStockType][reversePrice].total += buyerQuantity;

                if (!particularStock[reverseStockType][reversePrice].orders) {
                    particularStock[reverseStockType][reversePrice].orders = {};
                }

                particularStock[reverseStockType][reversePrice].orders[buyerID] =
                    (particularStock[reverseStockType][reversePrice].orders[buyerID] || 0) + buyerQuantity;

                if (!STOCK_BALANCES.hasOwnProperty(buyerID)) {
                    STOCK_BALANCES[buyerID] = {};
                }

                if (!STOCK_BALANCES[buyerID].hasOwnProperty(buyerStockSymbol)) {
                    STOCK_BALANCES[buyerID][buyerStockSymbol] = {};
                    STOCK_BALANCES[buyerID][buyerStockSymbol][reverseStockType] = { quantity: 0, locked: 0 };
                }

                if (!STOCK_BALANCES[buyerID][buyerStockSymbol][reverseStockType]) {
                    STOCK_BALANCES[buyerID][buyerStockSymbol][reverseStockType] = { quantity: 0, locked: 0 };
                }

                STOCK_BALANCES[buyerID][buyerStockSymbol][reverseStockType].balance += buyerQuantity;
                INR_BALANCES[buyerID].locked += reversePrice * buyerQuantity

                await publisher.publish(`response.${uid}`, JSON.stringify({
                    error: false,
                    msg: JSON.stringify(particularStock[reverseStockType][reversePrice])
                }));

                await publisher.publish(buyerStockSymbol, JSON.stringify({
                    event: "event_orderbook_update",
                    message: JSON.stringify({
                        msg: {
                            [reverseStockType]: {
                                [reversePrice]: {
                                    total: particularStock[reverseStockType][reversePrice].total,
                                    orders: {
                                        [buyerID]: {
                                            type: "reverted",
                                            quantity: particularStock[reverseStockType][reversePrice].orders[buyerID]
                                        }
                                    }
                                }
                            }
                        }
                    })
                }));
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

            if (!ORDERBOOK.hasOwnProperty(sellerStockSymbol)) {
                ORDERBOOK[sellerStockSymbol] = {
                    yes: {},
                    no: {}
                };
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
                ORDERBOOK[sellerStockSymbol][sellerStockType][sellerPriceInRs].orders[sellerID] = sellerQuantity
            } else {
                ORDERBOOK[sellerStockSymbol][sellerStockType][sellerPriceInRs].orders[sellerID] += sellerQuantity;
            }

            STOCK_BALANCES[sellerID][sellerStockSymbol][sellerStockType].locked += sellerQuantity;
            STOCK_BALANCES[sellerID][sellerStockSymbol][sellerStockType].quantity -= sellerQuantity;


            await publisher.publish(`response.${uid}`, JSON.stringify({
                error: false,
                msg: JSON.stringify(ORDERBOOK)
            }));

            await publisher.publish(sellerStockSymbol, JSON.stringify({
                event: "event_orderbook_update",
                message: JSON.stringify({
                    [sellerStockType]: {
                        [sellerPriceInRs]: {
                            total: ORDERBOOK[sellerStockSymbol][sellerStockType][sellerPriceInRs].total,
                            orders: {
                                [sellerID]: {
                                    type: "sell",
                                    quantity: ORDERBOOK[sellerStockSymbol][sellerStockType][sellerPriceInRs].orders[sellerID]
                                }
                            }
                        }
                    }
                })
            }));
            break;

        case 'resetAll':
            INR_BALANCES = {}
            ORDERBOOK = {}
            STOCK_BALANCES = {}
            await publisher.publish(`response.${uid}`, JSON.stringify({
                error: false,
                msg: `all reset done`
            }));

            break;

        case 'mint':
            const mintId = data.userId;
            const mintStockSymbol = data.stockSymbol;
            const mintQuantity = data.quantity;
            if (!STOCK_BALANCES.hasOwnProperty(mintId)) {
                STOCK_BALANCES[mintId] = {}
                STOCK_BALANCES[mintId][mintStockSymbol] = {
                    "yes": {
                        "quantity": mintQuantity,
                        "locked": 0
                    },
                    "no": {
                        "quantity": mintQuantity,
                        "locked": 0
                    }
                }
            }
            await publisher.publish(`response.${uid}`, JSON.stringify({
                error: false,
                msg: JSON.stringify(STOCK_BALANCES[mintId][mintStockSymbol])
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
