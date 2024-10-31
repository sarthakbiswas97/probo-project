import WebSocket, { WebSocketServer } from "ws";
import http from "http";
import { createClient } from "redis";

const server = http.createServer(() => {
  console.log(`websocket server is running`);
});
const wss = new WebSocketServer({ server });
const subscriber = createClient();

const clientSubscriptions = new Map();

wss.on('connection', (ws) => {
  console.log(`New client connected`);

  const subscriptions = new Set();

  clientSubscriptions.set(ws, subscriptions);
  console.log(ws);


  ws.on('message', async (message) => {
    try {
      const data = JSON.parse(message);
      if (data.type === 'subscribe' && data.stockSymbol) {
        subscriptions.add(data.stockSymbol);

        await subscriber.subscribe(data.stockSymbol, (message) => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(message);
          }
        });
        console.log(`Client subscribed to ${data.stockSymbol}`);
      }
    } catch (error) {
      console.error(`Error processing message:`, error);
    }
  });

  ws.on('close', async () => {

    const subs = clientSubscriptions.get(ws);
    if (subs) {
      for (const symbol of subs) {
        await subscriber.unsubscribe(symbol);
      }
    }
    clientSubscriptions.delete(ws);
    console.log('Client disconnected');
  });
});

const PORT = 8080;
server.listen(PORT, async () => {
  try {
    await subscriber.connect();
    console.log(`WebSocket server listening on PORT: ${PORT}`);
  } catch (error) {
    console.error(`Failed to connect to Redis:`, error);
  }
});

export default server;