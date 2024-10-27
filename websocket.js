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


  ws.on('message', async (message) => {
    try {
      const data = JSON.parse(message);
      
      if (data.type === 'subscribe' && data.stockSymbol) {
        // Add to clients subscriptions
        subscriptions.add(data.stockSymbol);
        
        // Subscribe to Redis channel
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
    // Clean up subscriptions when client disconnects
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


//*****************/ older one **************////

// import WebSocket, { WebSocketServer } from "ws";
// import http from "http";
// import { createClient } from "redis";

// const server = http.createServer(() => {
//     console.log(`websocket server is running`);
// });
// const wss = new WebSocketServer({ server });
// const subscriber  = createClient();

// // const clients = new Map();

// wss.on('connection', (ws) => {
//     console.log(`new client has connected`);
//     ws.on('message', (message) => {
//         try {
//             const data = JSON.parse(message);
//             console.log("Clinet Subscribed to ",data)
//             if (data.stockSymbol && (ws.readyState === WebSocket.OPEN)){


//                 subscriber.subscribe(data.stockSymbol, (message) => {
//                     try {
//                         ws.send(message); 
//                     } catch (error) {
//                         console.log(`Error broadcasting orderbook update:`, error);
//                     }
//                 });

//             }


//             // if(data.userId) {
//             //     clients.set(data.userId, ws);
//             //     console.log(`Client ${data.userId} connected`);
//             //     ws.send(JSON.stringify({
//             //         type: 'CONNECTION_SUCCESS',
//             //         message: 'Connected to orderbook updates'
//             //     }));
//             // }



//         } catch (error) {
//             console.log(`Error processing message:`, error);
//         }
//     });

//     ws.on('close', () => {
//     //     for(const [userId, socket] of clients.entries()) {
//     //         if(socket === ws) {
//     //             clients.delete(userId);
//     //             console.log(`Client ${userId} disconnected`);
//     //             break;
//     //         }
//     //     }



//     });

//     console.log("Disconneted")

// });

// // subscriber.subscribe('orderbookWs', (message) => {
// //     try {
// //         // send msg to all connected clients
// //         clients.forEach((ws) => {
// //             if(ws.readyState === WebSocket.OPEN) {
// //                 ws.send(message); // Send the message
// //             }
// //         });
// //     } catch (error) {
// //         console.log(`Error broadcasting orderbook update:`, error);
// //     }
// // });

// const PORT = 8080;
// server.listen(PORT, async () => {
//     try {
//         await subscriber.connect();
//         console.log(`WebSocket server listening on PORT: ${PORT}`);
//     } catch (error) {
//         console.log(`Failed to connect to Redis:`, error);
//     }
// });

// export default server;