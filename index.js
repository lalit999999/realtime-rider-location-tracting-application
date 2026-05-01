import http from 'http';
import express from 'express';
import { Server } from 'socket.io';
import path from 'path';

import { kafkaClient } from './kafka/kafka-client.js';

async function main() {
    const PORT = process.env.PORT ?? 3300;

    const app = express();
    const server = http.createServer(app);
    const io = new Server(server, {
        cors: {
            origin: "*",
            methods: ["GET", "POST"]
        }
    });
    io.attach(server);




    // kafka producer setup
    const KafkaProducer = kafkaClient.producer();
    await KafkaProducer.connect();
    console.log('Kafka producer connected successfully...');

    // kafka consumer setup
    const KafkaConsumer = kafkaClient.consumer({ groupId: `socket-server-${PORT}` });
    await KafkaConsumer.connect();
    console.log('Kafka consumer connected successfully...');

    await KafkaConsumer.subscribe({ topic: 'location-update', fromBeginning: true });
    KafkaConsumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat }) => {
            const data = JSON.parse(message.value.toString());
            console.log('Received message from Kafka', { data });
            io.emit('server:location:update', {
                id: data.id,
                latitude: data.latitude,
                longitude: data.longitude
            });
            await heartbeat();
        }
    });

    io.on('connection', async (socket) => {
        console.log('a user connected', { id: socket.id });

        socket.on('client:location:update', async (LocationData) => {
            const { latitude, longitude } = LocationData;

            console.log('Received location update from client', { id: socket.id, LocationData });
            await KafkaProducer.send({
                topic: 'location-update',
                messages: [{
                    key: socket.id,
                    value: JSON.stringify({ id: socket.id, latitude, longitude })
                }]
            });
        });
    });


    app.use(express.static(path.resolve('./public')));

    app.get('/health', (req, res) => {
        return res.json({ status: 'ok' })

    })
    server.listen(PORT, () => {
        console.log(`Server is running on port ${PORT}`);
    });
}

main()
