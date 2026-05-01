import http from 'http';
import express from 'express';
import { Server } from 'socket.io';
import path from 'path';

import { kafkaClient } from './kafka/kafka-client.js';

async function main() {
    const PORT = process.env.PORT ?? 3300;

    const app = express();
    const server = http.createServer(app);
    const io = new Server();

    // kafka setup
    const KafkaProducer = kafkaClient.producer();
    await KafkaProducer.connect();
    console.log('Kafka producer connected successfully...');
    io.attach(server);

    io.on('connection', async (socket) => {
        console.log('a user connected', { id: socket.id });
        socket.on('client:location:update', (LocationData) => {
            const { latitude, longitude } = LocationData;
            console.log('Received location update from client', { id: socket.id, LocationData });
        });
        await KafkaProducer.send({
            topic: 'location-update',
            messages: [{
                key: socket.id,
                value: JSON.stringify({ id: socket.id, latitude, longitude })
            }]
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
