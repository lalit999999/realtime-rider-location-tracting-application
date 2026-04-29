import http from 'http';
import express from 'express';
import { Server } from 'socket.io';
import path from 'path';

async function main() {
    const PORT = process.env.PORT ?? 3300;

    const app = express();
    const server = http.createServer(app);
    const io = new Server();
    io.attach(server);

    io.on('connection', (socket) => {
        console.log('a user connected', { id: socket.id });
        socket.on('client:location:update', (LocationData) => {
            const { latitude, longitude } = LocationData;
            console.log('Received location update from client', { id: socket.id, LocationData });
        })
    })

    // app.use(express.static(path.resolve(__dirname, 'public')))

    app.get('/health', (req, res) => {
        return res.json({ status: 'ok' })

    })
    server.listen(PORT, () => {
        console.log(`Server is running on port ${PORT}`);
    });
}

main()
