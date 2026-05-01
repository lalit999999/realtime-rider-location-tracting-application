import 'dotenv/config';

import http from 'http';
import path from 'path';

import connectMongo from 'connect-mongo';
import express from 'express';
import session from 'express-session';
import mongoose from 'mongoose';
import { Server } from 'socket.io';

import { ensureAuthenticated } from './api/auth/middleware.js';
import { configurePassport, passport } from './api/auth/passport.js';
import { createAuthRouter } from './api/auth/routes.js';
import { kafkaClient } from './api/kafka/kafka-client.js';

const PORT = Number(process.env.PORT ?? 3300);
const DB_URL = process.env.DB_URL;
const SESSION_SECRET = process.env.SESSION_SECRET;
const APP_BASE_URL = process.env.APP_BASE_URL ?? `http://localhost:${PORT}`;
const LOGIN_VIEW_PATH = path.resolve('./public/login.html');
const APP_VIEW_PATH = path.resolve('./public/index.html');

async function connectMongoDB() {
    if (!DB_URL) {
        throw new Error('DB_URL is not defined in environment variables');
    }

    await mongoose.connect(DB_URL);
    console.log('MongoDB connected successfully...');
}

async function main() {
    if (!SESSION_SECRET) {
        throw new Error('SESSION_SECRET is not defined in environment variables');
    }

    await connectMongoDB();
    configurePassport();

    const app = express();
    app.set('trust proxy', 1);
    app.use(express.json());
    app.use(express.urlencoded({ extended: false }));

    const sessionMiddleware = session({
        secret: SESSION_SECRET,
        resave: false,
        saveUninitialized: false,
        store: connectMongo.create({
            mongoUrl: DB_URL,
            collectionName: 'sessions',
        }),
        cookie: {
            httpOnly: true,
            sameSite: 'lax',
            secure: process.env.NODE_ENV === 'production',
        },
    });

    app.use(sessionMiddleware);
    app.use(passport.initialize());
    app.use(passport.session());
    app.use(createAuthRouter(passport));

    const server = http.createServer(app);
    const io = new Server(server, {
        cors: {
            origin: APP_BASE_URL,
            methods: ['GET', 'POST'],
            credentials: true,
        },
    });

    io.attach(server);

    const wrapMiddleware = (middleware) => (socket, next) => middleware(socket.request, {}, next);

    io.use(wrapMiddleware(sessionMiddleware));
    io.use(wrapMiddleware(passport.initialize()));
    io.use(wrapMiddleware(passport.session()));
    io.use((socket, next) => {
        if (socket.request.isAuthenticated?.() && socket.request.user) {
            socket.data.user = socket.request.user;
            socket.data.userId = socket.request.user.userId;
            return next();
        }

        return next(new Error('Unauthorized socket connection'));
    });

    const KafkaProducer = kafkaClient.producer();
    await KafkaProducer.connect();
    console.log('Kafka producer connected successfully...');

    const KafkaConsumer = kafkaClient.consumer({ groupId: `socket-server-${PORT}` });
    await KafkaConsumer.connect();
    console.log('Kafka consumer connected successfully...');

    await KafkaConsumer.subscribe({ topic: 'location-update', fromBeginning: true });
    KafkaConsumer.run({
        eachMessage: async ({ message, heartbeat }) => {
            const data = JSON.parse(message.value.toString());
            console.log('Received message from Kafka', { data });

            io.emit('server:location:update', {
                id: data.id,
                userId: data.userId ?? data.id,
                name: data.name,
                latitude: data.latitude,
                longitude: data.longitude,
                timestamp: data.timestamp,
            });

            await heartbeat();
        },
    });

    io.on('connection', async (socket) => {
        const authenticatedUser = socket.request.user;

        console.log('a user connected', {
            userId: authenticatedUser.userId,
            email: authenticatedUser.email,
        });

        socket.emit('user:authenticated', {
            userId: authenticatedUser.userId,
            name: authenticatedUser.name,
            email: authenticatedUser.email,
            avatar: authenticatedUser.avatar,
        });

        socket.on('client:location:update', async (locationData) => {
            const eventUserId = String(locationData?.userId ?? authenticatedUser.userId);
            const latitude = Number(locationData?.latitude);
            const longitude = Number(locationData?.longitude);

            if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
                socket.emit('client:error', { message: 'Invalid latitude or longitude' });
                return;
            }

            if (eventUserId !== authenticatedUser.userId) {
                console.warn('Socket location userId mismatch; using authenticated userId instead', {
                    eventUserId,
                    authenticatedUserId: authenticatedUser.userId,
                });
            }

            console.log('Received location update from client', {
                userId: authenticatedUser.userId,
                latitude,
                longitude,
            });

            await KafkaProducer.send({
                topic: 'location-update',
                messages: [{
                    key: authenticatedUser.userId,
                    value: JSON.stringify({
                        id: authenticatedUser.userId,
                        userId: authenticatedUser.userId,
                        name: authenticatedUser.name,
                        latitude,
                        longitude,
                        timestamp: new Date().toISOString(),
                    }),
                }],
            });
        });

        socket.on('disconnect', () => {
            console.log('user disconnected', {
                userId: authenticatedUser.userId,
            });
        });
    });

    app.get('/', (req, res) => {
        if (req.isAuthenticated?.() && req.user) {
            return res.redirect('/app');
        }

        return res.redirect('/login');
    });

    app.get('/login', (req, res) => {
        if (req.isAuthenticated?.() && req.user) {
            return res.redirect('/app');
        }

        return res.sendFile(LOGIN_VIEW_PATH);
    });

    app.get('/app', ensureAuthenticated, (req, res) => {
        return res.sendFile(APP_VIEW_PATH);
    });

    app.get('/health', (req, res) => {
        return res.json({ status: 'ok' });
    });

    app.get('/api/me', ensureAuthenticated, (req, res) => {
        const { userId, name, email, avatar, provider } = req.user;
        return res.json({ userId, name, email, avatar, provider });
    });

    server.listen(PORT, () => {
        console.log(`Server is running on port ${PORT}`);
    });
}

main().catch((error) => {
    console.error('Application failed to start:', error);
    process.exit(1);
});
