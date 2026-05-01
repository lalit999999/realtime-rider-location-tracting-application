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
const STALE_USER_TIMEOUT_MS = Number(process.env.STALE_USER_TIMEOUT_MS ?? 15000);
const STALE_SWEEP_INTERVAL_MS = Number(process.env.STALE_SWEEP_INTERVAL_MS ?? 5000);
const LOGIN_VIEW_PATH = path.resolve('./public/login.html');
const APP_VIEW_PATH = path.resolve('./public/index.html');

function buildLocationKey(latitude, longitude) {
    return `${latitude}:${longitude}`;
}

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

    const activeUsers = new Map();

    function getUserState(user) {
        const existingUserState = activeUsers.get(user.userId);

        if (existingUserState) {
            return existingUserState;
        }

        const createdUserState = {
            userId: user.userId,
            name: user.name,
            email: user.email,
            avatar: user.avatar ?? null,
            lastSeenAt: Date.now(),
            lastLocationKey: null,
            lastLocation: null,
            sockets: new Set(),
        };

        activeUsers.set(user.userId, createdUserState);
        return createdUserState;
    }

    function snapshotActiveUsers() {
        return Array.from(activeUsers.values()).map((userState) => ({
            userId: userState.userId,
            name: userState.name,
            email: userState.email,
            avatar: userState.avatar,
            lastSeenAt: userState.lastSeenAt,
            latitude: userState.lastLocation?.latitude ?? null,
            longitude: userState.lastLocation?.longitude ?? null,
            timestamp: userState.lastLocation?.timestamp ?? null,
        }));
    }

    function unregisterSocketFromUser(socket, reason = 'disconnect') {
        const userId = socketToUserId.get(socket.id);

        if (!userId) {
            return;
        }

        const userState = activeUsers.get(userId);

        socketToUserId.delete(socket.id);

        if (!userState) {
            return;
        }

        userState.sockets.delete(socket.id);

        if (userState.sockets.size === 0) {
            activeUsers.delete(userId);
            io.emit('server:user:disconnected', { userId, reason });
        }
    }

    const socketToUserId = new Map();

    const staleSweepTimer = setInterval(() => {
        const now = Date.now();

        for (const [userId, userState] of activeUsers.entries()) {
            const isStale = now - userState.lastSeenAt > STALE_USER_TIMEOUT_MS;

            if (!isStale) {
                continue;
            }

            activeUsers.delete(userId);

            for (const socketId of userState.sockets) {
                socketToUserId.delete(socketId);
            }

            io.emit('server:user:inactive', {
                userId,
                reason: 'stale',
                lastSeenAt: userState.lastSeenAt,
            });
        }
    }, STALE_SWEEP_INTERVAL_MS);

    staleSweepTimer.unref?.();

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
        const userState = getUserState(authenticatedUser);

        userState.sockets.add(socket.id);
        userState.lastSeenAt = Date.now();
        socketToUserId.set(socket.id, authenticatedUser.userId);

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

        socket.emit('server:active-users', snapshotActiveUsers());

        socket.on('client:location:update', async (locationData) => {
            const eventUserId = String(locationData?.userId ?? authenticatedUser.userId);
            const latitude = Number(locationData?.latitude);
            const longitude = Number(locationData?.longitude);
            const timestamp = locationData?.timestamp ?? new Date().toISOString();

            if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
                socket.emit('client:error', { message: 'Invalid latitude or longitude' });
                return;
            }

            if (locationData?.userId && eventUserId !== authenticatedUser.userId) {
                socket.emit('client:error', { message: 'Authenticated user mismatch' });
                return;
            }

            const locationKey = buildLocationKey(latitude, longitude);

            if (userState.lastLocationKey === locationKey) {
                socket.emit('client:duplicate', {
                    userId: authenticatedUser.userId,
                    latitude,
                    longitude,
                });
                return;
            }

            userState.lastLocationKey = locationKey;
            userState.lastLocation = { latitude, longitude, timestamp };
            userState.lastSeenAt = Date.now();

            if (eventUserId !== authenticatedUser.userId) {
                console.warn('Socket location userId mismatch; ignoring client-provided userId', {
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
                        timestamp,
                    }),
                }],
            });
        });

        socket.on('disconnect', () => {
            console.log('user disconnected', {
                userId: authenticatedUser.userId,
            });

            unregisterSocketFromUser(socket, 'disconnect');
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

    app.get('/api/active-users', ensureAuthenticated, (req, res) => {
        return res.json(snapshotActiveUsers());
    });

    server.listen(PORT, () => {
        console.log(`Server is running on port ${PORT}`);
    });
}

main().catch((error) => {
    console.error('Application failed to start:', error);
    process.exit(1);
});
