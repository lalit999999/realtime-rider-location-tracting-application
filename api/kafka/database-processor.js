import 'dotenv/config';

import mongoose from 'mongoose';

import { LocationEvent } from '../models/LocationEvent.js';
import { kafkaClient } from './kafka-client.js';

const TOPIC = process.env.KAFKA_TOPIC ?? 'location-update';
const GROUP_ID = 'database-processor';
// Previously this processor appended events to a public NDJSON file.
// That behavior has been removed for privacy — events are persisted in MongoDB only.
const DB_URL = process.env.DB_URL;

const lastSeenByUser = new Map();

async function connectMongoDB() {
    if (!DB_URL) {
        throw new Error('DB_URL is not defined in environment variables');
    }

    mongoose.connection.on('connected', () => {
        console.log('MongoDB connection established successfully...');
    });

    mongoose.connection.on('error', (error) => {
        console.error('MongoDB connection error:', error.message);
    });

    mongoose.connection.on('disconnected', () => {
        console.warn('MongoDB connection disconnected');
    });

    await mongoose.connect(DB_URL);
}

// removed: ensureHistoryFile() — no on-disk public history

function parseLocationMessage(messageValue) {
    if (!messageValue) {
        throw new Error('Kafka message is empty');
    }

    const parsed = JSON.parse(messageValue.toString());
    const id = parsed.id ?? parsed.userId ?? parsed.socketId;
    const latitude = Number(parsed.latitude);
    const longitude = Number(parsed.longitude);

    if (!id) {
        throw new Error('Missing user id in Kafka message');
    }

    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
        throw new Error('Invalid latitude or longitude in Kafka message');
    }

    return {
        id: String(id),
        latitude,
        longitude,
        timestamp: parsed.timestamp ?? new Date().toISOString(),
    };
}

async function appendLocationHistory(entry) {
    // No-op: retained function for backward-compatibility with callers,
    // but we no longer write sensitive location history to a public file.
    return;
}

async function startDatabaseProcessor() {
    await connectMongoDB();

    const KafkaConsumer = kafkaClient.consumer({ groupId: GROUP_ID });
    await KafkaConsumer.connect();
    console.log('Database processor connected to Kafka successfully...');

    const kafkaAdmin = kafkaClient.admin();
    await kafkaAdmin.connect();
    await kafkaAdmin.createTopics({
        topics: [{ topic: TOPIC, numPartitions: 2 }],
    });
    await kafkaAdmin.disconnect();

    await KafkaConsumer.subscribe({ topic: TOPIC, fromBeginning: false });

    await KafkaConsumer.run({
        eachMessage: async ({ message, heartbeat, partition }) => {
            try {
                const locationEvent = parseLocationMessage(message.value);
                const currentSignature = `${locationEvent.latitude}:${locationEvent.longitude}`;
                const previousSignature = lastSeenByUser.get(locationEvent.id);

                if (previousSignature === currentSignature) {
                    console.log('Skipping duplicate location event', {
                        id: locationEvent.id,
                        partition,
                        latitude: locationEvent.latitude,
                        longitude: locationEvent.longitude,
                    });
                    await heartbeat();
                    return;
                }

                lastSeenByUser.set(locationEvent.id, currentSignature);

                try {
                    const locationEventDocument = new LocationEvent({
                        userId: locationEvent.id,
                        latitude: locationEvent.latitude,
                        longitude: locationEvent.longitude,
                        timestamp: new Date(locationEvent.timestamp),
                        partition,
                    });

                    await locationEventDocument.save();
                    console.log('Location event saved to MongoDB', {
                        userId: locationEvent.id,
                        partition,
                        latitude: locationEvent.latitude,
                        longitude: locationEvent.longitude,
                    });
                } catch (dbError) {
                    console.error('Error saving location event to MongoDB:', dbError.message);
                }

                // Location event already saved to MongoDB above.
                // Do NOT write location events to public files to avoid data leakage.
                await heartbeat();
            } catch (error) {
                console.error('Error processing Kafka location event:', error.message);
                await heartbeat();
            }
        },
    });
}

startDatabaseProcessor().catch((error) => {
    console.error('Database processor failed to start:', error);
    process.exit(1);
});

