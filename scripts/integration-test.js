#!/usr/bin/env node
import dotenv from 'dotenv';
dotenv.config();

import { Kafka } from 'kafkajs';
import mongoose from 'mongoose';
import { LocationEvent } from '../api/models/LocationEvent.js';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const TOPIC = process.env.KAFKA_TOPIC || 'location-updates';
const MONGO_URI = process.env.MONGO_URI || process.env.MONGODB_URL || 'mongodb://localhost:27017/rider';

async function main() {
    console.log('Starting integration test: produce -> ensure stored in MongoDB');

    await mongoose.connect(MONGO_URI, { dbName: process.env.MONGO_DB || undefined });
    console.log('Connected to MongoDB');

    const kafka = new Kafka({ brokers: KAFKA_BROKERS });
    const producer = kafka.producer();
    await producer.connect();
    console.log('Connected to Kafka brokers:', KAFKA_BROKERS);

    const testUserId = `integration-test-${Date.now()}`;
    const payload = {
        userId: testUserId,
        latitude: 12.34567,
        longitude: 76.54321,
        timestamp: new Date().toISOString(),
    };

    console.log('Sending test message to topic', TOPIC, payload);
    await producer.send({
        topic: TOPIC,
        messages: [{ value: JSON.stringify(payload) }],
    });

    // Wait/poll MongoDB for the created document
    const timeoutMs = 15000;
    const pollInterval = 500;
    const start = Date.now();

    while (Date.now() - start < timeoutMs) {
        const found = await LocationEvent.findOne({ userId: testUserId }).lean().exec();
        if (found) {
            console.log('SUCCESS: Event found in MongoDB:', found);
            await producer.disconnect();
            await mongoose.disconnect();
            process.exit(0);
        }
        await new Promise((r) => setTimeout(r, pollInterval));
    }

    console.error('FAIL: event not found in MongoDB within timeout');
    await producer.disconnect();
    await mongoose.disconnect();
    process.exit(2);
}

main().catch((err) => {
    console.error('Integration test error:', err);
    process.exit(3);
});
