import { kafkaClient } from './kafka-client.js';

const TOPIC = process.env.KAFKA_TOPIC ?? 'location-update';


async function setup() {
    const admin = kafkaClient.admin();
    console.log('Connecting to Kafka...');
    await admin.connect();
    console.log('Connected to Kafka successfully...');

    await admin.createTopics({
        topics: [
            {
                topic: TOPIC,
                numPartitions: 2,
            }
        ]
    });
    await admin.disconnect();
    console.log('Kafka topic created successfully...');
}

setup().catch((error) => {
    console.error('Error setting up Kafka:', error);
    process.exit(1);
});