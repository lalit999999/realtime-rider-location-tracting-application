import { kafkaClient } from "./kafka-client";

async function init() {
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
}

init()
