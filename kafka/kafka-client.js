import { Kafka} from 'kafkajs';

export const kafkaClient = new Kafka({
    clientId: 'chai-code',
    brokers: ['localhost:9092']
});