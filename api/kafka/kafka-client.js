import fs from 'fs';

import { Kafka } from 'kafkajs';

const broker = process.env.KAFKA_BROKER ?? 'localhost:9092';
const kafkaUsername = process.env.KAFKA_USERNAME;
const kafkaPassword = process.env.KAFKA_PASSWORD;
const kafkaSaslMechanism = process.env.KAFKA_SASL_MECHANISM ?? 'scram-sha-512';
const rejectUnauthorized = process.env.KAFKA_SSL_REJECT_UNAUTHORIZED !== 'false';

function readPemFile(filePath) {
    if (!filePath) {
        return undefined;
    }

    return fs.readFileSync(filePath, 'utf-8');
}

function buildSslConfig() {
    const ca = readPemFile(process.env.KAFKA_SSL_CA);
    const key = readPemFile(process.env.KAFKA_SSL_KEY);
    const cert = key ? readPemFile(process.env.KAFKA_SSL_CERT) : undefined;

    if (!ca && !cert && !key) {
        return broker.includes('localhost') ? false : true;
    }

    const sslConfig = {
        rejectUnauthorized,
    };

    if (ca) {
        sslConfig.ca = [ca];
    }

    if (cert) {
        sslConfig.cert = cert;
    }

    if (key) {
        sslConfig.key = key;
    }

    return sslConfig;
}

const kafkaConfig = {
    clientId: 'chai-code',
    brokers: [broker],
    ssl: buildSslConfig(),
};

if (kafkaUsername && kafkaPassword) {
    kafkaConfig.sasl = {
        mechanism: kafkaSaslMechanism,
        username: kafkaUsername,
        password: kafkaPassword,
    };
}

export const kafkaClient = new Kafka(kafkaConfig);