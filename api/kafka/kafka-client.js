import fs from 'fs';

import { Kafka } from 'kafkajs';

const broker = process.env.KAFKA_BROKER ?? 'localhost:9092';
const authMethod = (process.env.KAFKA_AUTH_METHOD ?? '').toLowerCase();
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

function resolveAuthMode() {
    if (authMethod === 'sasl' || authMethod === 'mtls') {
        return authMethod;
    }

    if (process.env.KAFKA_SSL_KEY && process.env.KAFKA_SSL_CERT) {
        return 'mtls';
    }

    if (kafkaUsername && kafkaPassword) {
        return 'sasl';
    }

    return 'none';
}

const kafkaConfig = {
    clientId: 'chai-code',
    brokers: [broker],
};

const resolvedAuthMode = resolveAuthMode();

if (resolvedAuthMode === 'mtls') {
    kafkaConfig.ssl = buildSslConfig();
} else if (resolvedAuthMode === 'sasl') {
    const sslConfig = buildSslConfig();

    kafkaConfig.ssl = sslConfig && typeof sslConfig === 'object' ? sslConfig : true;
    kafkaConfig.sasl = {
        mechanism: kafkaSaslMechanism,
        username: kafkaUsername,
        password: kafkaPassword,
    };
} else {
    kafkaConfig.ssl = broker.includes('localhost') ? false : true;
}

export const kafkaClient = new Kafka(kafkaConfig);