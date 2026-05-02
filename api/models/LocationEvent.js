import mongoose from 'mongoose';

const locationEventSchema = new mongoose.Schema(
    {
        userId: {
            type: String,
            required: true,
            index: true,
            trim: true,
        },
        latitude: {
            type: Number,
            required: true,
        },
        longitude: {
            type: Number,
            required: true,
        },
        timestamp: {
            type: Date,
            required: true,
            default: Date.now,
            index: true,
        },
        partition: {
            type: Number,
            default: null,
        },
    },
    {
        timestamps: true,
    },
);

locationEventSchema.index({ userId: 1, timestamp: -1 });

// TTL index: remove events after LOCATION_EVENT_TTL_SECONDS (default: 30 days)
const defaultTTL = 30 * 24 * 60 * 60; // 30 days in seconds
const ttlSeconds = Number(process.env.LOCATION_EVENT_TTL_SECONDS) || defaultTTL;
locationEventSchema.index({ timestamp: 1 }, { expireAfterSeconds: ttlSeconds });

export const LocationEvent = mongoose.model('LocationEvent', locationEventSchema);
