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

export const LocationEvent = mongoose.model('LocationEvent', locationEventSchema);
