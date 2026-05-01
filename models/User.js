import mongoose from 'mongoose';

const userSchema = new mongoose.Schema(
    {
        userId: {
            type: String,
            required: true,
            unique: true,
            index: true,
            trim: true,
        },
        name: {
            type: String,
            required: true,
            trim: true,
        },
        email: {
            type: String,
            required: true,
            unique: true,
            index: true,
            lowercase: true,
            trim: true,
        },
        provider: {
            type: String,
            default: 'google',
            trim: true,
        },
        providerId: {
            type: String,
            required: true,
            unique: true,
            index: true,
            trim: true,
        },
        avatar: {
            type: String,
            default: null,
            trim: true,
        },
        passwordHash: {
            type: String,
            default: null,
        },
    },
    {
        timestamps: true,
    },
);

userSchema.index({ email: 1 }, { unique: true });
userSchema.index({ userId: 1 }, { unique: true });
userSchema.index({ provider: 1, providerId: 1 }, { unique: true });

export const User = mongoose.model('User', userSchema);
