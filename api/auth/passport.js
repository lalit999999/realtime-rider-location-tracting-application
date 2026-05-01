import crypto from 'crypto';

import passport from 'passport';
import { Strategy as GoogleStrategy } from 'passport-google-oauth20';

import { User } from '../../models/User.js';

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const GOOGLE_CALLBACK_URL = process.env.GOOGLE_CALLBACK_URL;

export function configurePassport() {
    if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET || !GOOGLE_CALLBACK_URL) {
        throw new Error('Google OAuth environment variables are missing');
    }

    passport.use(
        new GoogleStrategy(
            {
                clientID: GOOGLE_CLIENT_ID,
                clientSecret: GOOGLE_CLIENT_SECRET,
                callbackURL: GOOGLE_CALLBACK_URL,
            },
            async (accessToken, refreshToken, profile, done) => {
                try {
                    const email = profile.emails?.[0]?.value?.toLowerCase();
                    const avatar = profile.photos?.[0]?.value ?? null;

                    if (!email) {
                        return done(new Error('Google account does not provide an email address'));
                    }

                    let user = await User.findOne({ provider: 'google', providerId: profile.id });

                    if (!user) {
                        user = await User.create({
                            userId: crypto.randomUUID(),
                            name: profile.displayName || profile.name?.givenName || email,
                            email,
                            provider: 'google',
                            providerId: profile.id,
                            avatar,
                            passwordHash: null,
                        });
                    } else {
                        user.name = profile.displayName || user.name;
                        user.email = email;
                        user.avatar = avatar;
                        user.provider = 'google';
                        user.providerId = profile.id;
                        await user.save();
                    }

                    return done(null, user);
                } catch (error) {
                    return done(error);
                }
            },
        ),
    );

    passport.serializeUser((user, done) => {
        done(null, user.userId);
    });

    passport.deserializeUser(async (userId, done) => {
        try {
            const user = await User.findOne({ userId });
            done(null, user || false);
        } catch (error) {
            done(error);
        }
    });

    return passport;
}

export { passport };
