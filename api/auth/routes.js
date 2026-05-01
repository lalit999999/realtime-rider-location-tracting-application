import { Router } from 'express';

import { ensureAuthenticated } from './middleware.js';

export function createAuthRouter(passport) {
    const router = Router();

    router.get(
        '/auth/google',
        passport.authenticate('google', {
            scope: ['profile', 'email'],
            session: true,
        }),
    );

    router.get(
        '/auth/google/callback',
        passport.authenticate('google', {
            failureRedirect: '/login',
            session: true,
        }),
        (req, res) => {
            res.redirect('/app');
        },
    );

    router.get('/logout', (req, res, next) => {
        req.logout((error) => {
            if (error) {
                return next(error);
            }

            req.session.destroy((sessionError) => {
                if (sessionError) {
                    return next(sessionError);
                }

                res.clearCookie('connect.sid');
                res.redirect('/login');
            });
        });
    });

    router.get('/api/me', ensureAuthenticated, (req, res) => {
        const { userId, name, email, avatar, provider } = req.user;
        res.json({ userId, name, email, avatar, provider });
    });

    return router;
}
