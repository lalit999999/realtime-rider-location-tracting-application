export function ensureAuthenticated(req, res, next) {
    if (req.isAuthenticated?.() && req.user) {
        return next();
    }

    return res.redirect('/login');
}
