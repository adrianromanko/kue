'use strict';

/**
 * Module dependencies.
 */
var config = require('./config'),
    routes = require('../controllers'),
    json = require('../controllers/json');


module.exports = function(app) {
    app.get('/stats', json.stats);
    app.get('/job/search', json.search);
    app.get('/jobs/:from..:to/:order?', json.jobRange);
    app.get('/jobs/:type/:state/:from..:to/:order?', json.jobTypeRange);
    app.get('/jobs/:state/:from..:to/:order?', json.jobStateRange);
    app.get('/job/types', json.types);
    app.get('/job/:id', json.job);
    app.get('/job/:id/log', json.log);
    app.put('/job/:id/state/:state', json.updateState);
    app.put('/job/:id/priority/:priority', json.updatePriority);
    app.delete('/job/:id', json.remove);
    app.post('/job', json.createJob);

    app.get('/active', routes.jobs('active'));
    app.get('/inactive', routes.jobs('inactive'));
    app.get('/failed', routes.jobs('failed'));
    app.get('/complete', routes.jobs('complete'));
    app.get('/delayed', routes.jobs('delayed'));

    app.get('/', function(req, res) {
        res.redirect(app.path() + 'active');
    });
};