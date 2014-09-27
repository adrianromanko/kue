/*!
 * q - http
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var express = require('express');

// setup
var app = express(),
    stylus = require('stylus'),
    routes = require('./routes'),
    json = require('./routes/json'),
    util = require('util'),
    nib = require('nib');


var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var methodOverride = require('method-override');

// expose the app
module.exports = app;

// stylus config
function compile(str, path) {
    return stylus(str)
        .set('filename', path)
        .use(nib());
}

// config
app.set('view engine', 'jade');
app.set('views', __dirname + '/views');
app.set('title', 'Kue');

app.use(methodOverride('_method'));
app.use(cookieParser());
app.use(bodyParser.urlencoded({
    extended: true
}));
app.use(express.static(__dirname + '/public'));


// routes
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

app.get('/', function(req, res) {
    res.redirect(app.path() + 'active');
});

app.get('/active', routes.jobs('active'));
app.get('/inactive', routes.jobs('inactive'));
app.get('/failed', routes.jobs('failed'));
app.get('/complete', routes.jobs('complete'));
app.get('/delayed', routes.jobs('delayed'));