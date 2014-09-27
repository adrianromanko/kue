/**
 * Module dependencies.
 */

var express = require('express');

var app = express(),
    stylus = require('stylus'),
    env = process.env.NODE_ENV || 'development',
    config = require('./config/config')[env],
    util = require('util'),
    nib = require('nib'),
    logger = require('morgan'),
    cookieParser = require('cookie-parser'),
    bodyParser = require('body-parser'),
    methodOverride = require('method-override');

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
app.set('port', config.express.port);

// routes
require('./config/routes')(app);

app.listen(app.get('port'), function(){
  console.log('Kue front-end server listening on port ' + app.get('port'));
});

module.exports = app;