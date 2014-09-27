'use strict';

var kue = require('../..');
var express = require('express');
var q = kue.createQueue();

process.once('SIGTERM', function(sig) {
    queue.shutdown(function(err) {
        console.log('Kue is shut down.', err || '');
        process.exit(0);
    }, 5000);
});


var app = express();

app.set('port', 8080);
app.use(kue.app);
app.listen(app.get('port'));


var jobs = kue.createQueue();
jobs.promote(200);

var jobData = {
    title: 'welcome email for tj',
    to: '"TJ" <tj@learnboost.com>',
    template: 'welcome-email'
};

var job = jobs.create('email', jobData).priority('high').save();
jobs.process('email', function(job, cb) {

    cb();

});