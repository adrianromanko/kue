'use strict';

var chai = chai || require('chai');
var should = chai.should();
var kue = require('../');



describe('Jobs', function() {

   it('should create an instance', function() {
        var jobs = kue.createQueue();
        should.exist(jobs);
    });

    it('should create a job', function() {
        var q = kue.createQueue();

        var jobData = {
            title: 'welcome email for tj',
            to: '"TJ" <tj@learnboost.com>',
            template: 'welcome-email'
        };

        (function() {
            var job = q.create('email', jobData).save(function(err) {
                if (err) throw err;
                job.remove(function(err) {
                    if (err) throw err;
                    console.log('Removed completed job ' + job.id);
                });
            });
            should.exist(job);
        }).should.not.throw();

    });

    it('should be process a job', function(done) {
        var jobs = kue.createQueue();
        jobs.promote(200);

        var jobData = {
            title: 'welcome email for tj',
            to: '"TJ" <tj@learnboost.com>',
            template: 'welcome-email'
        };

        var job = jobs.create('email', jobData).priority('high').save();
        jobs.process('email', function(job, cb) {
            job.data.should.be.eql(jobData);
            job.log('<p>This is <span style="color: green;">a</span> formatted log<p/>');
            cb();
            done()
        });
    });

});