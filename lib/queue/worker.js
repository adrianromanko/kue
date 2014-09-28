/*!
 * kue - Worker
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var EventEmitter = require('events').EventEmitter,
    redis = require('../redis'),
    events = require('./events'),
    Job = require('./job'),
    _ = require('lodash'),
    domain = require('domain');

/**
 * Expose `Worker`.
 */

module.exports = Worker;

/**
 * Redis connections used by `getJob()` when blocking.
 */

var clients = {};

/**
 * Initialize a new `Worker` with the given Queue
 * targetting jobs of `type`.
 *
 * @param {Queue} queue
 * @param {String} type
 * @api private
 */

function Worker(queue, type) {
    this.queue = queue;
    this.type = type;
    this.client = Worker.client || (Worker.client = redis.createClient());
    this.running = true;
    this.job = null;
}

/**
 * Inherit from `EventEmitter.prototype`.
 */

Worker.prototype.__proto__ = EventEmitter.prototype;

/**
 * Start processing jobs with the given `fn`,
 *
 * @param {Function} fn
 * @return {Worker} for chaining
 * @api private
 */

Worker.prototype.start = function(fn) {
    var self = this;
    self.job = null;
    self.handler = fn;
    if (!self.running) return;
    this.processJobs();

    console.log('***********************************');
    console.log('*          RELIABLE KUE           *');
    console.log('***********************************');
    console.log('*            STARTED              *');
    console.log('***********************************');
    return this;
};

/**
  Process jobs that have been added to the active list
*/
Worker.prototype.processStalledJobs = function() {
    // NOT IMPLEMENTED
};

/**
 * Error handler, currently does nothing.
 *
 * @param {Error} err
 * @param {Job} job
 * @return {Worker} for chaining
 * @api private
 */

Worker.prototype.error = function(err, job) {
    var errorObj = err;
    if (err.stack) {
        errorObj = {
            stack: err.stack,
            message: err.message
        };
    }
    this.emit('error', errorObj, job);
    //    console.error(err.stack || err.message || err);
    return this;
};

/**
 * Process a failed `job`. Set's the job's state
 * to "failed" unless more attempts remain, in which
 * case the job is marked as "inactive" and remains
 * in the queue.
 *
 * @param {Function} fn
 * @return {Worker} for chaining
 * @api private
 */

Worker.prototype.failed = function(job, err) {
    var self = this;
    job.error(err);

    // Job retry attempts are done as soon as they fail, with no delay
    job.attempt(function(error, remaining, attempts, max) {
        if (error) {
            return self.error(error, job);
        }
        if (remaining) {
            var emit = function() {
                self.emitJobEvent('failed attempt', job, attempts);
            };

            if (job.backoff()) {
                var delay = job.delay();
                if (_.isFunction(job._getBackoffImpl())) {
                    try {
                        delay = job._getBackoffImpl().apply(job, [attempts]);
                    } catch (e) {
                        self.error(e, job);
                    }
                }
                job.failed(function() { // remove job, in order to add delay job re-attempts
                    job.delay(delay).update().delayed(emit);
                });
            }
            self.emitJobEvent('failed attempt', job, attempts);
        } else { // no more attempts remaining, remove failed job
            job.failed(function() {
                self.emitJobEvent('failed', job);
            });
        }
    });

};

/**
 * ProcessJobs `job`, marking it as active,
 * invoking the given callback `fn(job)`,
 * if the job fails `Worker#failed()` is invoked,
 * otherwise the job is marked as "complete".
 *
 * @param {Job} job
 * @param {Function} fn
 * @return {Worker} for chaining
 * @api public
 */
Worker.prototype.processJobs = function() {
    var self = this;

    self.getNextJob(function(err, job) {
        if (err) self.error(err, job);
        if (!job || err) return process.nextTick(function() {
            self.processJobs();
        });
        self.job = job;
        var start = new Date();
        self.handler(job, function(err, result) {
            if (self.drop_user_callbacks) return;
            if (err) {
                self.failed(job, err);
                return self.processJobs();
            }
            job.set('duration', job.duration = new Date - start);
            if (result) {
                try {
                    job.result = result;
                    job.set('result', JSON.stringify(result));
                } catch (e) {
                    job.set('result', JSON.stringify({
                        error: true,
                        message: 'Invalid JSON Result: "' + result + '"'
                    }));
                }
            }
            job.complete(function() {
                job.attempt(function() {
                    if (job.removeOnComplete()) {
                        job.remove();
                    }
                    self.emitJobEvent('complete', job, result);
                });
            });
            self.processJobs();
        }, {
            /**
             * @author behrad
             * @pause: let the processor to tell worker not to continue processing new jobs
             */
            pause: function(fn, timeout) {
                timeout = timeout || 5000;
                self.queue.shutdown(self.handler, Number(timeout), self.type);
            },
            /**
             * @author behrad
             * @pause: let the processor to trigger restart for they job processing
             */
            resume: function() {
                if (self.resume()) {
                    self.processJobs();
                }
            }
        });
    });
};

/**
 * Atomic ZPOP implementation.
 *
 * @param {String} key
 * @param {Function} fn
 * @api private
 */

Worker.prototype.zpop = function(key, cb) {
    var KEYS = [key];

    // 1) atomic zpop
    // 2) atomically move job from inactive zset to active zset
    // 3) return jobId
    var script = [
        'local prefix = "q:"',
        'local element = redis.call("ZRANGE", KEYS[1], 0, 0)',
        'local job_id = element[1]',
        'if job_id then',
        'redis.call("ZREMRANGEBYRANK", KEYS[1], 0, 0)',
        'local key_job = prefix .. "job:" .. job_id',
        'local priority = tonumber(redis.call("HGET", key_job, "priority"))',
        'local type = redis.call("HGET", key_job, "type")',
        'local key_a = prefix .. "jobs:active"',
        'local key_b = prefix .. "jobs:" .. type .. ":active"',
        'local key_c = prefix .. "jobs:inactive"',
        'local key_d = prefix .. "jobs:" .. type .. ":inactive"',
        'redis.call("ZADD", key_a, priority, job_id)',
        'redis.call("ZADD", key_b, priority, job_id)',
        'redis.call("ZREM", key_c, job_id)',
        'redis.call("ZREM", key_d, job_id)',
        'redis.call("HSET", key_job, "state", "active")',
        'return job_id',
        'else',
        'return nil',
        'end'
    ].join('\n').toString();

    this.client.eval(script, 1, KEYS[0], function(err, id) {
        if (err) return cb(err);
        return cb(null, id);
    });
};


/**
 * Attempt to fetch the next job.
 *
 * @param {Function} fn
 * @api private
 */

Worker.prototype.getNextJob = function(cb) {
    var self = this;
    if (!self.running) {
        return fn("Already Shutdown");
    }

    var client = clients[self.type] || (clients[self.type] = redis.createClient());
    // BLPOP indicates we have a new inactive job to process
    client.blpop(client.getKey(self.type + ':jobs'), 0, function(err) {
        if (err || !self.running) {
            client.lpush(client.getKey(self.type + ':jobs'), 1);
            return cb(err);
        }
        // Set job to a temp value so shutdown() knows to wait
        self.job = true;

        var key = client.getKey('jobs:' + self.type + ':inactive');
        self.zpop(key, function(err, id) {
            if (err || !id) {
                self.job = null;
                return cb(err); // "No job to pop!"
            }
            Job.get(id, cb);
        });
    });
};

/**
 * Gracefully shut down the worker
 *
 * @param {Function} fn
 * @param {int} timeout
 * @api private
 */

Worker.prototype.shutdown = function(fn, timeout) {
    var self = this,
        shutdownTimer = null;

    // Wrap `fn` so we don't pass `job` to it
    var _fn = function(job) {
        if (job && self.job && job.id != self.job.id) {
            return; // simply ignore older job events currently being received until the right one comes...
        }
        shutdownTimer && clearTimeout(shutdownTimer);
        self.removeAllListeners();
        self.job = null;
        //fix half-blob job fetches if any...
        self.client.lpush(self.client.getKey(self.type + ':jobs'), 1);
        //Safeyly kill any blpop's that are waiting.
        (self.type in clients) && clients[self.type].quit();
        delete clients[self.type];
        self.cleaned_up = true;
        fn();
    };
    if (!this.running) return _fn();
    this.running = false;

    // As soon as we're free, signal that we're done
    if (!this.job) {
        return _fn();
    }
    this.on('job complete', _fn);
    this.on('job failed', _fn);
    this.on('job failed attempt', _fn);

    if (timeout) {
        shutdownTimer = setTimeout(function() {
            if (self.job) {
                if (self.job !== true) {
                    self.drop_user_callbacks = true;
                    self.removeAllListeners();
                    //                    a job is running - fail it and call _fn when failed
                    self.once('job failed', _fn);
                    self.failed(self.job, {
                        error: true,
                        message: "Shutdown"
                    });
                }
            } else {
                // no job running - just finish immediately
                _fn();
            }
        }.bind(this), timeout);
    }
};

Worker.prototype.emitJobEvent = function(event, job, args) {
    if (this.cleaned_up) return;
    events.emit(job.id, event, args);
    this.emit('job ' + event, job);
};

Worker.prototype.resume = function() {
    if (this.running) return false;
    this.running = true;
    return true;
};