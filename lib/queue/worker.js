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
    //this.client = Worker.client || (Worker.client = redis.createClient());
    this.client = queue.client;
    this.bclient = redis.createClient();
    this.running = true;
    this.job = null;

    // on Redis error events and attempt to restart queue
    var redisErrorOccurred = false;
    var self = this;

    this.bclient.on('error', function(err) {
        //queue.emit('error', err);
        console.log('Received error')
        redisErrorOccurred = true;
        self.queue.shutdown(function(err) {
            console.log('Kue is shut down.', err || '');
        }, 5000, self.type);
    });

    this.bclient.on('ready', function() {
        console.log('READY!')
        if (redisErrorOccurred) {
            redisErrorOccurred = false;
            if (self.resume()) {
                self.start();
            }
        }
    });

    this.on('error', function(err) {
        console.log('here')
        console.log(err);
    });
}

/**
 * Redis connections used by `getJob()` when blocking.
 */

var clients = {};

var async = require('async');
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

    this.processStalledJobs(function() {
        self.processJobs();
    })

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
Worker.prototype.processStalledJobs = function(cb) {
    var self = this;
    console.log('Processing stalled jobs...')
    this.queue.active(function(err, ids) {
        if (!ids) {
            return cb();
        }
        async.map(ids, function(id, callback) {
            self.getJobById(id, function(err, job) {
                job.inactive(function(err, res) {
                    console.log(err)
                    callback(null, res);
                });
            });

        }, function(err, results) {
            cb();
        })
    });
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

Worker.prototype.failed = function(job, cb) {
    var self = this;
    job.error(job._err);

    // Job retry attempts are done as soon as they fail, with no delay
    job.attempt(function(err, remaining, attempts, max) {
        if (err) {
            self.error(err, job);
            return cb(err);
        }
        if (remaining) {
            if (job.backoff()) {
                var delay = job.delay();
                if (_.isFunction(job._getBackoffImpl())) {
                    try {
                        delay = job._getBackoffImpl().apply(job, [attempts]);
                    } catch (e) {
                        self.error(e, job);
                    }
                }
                //job.failed(function(err, res) { // remove job, in order to add delay job re-attempts
                //if (err) return cb(err);
                //job.delay(delay).update(function(err,res){

                job.delay(delay).delayed(function(err, res) { // delayed retry, move job to 'delayed' zset
                    if (err) return cb(err);
                    self.emitJobEvent('failed attempt', job, attempts);
                    return cb();
                });
                //});
                //});
            } else {
                job.inactive(function(err, res) { // retry, move job into 'inactive' zset
                    if (err) return cb(err);
                    self.emitJobEvent('failed attempt', job, attempts);
                    return cb();
                });
            }
        } else { // no more attempts remaining, move job to 'failed' zset
            job.failed(function(err, res) {
                if (err) return cb(err);
                self.emitJobEvent('failed', job);
                return cb();
            });
        }
    });

};

/**
 * ProcessJob `job`, marking it as active,
 * invoking the given callback `fn(job)`,
 * if the job fails `Worker#failed()` is invoked,
 * otherwise the job is marked as "complete".
 *
 * @param {Job} job
 * @param {Function} cb
 * @api public
 */

Worker.prototype.processJob = function(job, cb) {
    var self = this;
    var start = new Date();
    console.log('Processing job...')
    self.handler(job, function(err, result) {
        if (self.drop_user_callbacks) return cb(new Error('Drop user callbacks.'));
        if (err) {
            job._err = err;
            self.failed(job, function(err, res) {
                if (err) return cb(err); //self.error(err, job);
                return cb();
            });
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

        job.complete(function(err, res) {
            job.attempt(function() {
                if (job.removeOnComplete()) {
                    job.remove();
                }
                self.emitJobEvent('complete', job, result);
                return cb();
            });
        });

    }, {
        /**
         * @author behrad
         * @pause: let the processor to tell worker not to continue processing new jobs
         */
        pause: function(fn, timeout) {
            timeout = timeout || 5000;
            self.queue.shutdown(fn, Number(timeout), self.type);
        },
        /**
         * @author behrad
         * @pause: let the processor to trigger restart for they job processing
         */
        resume: function() {
            if (self.resume()) {
                self.start();
            }
        }
    });
};

Worker.prototype.processJobs = function() {
    var self = this;
    console.log('Getting next job...')
    self.getNextJob(function(err, job) {
        if (err) {
            //console.log(err)
            //self.error(err, job);
            return;
        }
        if (!job || err) {
            return self.processJobs();
        }

        self.job = job;
        self.processJob(job, function(err, res) {
            self.processJobs();
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

Worker.prototype.zpop = function(cb) {

    var key = this.client.getKey('jobs:' + this.type + ':inactive');
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
    var client = this.client;
    if (!self.running) {
        return cb("Already Shutdown");
    }

    // BLPOP indicates we have a new inactive job to process
    self.bclient.blpop(client.getKey(self.type + ':jobs'), 0, function(err, jobId) {
        if (err || !self.running) {
            self.client.lpush(client.getKey(self.type + ':jobs'), 1);
            return cb(err);
        }
        // Set job to a temp value so shutdown() knows to wait
        self.job = true;

        self.zpop(function(err, id) {
            if (err || !id) {
                self.job = null;
                return cb(err); // "No job to pop!"
            }
            self.getJobById(id, cb);
        });
    });
};

/**
 * Get job with `id` and callback `fn(err, job)`.
 *
 * @param {Number} id
 * @param {Function} fn
 * @api public
 */

Worker.prototype.getJobById = function(id, fn) {
    //var client = redis.client(),
    var client = this.client;
    var job = new Job(this.type, {}).setClient(this.client);

    job.id = id;
    this.client.hgetall(client.getKey('job:' + job.id), function(err, hash) {
        if (err) return fn(err);
        if (!hash) {
            self.removeBadJob(job.id);
            return fn(new Error('job "' + job.id + '" doesnt exist'));
        }
        if (!hash.type) {
            self.removeBadJob(job.id, hash);
            return fn(new Error('job "' + job.id + '" is invalid'))
        }

        // TODO: really lame, change some methods so
        // we can just merge these
        job.type = hash.type;
        job._delay = hash.delay;
        job.priority(Number(hash.priority));
        job._progress = hash.progress;
        job._attempts = hash.attempts;
        job._max_attempts = hash.max_attempts;
        job._state = hash.state;
        job._error = hash.error;
        job.created_at = hash.created_at;
        job.updated_at = hash.updated_at;
        job.failed_at = hash.failed_at;
        job.duration = hash.duration;
        job._removeOnComplete = hash.removeOnComplete;
        try {
            if (hash.data) job.data = JSON.parse(hash.data);
            if (hash.result) job.result = JSON.parse(hash.result);
            if (hash.backoff) {
                var source = 'job._backoff = ' + hash.backoff + ";";
                //                require('vm').runInContext( source );
                eval(source);
            }
        } catch (e) {
            err = e;
        }
        fn(err, job);
    });
};

Worker.prototype.removeBadJob = function(id, hash) {
    var client = this.client;
    if (hash && hash.state) {
        client.zrem(client.getKey('jobs:' + hash.state), id);
    }
    client.multi()
        .del(client.getKey('job:' + id + ':log'))
        .del(client.getKey('job:' + id))
        .zrem(client.getKey('jobs'), id)
        .exec();
    if (!exports.disableSearch) {
        getSearch().remove(id);
    }
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