/**
 * Module dependencies.
 */
var env = process.env.NODE_ENV || 'development',
    config = require('../config/config')[env],
    kue = require('../lib/kue'),
    Job = require('../lib/queue/job'),
    queue = kue.createQueue();/*{
        prefix: 'q',
        redis: {
            port: config.redis.port,
            host: config.redis.host,
            auth: config.redis.passwd,
            db: 1,
            options: {}
        }
    });*/

/**
 * Serve the index page.
 */

exports.jobs = function(state) {
    return function(req, res) {
        queue.types(function(err, types) {
            res.render('job/list', {
                state: state,
                types: types
            });
        });
    };
};