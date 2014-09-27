'use strict'

var path = require('path'),
    fs = require('fs'),
    rootPath = path.normalize(__dirname + '/..'),
    packageStr = fs.readFileSync('package.json'),
    version = JSON.parse(packageStr).version;

module.exports = {
    production: {
        redis: {
            port: 6379,
            host: '',
            passwd: ''
        },
        express: {
            port: 3001,
            ip: ''
        }
    },
    development: {
        redis: {
            port: 6379,
            host: '127.0.0.1',
            passwd: ''
        },
        express: {
            port: 8080,
            ip: '127.0.0.1'
        }
    },
    version: version
};