'use strict';

module.exports = function(grunt) {

    //Load NPM tasks
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-mocha-test');
    grunt.loadNpmTasks('grunt-env');

    // Project Configuration
    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),


        jshint: {
            all: {
                src: ['Gruntfile.js', 'index.js', 'lib/**/*.js', 'config/*.js'],
                options: {
                    jshintrc: true
                }
            }
        },
        mochaTest: {
            options: {
                reporter: 'spec',
            },
            src: ['test/*.js'],
        },

        env: {
            test: {
                NODE_ENV: 'test'
            }
        },
        jsdoc: {
            dist: {
                src: ['js/models/*.js', 'plugins/*.js'],
                options: {
                    destination: 'doc',
                    configure: 'jsdoc.conf.json',
                    template: './node_modules/grunt-jsdoc/node_modules/ink-docstrap/template',
                    theme: 'flatly'
                }
            }
        }
    });

    //Test task.
    grunt.registerTask('test', ['env:test', 'mochaTest']);
    grunt.registerTask('docs', ['jsdoc']);
};