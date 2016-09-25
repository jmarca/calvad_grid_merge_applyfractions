var should=require('should')


var me = require('../.')

var csvparser = require('csv-parse')


var fs = require('fs')
var _ = require('lodash')
var queue = require('d3-queue').queue


var utils = require('./utils')
var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'
var hpmsfile = rootdir+'/files/hpms2008_reduced.json'
var hpmsgrids={'2008':{}}

var config_okay = require('config_okay')

var request = require('request')

var express = require('express')

var date = new Date()
var test_db_unique = date.getHours()+'-'
                   + date.getMinutes()+'-'
                   + date.getSeconds()+'-'
                   + date.getMilliseconds()

var task
var env = process.env;
var testhost = env.TEST_HOST || '127.0.0.1'
var testport = env.TEST_PORT || 3000
testport += 3

var date = new Date()
var test_db_unique = date.getHours()+'-'
                   + date.getMinutes()+'-'
                   + date.getSeconds()+'-'
                   + date.getMilliseconds()
var options = {}


before(function(done){
    config_okay(config_file,function(err,c){
        options.couchdb =_.clone(c.couchdb,true)
        options.couchdb.grid_merge_couchdbquery_hpms_db += test_db_unique
        options.couchdb.grid_merge_couchdbquery_detector_db += test_db_unique
        options.couchdb.grid_merge_couchdbquery_state_db += test_db_unique

        var app = express()
        queue()
        .defer(me.grid_hpms_handler_one_hour,options,[hpmsfile],app)
        .defer(utils.demo_db_before(options))
        .await(function(e){
            should.not.exist(e)
            app.listen(testport,testhost,done)
            return  null
        })
        return null
    })
    return null
})
after(utils.demo_db_after(options))

var server_host = 'http://'+testhost + ':'+testport

describe('server route',function(){
    it('should work',function(done){
        request.get(server_host+'/hpms/data_by_hr/2008/01/22.json'
                   ,function(e,r,b){
                        // b is the output memo I want
                        var memo = JSON.parse(b)
                        Object.keys(memo).should.have.lengthOf(3)
                        return done()

                    })
        return null;
    })
    it('should work for csv too')
})
