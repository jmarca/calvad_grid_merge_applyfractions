var should=require('should')

var reduce = require('../lib/reduce')
var routes = require('../lib/routes.js')

var queries = require('calvad_grid_merge_sqlquery')
var hourlies = require('calvad_grid_merge_couchdbquery')
var get_hpms_fractions = hourlies.get_hpms_fractions
var post_process_couch_query = hourlies.post_process_couch_query
var get_detector_fractions = hourlies.get_detector_fractions
var fs = require('fs')
var _ = require('lodash')
var queue = require("queue-async")


var config={}
var utils = require('./utils')
var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'
var hpmsfile = rootdir+'/files/hpms2008.json'
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

before(function(done){
    config_okay(config_file,function(err,c){
        config.couchdb =_.clone(c.couchdb,true)
        var date = new Date()
        var test_db_unique = date.getHours()+'-'
                           + date.getMinutes()+'-'
                           + date.getSeconds()+'-'
                           + date.getMilliseconds()

        config.couchdb.hpms_db += test_db_unique
        config.couchdb.detector_db += test_db_unique
        config.couchdb.state_db += test_db_unique

        var app = express()
        queue()
        .defer(routes.grid_hpms_hourly_handler,config,[hpmsfile],app)
        .defer(utils.demo_db_before(config))
        .await(function(e){
            should.not.exist(e)
            app.listen(testport,testhost,done)
            return  null
        })
        return null
    })
    return null
})
after(utils.demo_db_after(config))

var server_host = 'http://'+testhost + ':'+testport

describe('server route',function(){
    it('should work',function(done){
        request.get(server_host+'/hpms/datahr/2008/189/72.json'
                   ,function(e,r,b){
                        // b is the output memo I want
                        var memo = JSON.parse(b)
                        Object.keys(memo).should.have.lengthOf(744)
                        _.each(memo,function(v,ts){
                            v.should.have.keys(['detector_based'
                                               ,'Urban Other Principal Arterial (OPA)'
                                               ,'Urban Minor Arterial (MA)'
                                               ,'Urban Collector (COL)'
                                               ,'hpms_totals'
                                               ])
                            _.each(v,function(vv,road_type){
                                if(road_type === 'detector_based'){
                                    vv.should.have.keys([ 'n',
                                                          'n_mt',
                                                          'hh_mt',
                                                          'nhh_mt',
                                                          'lane_miles'])
                                }else{
                                    vv.should.have.keys([
                                        'sum_lane_miles',
                                        'sum_single_unit_mt',
                                        'sum_vmt',
                                        'sum_combination_mt'])
                                }

                            });


                        });
                        return done()

                    })
        return null;
    })
})
