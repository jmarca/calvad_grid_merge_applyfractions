var should=require('should')

var reduce = require('../lib/reduce')
var routes = require('../lib/routes.js')

var csvparser = require('csv-parse')

var queries = require('calvad_grid_merge_sqlquery')
var hourlies = require('calvad_grid_merge_couchdbquery')
var get_hpms_fractions = hourlies.get_hpms_fractions
var post_process_couch_query = hourlies.post_process_couch_query
var get_detector_fractions = hourlies.get_detector_fractions
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
        .defer(routes.grid_hpms_hourly_handler,options,[hpmsfile],app)
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
    it('should work for csv too',function(done){
        request.get(server_host+'/hpms/datahr/2008/189/72.csv'
                   ,function(e,r,b){
                        // b is the output memo I want

                        csvparser(b,{columns:true},function(err,memo){

                            memo.should.have.lengthOf(2976)
                            // not really

                            _.each(memo,function(v,ts){
                                v.should.have.keys([
                                    'timestamp'
                                  ,'roadway'
                                  ,'sum vehicle miles traveled'
                                  ,'sum single unit miles traveled'
                                  ,'sum combination miles traveled'
                                  ,'sum not heavy heavy-duty miles traveled'
                                  ,'sum heavy heavy-duty miles traveled'
                                  ,'sum lane miles'
                                ])
                                if(v.roadway === 'detector_based'){
                                    v['sum single unit miles traveled'].should.eql('')
                                    v['sum combination miles traveled'].should.eql('')
                                }else{
                                    v['sum not heavy heavy-duty miles traveled'].should.eql('')
                                    v['sum heavy heavy-duty miles traveled'].should.eql('')
                                }

                            });


                        });
                        return done()

                    })
        return null;
    })
})
