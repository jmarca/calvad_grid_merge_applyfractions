var should=require('should')
var makedir = require('makedir').makedir


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
        request.get(server_host+'/hpms/data_by_hr/2008/01/22/08.json'
                   ,function(e,r,b){
                       // b is the output memo I want
                       var memo = JSON.parse(b)
                       //console.log(memo)
                       Object.keys(memo).should.have.lengthOf(5)
                       memo.should.have.keys('132_164'
                                             ,'134_163'
                                             ,'100_223'
                                             ,'178_97'
                                             ,'189_72'
                                            );

                       (memo['132_164']).should.have.keys('detector_based');
                       // 132_164 not in reduced hpms file

                       (memo['134_163']).should.have.keys(
                           "detector_based"
                           ,"Urban Other Principal Arterial (OPA)"
                           ,"Urban Minor Arterial (MA)"
                           ,"Urban Collector (COL)"
                       );

                       (memo['189_72']).should.have.keys(
                           "detector_based"
                           ,"Urban Other Principal Arterial (OPA)"
                           ,"Urban Minor Arterial (MA)"
                           ,"Urban Collector (COL)"
                       );

                       (memo['100_223']).should.have.keys(
                           "Rural Minor Collector (MNC)"
                       );

                       (memo['178_97']).should.have.keys(
                           "Rural Major Collector (MJC)"
                           ,"Rural Minor Collector (MNC)"
                       );
                       fs.writeFile('08.json',b,'utf8',function(e){
                           return done()
                       })
                       return null
                    })
        return null;
    })
    it('should work for csv too',function (done){
        request.get(server_host+'/hpms/data_by_hr/2008/01/22/09.csv'
                   ,function(e,r,b){
                       // b is the output memo I want
                       should.not.exist(e)
                       should.exist(b)
                       return done()
                   })
    })
    it('should work for a cached json to csv too',function (done){
        var csv_10
        request.get(server_host+'/hpms/data_by_hr/2008/01/22/10.csv'
                    ,function(e,r,b){
                        // b is the output memo I want
                        should.not.exist(e)
                        should.exist(b)
                        csv_10 = b
                        request.get(server_host+'/hpms/data_by_hr/2008/01/22/10.json'
                                    ,function(e,r,b){
                                        // b is the output memo I want
                                        should.not.exist(e)
                                        should.exist(b)
                                        // put the json file in the expected cache spot
                                        var p ='public/hpms/data_by_hr/2008/01/22'
                                        makedir(p,function(emkdir){
                                            should.not.exist(emkdir)
                                            fs.writeFile(p+'/10.json',b,'utf8',function(e){
                                                request.get(server_host+'/hpms/data_by_hr/2008/01/22/10.csv'
                                                            ,function(e2,r2,b2){
                                                                // bd is the output memo I want
                                                                should.not.exist(e)
                                                                should.exist(b2)
                                                                b2.should.equal(csv_10)
                                                                return done()
                                                            })
                                                return null
                                            })
                                            return null
                                        })
                                        return null
                                    })
                        return null
                    })

        return null
    })

})
