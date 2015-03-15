/*global require before describe after it console */

// test for flatten

var reduce = require('../lib/reduce')
var routes = require('../lib/routes.js')

var queries = require('calvad_grid_merge_sqlquery')
var hourlies = require('calvad_grid_merge_couchdbquery')
var get_hpms_fractions = hourlies.get_hpms_fractions
var post_process_couch_query = hourlies.post_process_couch_query
var get_detector_fractions = hourlies.get_detector_fractions

var f = require('../lib/flatten')
var flatten_records = f.flatten_records

var config_okay = require('config_okay')

var should = require('should')

var queue = require('queue-async')
var _ = require('lodash')
var utils = require('./utils.js')
var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'

var superagent = require('superagent')
var date = new Date()
var test_db_unique = date.getHours()+'-'
                   + date.getMinutes()+'-'
                   + date.getSeconds()+'-'
                   + date.getMilliseconds()

var hpmsfile = rootdir+'/files/hpms2008.json'
var hpmsgrids={'2008':{}}

var options = {}
var fs = require('fs')

before(function(done){
    config_okay(config_file,function(err,c){
        options.couchdb=c.couchdb
        options.couchdb.grid_merge_couchdbquery_hpms_db += test_db_unique
        options.couchdb.grid_merge_couchdbquery_detector_db += test_db_unique
        options.couchdb.grid_merge_couchdbquery_state_db += test_db_unique

        queue()
        .defer(fs.readFile,hpmsfile,{'encoding':'utf8'})
        .defer(utils.demo_db_before(options))
        .await(function(e,hpmsdata,blahblah){
            should.not.exist(e)
            hpmsgrids['2008'] = routes.process_hpms_data(JSON.parse(hpmsdata))
            return done()
        })
        return null
    })
    return null
})

after(utils.demo_db_after(options))

describe('flatten',function(){

    it('should flatten',function(done){
        var task ={'options':options
                  ,'cell_id':'189_72'
                  ,'year':2008
                  }
        var handler = routes.fractions_handler(hpmsgrids[2008])
        queue(1)
        .defer(handler,task)
        .await(function(e,d){
            should.not.exist(e)
            // now flatten it, I guess
            var finish_task = {'area_type':'blech'
                              ,'area_name':'blanch'
                              ,'year':task.year
                      }
            finish_task.grid_cells = [task.grid_cell]
            finish_task.result = reduce.reduce({},task)

            flatten_records(finish_task,function(e,doc){
                should.not.exist(e)
                should.exist(doc)
                doc.should.not.have.property('result')
                doc.should.have.property('data')
                doc.should.have.property('header')
                var h = doc.header
                h.should.eql(['timestamp'
                             ,'roadway'
                             ,'sum vehicle miles traveled'
                             ,'sum single unit miles traveled'
                             ,'sum not heavy heavy-duty miles traveled'
                             ,'sum combination miles traveled'
                             ,'sum heavy heavy-duty miles traveled'
                             ,'sum lane miles'
                             ])
                doc.data.should.be.an.Array
                var re = /detector/i;
                _.each(doc.data,function(vv,road_type){
                    if(re.test(vv[1])){
                        should.not.exist(vv[3])
                        should.not.exist(vv[5])
                    }else{
                        should.not.exist(vv[4])
                        should.not.exist(vv[6])
                    }
                });

                return done()
            });
            return null
        })
        return null
    })
})

describe ('make_id',function(){
    it('should make an id',function(){
        should.exist(f.make_id)
        f.make_id.should.be.a.Function;
        f.make_id({year:2007,
                         area_type: 'airbasin',
                         area_name: 'NORTH COAST' }).should.eql('airbasin_NORTH COAST_2007')
        return null
    });
})
