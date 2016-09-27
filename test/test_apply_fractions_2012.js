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
var queue = require('d3-queue').queue



var utils = require('./utils')
var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'
var hpmsfile = rootdir+'/files/hpms2012_reduced.json'
var hpmsgrids={
               '2012':{}
              }

var config_okay = require('config_okay')

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

        queue()
            .defer(fs.readFile,hpmsfile,{'encoding':'utf8'})
            .defer(utils.demo_db_before(options))
            .await(function(e,hpmsdata,blahblah){
                should.not.exist(e)
                hpmsgrids['2012'] = routes.process_hpms_data(JSON.parse(hpmsdata))
                return done()
            })
        return null
    })
    return null
})
//after(utils.demo_db_after(options))


describe('apply fractions route',function(){
    it('should work',function(done){
        var task ={'options':options
                  ,'cell_id':'128_172'
                  ,'year':2012
                  }
        //console.log('grids are'+JSON.stringify(hpmsgrids['2012']['128_172'] ))
        var handler = routes.fractions_handler(hpmsgrids['2012'])
        queue()
        .defer(handler,task)
        .await(function(e,d){
            var memo = {}
            var len
            should.not.exist(e)
            len = Object.keys(task.accum).length
            len.should.equal(745)
            _.each(task.accum,function(v,k){
                var totals = v.totals
                Object.keys(v).forEach(function(key){
                    var record
                    if(key === 'totals') return null
                    record  = v[key]
                    _.each(record,function(vv,kk){
                        // totals should decrement down to zero
                        totals[kk] -= vv
                        return null
                    });
                    return null
                })
                _.each(totals,function(v){
                    v.should.be.approximately(0,0.01) // not exact
                    return null
                });
            });

            // test reduce too
            reduce.reduce(memo,task,function(e,m){
                var start = new Date('2012-01-01 00:00')
                var end =  new Date('2012-02-01 00:00')

                _.each(task,function(v,k){
                    delete(task[k])
                })

                _.each(memo,function(v,ts){
                    ts.should.be.instanceOf(String);
                    (new Date(ts)).should.be.within(start,end)
                    v.should.have.property('detector_based')
                })
                return done()
            })
        })
        return null;
    })
})
