var should=require('should')

var reduce = require('../lib/reduce')
var routes = require('../lib/routes')

var queries = require('calvad_grid_merge_sqlquery')
var hourlies = require('calvad_grid_merge_couchdbquery')
var get_hpms_fractions_one_hour = hourlies.get_hpms_fractions_one_hour
var post_process_couch_query_one_hour = hourlies.post_process_couch_query_one_hour
var get_detector_fractions_one_hour = hourlies.get_detector_fractions_one_hour
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
            if(e){throw new Error(e)}
            hpmsgrids['2008'] = routes.process_hpms_data(JSON.parse(hpmsdata))
            return done()
        })
        return null
    })
    return null
})
//after(utils.demo_db_after(options))

describe('apply fractions one hour',function(){

    it('should work 2008',function(done){
        var task = {'options':_.clone(options,true)
                    ,'ts':"2008-01-21 18:00"
                    ,'year':2008}
        var q = queue()
        q.defer(get_hpms_fractions_one_hour,task)
        q.defer(get_detector_fractions_one_hour,task)
        q.await(function(e){
            should.not.exist(e)
            queue()
            .defer(post_process_couch_query_one_hour,task)
            .await(function(ee){
                task.aadt_store = hpmsgrids['2008']
                // now have fractions and aadt_store
                console.log('task before fractions',task)
                reduce.apply_fractions_one_hour(task,function(e){
                    should.not.exist(e)
                    console.log('task after fractions',task)
                    // should be done
                    // run tests on it here
                    //var len =
                    Object.keys(task.accum).sort().should.eql(['100_223'
                                                               ,'132_164'
                                                               ,'134_163'
                                                               ,'178_97'
                                                               ,'189_72'
                                                              ])

                    _.each(task.accum,function(v,k){
                        var totals = v.totals
                        Object.keys(v).forEach(function(key){
                            if(key === 'totals') return null
                            var record  = v[key]
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
                    return done()
                })
                return null

            })
            return null
        })
        return null
    })
    it('should work 2012',function(done){
        var task = {'options':_.clone(options,true)
                    ,'ts':"2012-01-21 18:00"
                    ,'year':2012}
        var q = queue(2)
        q.defer(get_detector_fractions_one_hour,task)
        q.defer(get_hpms_fractions_one_hour,task)
        q.await(function(e){
            should.not.exist(e)
            queue()
            .defer(post_process_couch_query_one_hour,task)
            .await(function(ee){
                task.aadt_store = hpmsgrids['2008']
                // now have fractions and aadt_store
                reduce.apply_fractions_one_hour(task,function(e){
                    should.not.exist(e)
                    // should be done
                    // run tests on it here
                    var len = Object.keys(task.accum).length
                    len.should.equal(2)
                    _.each(task.accum,function(v,k){
                        var totals = v.totals
                        Object.keys(v).forEach(function(key){
                            if(key === 'totals') return null
                            var record  = v[key]
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
                    return done()
                })
                return null

            })
            return null
        })
        return null
    })

})

// same test, but for the route version

describe('apply fractions route_one_hour',function(){
    it('should work',function(done){
        var task ={'options':options
                   ,'ts':"2008-01-21 18:00"
                   ,'year':2008
                  }
        var handler = routes.fractions_handler_one_hour
        queue()
            .defer(handler,hpmsgrids['2008'],task)
        .await(function(e,d){
            should.not.exist(e)
            console.log(task)
            var len = Object.keys(task.accum).length
            len.should.equal(5)
            _.each(task.accum,function(v,k){
                var totals = v.totals
                Object.keys(v).forEach(function(key){
                    if(key === 'totals') return null
                    var record  = v[key]
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
            // also check detector_data
            len = Object.keys(task.detector_data).length
            len.should.equal(3)

            return done()
        })
        return null;
    })
})
