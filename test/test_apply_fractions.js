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
            hpmsgrids['2008'] = routes.process_hpms_data(JSON.parse(hpmsdata))
            return done()
        })
        return null
    })
    return null
})
after(utils.demo_db_after(options))

describe('apply fractions',function(){

    it('should work',function(done){
        var task ={'cell_id':'189_72'
                  ,'year':2008
                  }
        task.options = options
        var q = queue(4);
        q.defer(get_detector_fractions,task)
        q.defer(get_hpms_fractions,task)
        q.await(function(e){
            should.not.exist(e)
            queue()
            .defer(post_process_couch_query,task)
            .await(function(ee){
                task.aadt_store = hpmsgrids['2008']['189_72']
                // now have fractions and aadt_store
                reduce.apply_fractions(task,function(e){
                    should.not.exist(e)
                    // should be done
                    // run tests on it here
                    var len = Object.keys(task.accum).length
                    len.should.equal(744)
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

describe('apply fractions route',function(){
    it('should work',function(done){
        var task ={'options':options
                  ,'cell_id':'189_72'
                  ,'year':2008
                  }
        var handler = routes.fractions_handler(hpmsgrids['2008'])
        queue()
        .defer(handler,task)
        .await(function(e,d){
            should.not.exist(e)
            var len = Object.keys(task.accum).length
            len.should.equal(744)
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
            len = Object.keys(task.detector_data).length
            len.should.equal(744)

            return done()
        })
        return null;
    })
    it('should not crash on no work',function(done){
        var task ={'options':options
                  ,'cell_id':'100_222'
                  ,'year':2008
                  }
        var handler = routes.fractions_handler(hpmsgrids['2008'])
        queue()
        .defer(handler,task)
        .await(function(e,d){
            should.not.exist(e)
            var len = Object.keys(task.accum).length
            len.should.equal(0)
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
        return null;
    })
})
