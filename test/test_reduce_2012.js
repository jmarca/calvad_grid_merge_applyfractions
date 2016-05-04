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


var options={}
var utils = require('./utils')
var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'
var hpmsfile = rootdir+'/files/hpms2012_reduced.json'
var hpmsgrids={'2012':{}}

var config_okay = require('config_okay')

var date = new Date()
var test_db_unique = date.getHours()+'-'
                   + date.getMinutes()+'-'
                   + date.getSeconds()+'-'
                   + date.getMilliseconds()


var header=["ts"
           ,"freeway"
           ,"n"
           ,"hh"
           ,"not_hh"
           ,"o"
           ,"avg_veh_spd"
           ,"avg_hh_weight"
           ,"avg_hh_axles"
           ,"avg_hh_spd"
           ,"avg_nh_weight"
           ,"avg_nh_axles"
           ,"avg_nh_spd"
           ,"miles"
           ,"lane_miles"
           ,"detector_count"
           ,"detectors"]
var unmapper = {}
for (var i=0,j=header.length; i<j; i++){
    unmapper[header[i]]=i
}

before(function(done){
    config_okay(config_file,function(err,c){
        options.couchdb =_.clone(c.couchdb,true)
        options.couchdb.grid_merge_couchdbquery_hpms_db += test_db_unique
        options.couchdb.grid_merge_couchdbquery_detector_db += test_db_unique
        options.couchdb.grid_merge_couchdbquery_state_db += test_db_unique


        // dummy up a done grid and a not done grid in a test db
        queue()
        .defer(fs.readFile,hpmsfile,{'encoding':'utf8'})
        .defer(utils.demo_db_before(options))
        .await(function(e,hpmsdata,blahblah){
            hpmsgrids['2012'] = routes.process_hpms_data(JSON.parse(hpmsdata))
            return done()
        })
        return null
    })
    return null
})
after(utils.demo_db_after(options))


describe('reduce',function(){
    it('should work',function(done){
        var task ={'options':options
                  ,'cell_id':'128_172'
                  ,'year':2012
                  }
        // first apply fractions, which I know works, then test reducing
        var handler = routes.fractions_handler(hpmsgrids['2012'])
        queue()
        .defer(handler,task)
        .await(function(e,d){
            should.not.exist(e)
            var memo = reduce.reduce({},task)
            var memokeys = Object.keys(memo)
            memokeys.should.have.lengthOf(745) // 2012 json include feb 1 zero hr
            fs.readFile('./test/files/128_172_2012_JAN_detectors.json',{'encoding':'utf8'},function(e,data){
                var D = JSON.parse(data)
                _.each(memokeys,function(ts,i){
                    var v = memo[ts]
                    v.should.have.keys(['detector_based'
                                        ,'CO P'
                                        ,'Novato P'
                                        ,'RAMP'
                                       ])
                    _.each(v,function(vv,road_type){
                        if(road_type === 'detector_based'){
                            vv.should.have.keys([ 'n',
                                                  'n_mt',
                                                  'hh_mt',
                                                  'nhh_mt',
                                                  'lane_miles'])
                            should.exist(vv.n)
                            should.exist(vv.n_mt)
                            should.exist(vv.hh_mt)
                            should.exist(vv.nhh_mt)
                            should.exist(vv.lane_miles)
                            vv.n.should.eql(vv.n_mt)

                            vv.n.should.eql(D.rows[i].doc.data[0][unmapper.n] + D.rows[i].doc.data[1][unmapper.n])
                            vv.n_mt.should.eql(D.rows[i].doc.data[0][unmapper.n] + D.rows[i].doc.data[1][unmapper.n])
                            vv.hh_mt.should.eql(D.rows[i].doc.data[0][unmapper.hh]+D.rows[i].doc.data[1][unmapper.hh])
                            vv.nhh_mt.should.eql(D.rows[i].doc.data[0][unmapper.not_hh]+D.rows[i].doc.data[1][unmapper.not_hh])
                        }else{
                            vv.should.have.keys([
                                'sum_lane_miles',
                                'sum_single_unit_mt',
                                'sum_vmt',
                                'sum_combination_mt'])
                            should.exist(vv.sum_lane_miles)
                            should.exist(vv.sum_single_unit_mt)
                            should.exist(vv.sum_vmt)
                            should.exist(vv.sum_combination_mt)
                        }
                        return null
                    })
                    return null
                })
                done()
                return null
            })
            return null
        })
        return null;
    })
})
