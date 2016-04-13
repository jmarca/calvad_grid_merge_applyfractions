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
var hpmsfile = rootdir+'/files/hpms2008.json'
var hpmsgrids={'2008':{}}

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
            hpmsgrids['2008'] = routes.process_hpms_data(JSON.parse(hpmsdata))
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
                  ,'cell_id':'189_72'
                  ,'year':2008
                  }
        // first apply fractions, which I know works, then test reducing
        var handler = routes.fractions_handler(hpmsgrids['2008'])
        queue()
        .defer(handler,task)
        .await(function(e,d){
            should.not.exist(e)
            var memo = reduce.reduce({},task)
            var memokeys = Object.keys(memo)
            memokeys.should.have.lengthOf(744)
            fs.readFile('./test/files/189_72_2008_JAN.json',{'encoding':'utf8'},function(e,data){
                var D = JSON.parse(data)
                _.each(memokeys,function(ts,i){
                    var v = memo[ts]
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
                            vv.n.should.eql(vv.n_mt)
                            D.rows[i].doc.data[unmapper.n].should.eql(vv.n)
                            D.rows[i].doc.data[unmapper.hh].should.eql(vv.hh_mt)
                            D.rows[i].doc.data[unmapper.not_hh].should.eql(vv.nhh_mt)
                        }else{
                        vv.should.have.keys([
                            'sum_lane_miles',
                            'sum_single_unit_mt',
                            'sum_vmt',
                            'sum_combination_mt'])
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
