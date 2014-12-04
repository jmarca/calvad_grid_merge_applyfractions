var queue = require("queue-async")
var queries = require('calvad_grid_merge_sqlquery')
var hourlies = require('calvad_grid_merge_couchdbquery')
var get_hpms_fractions = hourlies.get_hpms_fractions
var post_process_couch_query = hourlies.post_process_couch_query
var get_detector_fractions = hourlies.get_detector_fractions
var fs = require('fs')
var _ = require('lodash')
var reduce = require('./reduce.js')

function task_init(req){
    return {'cell_id':req.params.i+'_'+req.params.j
           ,'cell_i':req.params.i
           ,'cell_j':req.params.j
           ,'year':req.params.yr}
}
function mixer(task,result){
    return function(v,k){
        v.road_type=queries.f_system[k]
        v.f_system=k
        v.cell_i = task.cell_i
        v.cell_j = task.cell_j
        v.year = task.year
        result.push(v)
    }
}

/** parse_hpms_data
 *
 *  process hpms json data d, return a hash for fast retrieval
 *
 */
function process_hpms_data(d){
    var store = {}
    d.forEach(function(row){
        var cell_id = row.cell_i + '_' + row.cell_j
        if(store[cell_id] === undefined){
            store[cell_id]=[row]
        }else{
            store[cell_id].push(row)
        }
        return null
    })
    return store
}

exports.process_hpms_data = process_hpms_data

// var hpmsgrids = {}
//var path = require('path')
//var rootdir = path.normalize(__dirname)
//rootdir+'/files/hpms2008.json'

// I'm pretty sure that hpmsgrids would be properly visible here,
// despite slow loading of files.  however, I'm wrapping this in a
// function that takes hpmsgrids as a parameter because for
// convenience testing, using elsewhere

function fractions_handler(hpmsgrids){
    return function(task,cb){

        var q = queue()
                .defer(get_detector_fractions,task)
                .defer(get_hpms_fractions,task)
                .await(function(e){
                    if(e) return cb(e)
                    queue()
                    .defer(post_process_couch_query,task)
                    .await(function(ee){
                        if(ee) return cb(ee)
                        task.aadt_store = hpmsgrids[task.year][task.cell_id]
                        // this next thing is actually synchronous, okay
                        // to call like this without wrapping in queue()
                        reduce.apply_fractions(task,function(eee){
                            if(eee) return cb(eee)
                            return cb(null,task)
                        })
                        return null
                    })
                    return null
                })
        return null
    }
}

exports.fractions_handler = fractions_handler

// some of this is stupid and could be done with sync rather than
// async queue stuff
var grid_hpms_hourly_handler = function(config,hpmsfiles,app){
    var hpmsgrids = {}
    // preload the hpms files
    var q = queue()
    hpmsfiles.forEach(function(f){q.defer(fs.readFile,f,{'encoding':'utf8'})})
    q.awaitAll(function(e,data){
        // data is an array of file contents, one per file
        hpmsfiles.forEach(function(f,idx){
            var year = (/(\d\d\d\d)/.exec(f))[1]
            if(!year){ throw new Error('problem with file: '+f+'.  Must be in form "hpms2008.json"')}
            // rejigger the json data for faster lookups

            hpmsgrids[year] = process_hpms_data(JSON.parse(data[idx]))

            return null
        })
        // now hpmsfiles are sorted, time to set up the route

        app.get('/hpms/datahr/:yr/:i/:j.:format?'
               ,function(req,res,next){
                    var task=task_init(req)
                    task.options = config
                    var q = queue(2);
                    res.setTimeout(0) // this could be slow
                    q.defer(get_detector_fractions,task)
                    q.defer(get_hpms_fractions,task)
                    q.await(function(e){
                        if(e) return next(e)
                        queue()
                        .defer(post_process_couch_query,task)
                        .defer(function(cb){
                            fs.readFile('./public/hpms'+task.year+'.json'
                                       ,function(e,d){
                                            task.aadt_store=JSON.parse(d)
                                            return cb()
                                        })
                            return null
                        })
                        .await(function(ee){
                            // now have fractions and aadt_store
                            reduce.apply_fractions(task,function(e){
                                if(e) return next(e)
                                // should be done
                                var result =
                                    _.reduce(task.accum,reduce.reduce,{})
                                res.json(result)
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
    return null
}