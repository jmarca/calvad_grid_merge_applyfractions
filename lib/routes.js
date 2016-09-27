"use strict"

var queue = require('d3-queue').queue
var queries = require('calvad_grid_merge_sqlquery')
var hourlies = require('calvad_grid_merge_couchdbquery')
var get_hpms_fractions = hourlies.get_hpms_fractions
var post_process_couch_query = hourlies.post_process_couch_query
var get_detector_fractions = hourlies.get_detector_fractions
// one hour versions
var get_hpms_fractions_one_hour = hourlies.get_hpms_fractions_one_hour
var post_process_couch_query_one_hour = hourlies.post_process_couch_query_one_hour
var get_detector_fractions_one_hour = hourlies.get_detector_fractions_one_hour

var fs = require('fs')
var reduce = require('./reduce.js')

var stringify = require('csv-stringify')
var arrayifier = require('./arrayifier.js')

function task_init(req){
    return {'cell_id':req.params.i+'_'+req.params.j
           ,'cell_i':req.params.i
           ,'cell_j':req.params.j
           ,'year':req.params.yr}
}
function task_init_one_hour(req){
    var year = +req.params.yr
    var month = req.params.month
    var day = req.params.day
    var hour = req.params.hour

    // sanitize

    var leading_zero = /0*(\d+)/;

    var formatted = ([year,month,day,hour]).map(function(input){
        var mo_match = leading_zero.exec(input)
        if(! mo_match && mo_match[1] === undefined){
            throw new Error('Year, month (1 to 12), day (1 to 31), and hour (0 to 23) must be integers, although they can have leading zeros.  For example, 2015/01/01/23.json is okay')
        }
        if(+(mo_match[1] < 10)){
            return '0'+mo_match[1]
        }else{
            return mo_match[1]
        }
    })

    var ts = formatted[0] + '-'
            +formatted[1] + '-'
            +formatted[2] + ' '
            +formatted[3] +':00'

    return {'ts':ts
           ,'year':req.params.yr}
}
// function mixer(task,result){
//     return function(v,k){
//         v.road_type=queries.f_system[k]
//         v.f_system=k
//         v.cell_i = task.cell_i
//         v.cell_j = task.cell_j
//         v.year = task.year
//         result.push(v)
//     }
// }

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


// var hpmsgrids = {}
//var path = require('path')
//var rootdir = path.normalize(__dirname)
//rootdir+'/files/hpms2008.json'

// I'm pretty sure that hpmsgrids would be properly visible here,
// despite slow loading of files.  however, I'm wrapping this in a
// function that takes hpmsgrids as a parameter because for
// convenience testing, using elsewhere

/**
 * Fractions handler
 * @param {Array[Object]} hpmsgrids - an array of data, one Object per year
 * @returns {Function} A function that can handle future requests and
 * apply fractions to the stored HPMS data for the grids in the
 * incoming task object
 */
function fractions_handler(hpmsgrids){
    /**
     * Given a task object, handle fractions for the hpms data for the
     * year specified.  It will go get the detector_fractions and then
     * the hpms fractions, and then handle the results
     * @param {Object} task
     * @param {Integer} task.year - the year
     * @param {Fucntion} cb - send results to this.
     * @returns {} a call to the callback.
     */
    return function(task,cb){

        var q = queue()
        q.defer(get_detector_fractions,task)
            .defer(get_hpms_fractions,task)
            .await(function(e,t1,t2){
                if(e) return cb(e)
                queue()
                    .defer(post_process_couch_query,task)
                    .await(function(ee){
                        if(ee) return cb(ee)
                        task.aadt_store = hpmsgrids[task.cell_id]
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

/**
 * fractions_handler_one_hour
 *
 * Given an hpmsgrids for a particular year, handle a task to apply
 * fractions for all grids.  This will go get the fractions for the
 * specified hour in the year (for both detector cells and hpms-only
 * cells), and then, for each grid cell, will multiply the hourly
 * fraction by the AADT for that grid cell.
 *
 * @param {Object} hpmsgrids - an object holding AADT values for each grid cell
 * @param {Object} task - an object holding the date to process
 * @returns {}  a call to the callback.
 */
function fractions_handler_one_hour(hpmsgrids,task,cb){

    queue(2)
        .defer(get_detector_fractions_one_hour,task)
        .defer(get_hpms_fractions_one_hour,task)
        .await(function(e,t1,t2){
            if(e){
                console.log(e)
                croak()
                return cb(e)
            }
            post_process_couch_query_one_hour(
                task
                ,function(ee){
                    if(ee){
                        console.log(ee)
                        croak()
                        return cb(ee)
                    }
                    task.aadt_store = hpmsgrids
                    reduce.apply_fractions_one_hour(task,function(eee){
                        if(eee) return cb(eee)
                        return cb(null,task)
                    })
                    return null
                })
            return null
        })
    return null
}



function reduce_handler(task){
    return  reduce.reduce({},task)
}

// some of this is stupid and could be done with sync rather than
// async queue stuff
function grid_hpms_hourly_handler(config,hpmsfiles,app,cb){
    var hpmsgrids = {}
    // preload the hpms files
    var q = queue()
    hpmsfiles.forEach(function(f){
        q.defer(fs.readFile,f,{'encoding':'utf8'})
        return null
    })
    q.awaitAll(function(e,data){
        // data is an array of file contents, one per file
        hpmsfiles.forEach(function(f,idx){
            var year = (/(\d\d\d\d)/.exec(f))[1]
            if(!year){
                throw new Error('problem with file: '+f+'.  Must be in form "hpms2008.json"')
            }
            // rejigger the json data for faster lookups

            hpmsgrids[year] = process_hpms_data(JSON.parse(data[idx]))

            return null
        })
        // now hpmsfiles are sorted, time to set up the route

        app.get('/hpms/datahr/:yr/:i/:j.:format?'
               ,function(req,res,next){
                    var task=task_init(req)
                    task.options = config
                   queue()
                       .defer(fractions_handler(hpmsgrids[task.year]),task)
                       .await(function(e2,t){
                           var memo,csv_test
                           if(e2) return next(e2)
                           memo = reduce.reduce({},task)
                           csv_test = /^csv$/i;
                           if(csv_test.test(req.params.format)){
                               // respond with csv
                               res.writeHead(200, { 'Content-Type': 'text/csv' })
                               stringify(arrayifier(memo),function(e3,arr){
                                   if(e3) throw new Error(e3)
                                   res.end(arr)
                               })
                               return null
                           }else{
                               return res.json(memo)

                        }
                    })

                    return null
                })
        return cb()
    })
    return null
}

function grid_hpms_handler_one_hour(config,hpmsfiles,app,cb){
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

        app.get('/hpms/data_by_hr/:yr/:month/:day/:hour.:format?'
                ,function(req,res,next){
                    res.connection.setTimeout(0); // this could take a while
                    // fix this it won't work
                    var task=task_init_one_hour(req)
                    task.options = config
                    fractions_handler_one_hour(
                        hpmsgrids[task.year]
                        ,task
                        ,function(e,t){
                            var memo = {}
                            var csv_test = /^csv$/i;
                            if(e) return next(e)
                            // iterate over each grid cell
                            // memo = {}//_.reduce()reduce.reduce({},task)
                            memo = reduce.reduce_one_hour({},task)
                            if(csv_test.test(req.params.format)){
                                // respond with csv
                                res.writeHead(200, { 'Content-Type': 'text/csv' })
                                stringify(arrayifier(memo),function(e,arr){
                                    res.end(arr)
                                })
                                return null
                            }else{
                                return res.json(memo)

                            }
                        });

                    return null
                })
        return cb()
    })
    return null
}

exports.reduce_handler = reduce_handler
exports.process_hpms_data = process_hpms_data
exports.fractions_handler = fractions_handler
exports.fractions_handler_one_hour = fractions_handler_one_hour
exports.grid_hpms_hourly_handler=grid_hpms_hourly_handler
exports.grid_hpms_handler_one_hour=grid_hpms_handler_one_hour
