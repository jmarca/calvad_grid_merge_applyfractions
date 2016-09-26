"use strict"
var _ = require('lodash')

// hard code some stuff, so I don't accidentally multiple lane miles
// by the hourly fraction which of course is nonsensical
var n_vars = ['sum_vmt']
var nhh_vars=['sum_single_unit_mt']
var hh_vars =['sum_combination_mt']

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
var i,j
for (i=0,j=header.length; i<j; i++){
    unmapper[header[i]]=i
}

/**
 * apply_fractions
 *
 * multiply aadt by the hour's fractional part
 *
 * this function is synchronous, but you can call it like async if you
 * want.  if cb is undefined, it will return task
 *
 * @param {Object} task the task object, with all the stuff
 * @param {Function} cb the callback
 * @returns {Object} task - modified to include accum element that has
 *                   the accumulated fractions applied to totals
 * @throws {}
 *
 */
function apply_fractions(task,cb){
    // now multiply the array, return the result
    // notes:
    //
    // inside of each loop, the record is the record for that hour,
    // the task is where things get accumulated, and the scale
    // variable is the thing that makes sure that I don't over weight
    // anything
    var scale = task.scale
    task.accum = {} // reset the accumulator here


    //console.log('task.aadt_store[0]',task.aadt_store[0])
    // looks like:
    // { sum_vmt: 1026,
    //   sum_lane_miles: 4.36698571248818,
    //   sum_single_unit_mt: 0,
    //   sum_combination_mt: 0,
    //   road_type: 'Rural Minor Collector (MNC)',
    //   f_system: '8',
    //   cell_i: '100',
    //   cell_j: '223',
    //   year: '2008' }
    //throw new Error('croak')
    _.each(task.aadt_store,function(aadt_record,index){
        // console.log('apply fractions, processing record'+index)
        // write out lane miles and initialize structure
        var road_class = aadt_record.road_type
        _.each(task.fractions,function(record,timestamp){
            //console.log(timestamp)
            if(task.accum[timestamp]===undefined){
                task.accum[timestamp]={}
            }
            if(task.accum[timestamp][road_class]===undefined){
                task.accum[timestamp][road_class]={}
            }
            task.accum[timestamp][road_class].sum_lane_miles
                = aadt_record.sum_lane_miles
            return null
        })
        _.each(nhh_vars,function(k2){
            // multiply by nhh
            _.each(task.fractions,function(record,timestamp){
                task.accum[timestamp][road_class][k2] = record.nhh * scale.nhh * aadt_record[k2]
                return null
            })
            return null
        })
        _.each(n_vars,function(k2){
            // multiply by n
            _.each(task.fractions,function(record,timestamp){
                task.accum[timestamp][road_class][k2] = record.n * scale.n * aadt_record[k2]
                return null
            })
            return null
        })
        _.each(hh_vars,function(k2){
            // multiply by hh
            _.each(task.fractions,function(record,timestamp){
                task.accum[timestamp][road_class][k2] = record.hh * scale.hh * aadt_record[k2]
                return null
            })
            return null
        })
        return null
    })

    // take out the trash if needed
    // task.fractions = null
    if(cb !== undefined && typeof cb === 'function') return cb(null,task)
    return task
}
/**
 * apply_fractions_one_hour
 *
 * multiply aadt by the hour's fractional part, for all grids and the hour
 *
 * this function is synchronous, but you can call it like async if you
 * want.  if cb is undefined, it will return task
 *
 * @param {Object} task the task object, with all the stuff
 * @param {Function} cb the callback
 * @returns {Object} task - modified to include accum element that has
 *                   the accumulated fractions applied to totals
 * @throws {}
 *
 */
function apply_fractions_one_hour(task,cb){

    task.accum = {} // reset the accumulator here


    // above case there was one grid cell, many times.
    // now there are many grid cells, one time.
    // I'm not sure I acutally need this as a unique
    // reduce function.


    // in this case,
    //
    // task.aadt_store holds a map.  key is grid cell id.  value is an
    // array of aadt_record, one for each road type.
    //
    // task.fractions also holds a map.  key is grid cell id, value is
    // a map of vehicle class fractions
    //
    // so step through task fractions, and fetch the corresponding
    // aadt for the grid cell from task.aadt_store, and apply the
    // fractions and save the result.


    _.forEach(task.fractions, function(cell_fractions, cellid){

        // task.aadt_store holds all of hpms.  Seems wasteful to hold
        // multiple copies
        var grid_scale = task.scale[cellid]
        // so fetch the aadt for this cell
        var aadt = task.aadt_store[cellid]
        if(task.accum[cellid]===undefined){
            task.accum[cellid]={}
        }

        // filter through the aadt for each
        // roadway type, apply fractions

        _.forEach(aadt,function(aadt_record,index){
            // write out lane miles and initialize structure
            var road_class = aadt_record.road_type
            if(task.accum[cellid][road_class]===undefined){
                task.accum[cellid][road_class]={}
            }

            task.accum[cellid][road_class].sum_lane_miles
                = aadt_record.sum_lane_miles


            _.each(nhh_vars,function(k2){
                // multiply by nhh
                task.accum[cellid][road_class][k2] =
                    cell_fractions.nhh * grid_scale.nhh * aadt_record[k2]
                return null
            })
            _.each(n_vars,function(k2){
                // multiply by n
                task.accum[cellid][road_class][k2] =
                    cell_fractions.n * grid_scale.n * aadt_record[k2]
                return null
            })

            _.each(hh_vars,function(k2){
                // multiply by hh
                task.accum[cellid][road_class][k2] =
                    cell_fractions.hh * grid_scale.hh * aadt_record[k2]
                return null
            })
            return null
        })
        // done interating over all grid cells
        return null
    })
    // take out the trash if needed
    // task.fractions = null
    if(cb !== undefined && typeof cb === 'function') return cb(null,task)
    return task
}


/**
 * reduce
 *
 * call after calling apply_fractions to aggregate together task data
 *
 * returns a memo object containing both detector-based totals, and
 * hpms data.  the previous "totals" data from hpms is deleted
 *
 * There is not grand total,  I delete it here.
 *
 * this function is synchronous, but you can call it like async if you
 * want.  if cb is undefined, it will return the reduced memo output
 *
 * @param {Object} memo the store object to collect items into
 * @param {Object} item the item to collect up
 * @param {function} cb the callback function.  assumes wants err, something
 * @returns {Object}  the memo object, all filled up
 *
 */
function reduce(memo,item,cb){
    // combine item.accum into memo
    // not doing speed or speed limit or whatever at the moment
    // hpms only has design speed and speed limit
    _.each(item.accum,function(roads,ts){
        if(roads.totals !== undefined){
            delete roads.totals
        }
        if(memo[ts]===undefined){
            memo[ts]=_.clone(roads,true)
        }else{
            _.each(roads,function(record,road_class){
                if(memo[ts][road_class]===undefined){
                    memo[ts][road_class]=_.clone(record,true)
                }else{
                    _.each(record,function(v,k){
                        memo[ts][road_class][k] += v
                    })
                }
                return null
            })
        }
        return null
    })

    _.each(item.detector_data,function(record,ts){
        // could also insert speed here into to the sum by
        // multiplying by n to weight it, as I do elsewhere
        //var detector_miles = record[unmapper.miles]
        if(memo[ts]===undefined){
            memo[ts]={}
        }
        // record is either an array, or an array of arrays
        if(! Array.isArray(record[0])){
            record = [record]
        }
        record.forEach(function(r){
            if(memo[ts].detector_based===undefined){
                memo[ts].detector_based={//'n':r[unmapper.n]
                                            'n_mt':r[unmapper.n]
                                            ,'hh_mt':r[unmapper.hh]
                                            ,'nhh_mt':r[unmapper.not_hh]
                                            ,'lane_miles':r[unmapper.lane_miles]
                                            ,'miles':r[unmapper.miles]
                                           }
            }else{
                //memo[ts].detector_based.n      += r[unmapper.n]
                memo[ts].detector_based.n_mt   += r[unmapper.n]
                memo[ts].detector_based.hh_mt  += r[unmapper.hh]
                memo[ts].detector_based.nhh_mt += r[unmapper.not_hh]
                memo[ts].detector_based.lane_miles += r[unmapper.lane_miles]
                memo[ts].detector_based.miles += r[unmapper.miles]
            }
            return null
        })
        return null
    })
    if(cb !== undefined && typeof cb === 'function') cb(null,memo)
    return memo

}

/**
 * reduce_one_hour
 *
 * call after calling apply_fractions to aggregate together task data
 *
 * returns a memo object containing both detector-based totals, and
 * hpms data.  the previous "totals" data from hpms is deleted
 *
 * There is not grand total,  I delete it here.
 *
 * this function is synchronous, but you can call it like async if you
 * want.  if cb is undefined, it will return the reduced memo output
 *
 * @param {Object} memo the store object to collect items into
 * @param {Object} item the item to collect up
 * @param {function} cb the callback function.  assumes wants err, something
 * @returns {Object}  the memo object, all filled up
 *
 */
function reduce_one_hour(memo,item,cb){
    // combine item.accum into memo
    // not doing speed or speed limit or whatever at the moment
    // hpms only has design speed and speed limit
    _.each(item.accum,function(roads,cellid){
        console.log('accumulate '+cellid)
        if(roads.totals !== undefined){
            delete roads.totals
        }
        if(memo[cellid]===undefined){
            memo[cellid]=_.clone(roads,true)
        }else{
            _.each(roads,function(record,road_class){
                if(memo[cellid][road_class]===undefined){
                    memo[cellid][road_class]=_.clone(record,true)
                }else{
                    _.each(record,function(v,k){
                        memo[cellid][road_class][k] += v
                    })
                }
                return null
            })
        }
        return null
    })

    _.each(item.detector_data,function(record,cellid){
        console.log('accumulate '+cellid)
        // could also insert speed here into to the sum by
        // multiplying by n to weight it, as I do elsewhere
        //var detector_miles = record[unmapper.miles]
        if(memo[cellid]===undefined){
            memo[cellid]={}
        }
        // record is either an array, or an array of arrays
        if(! Array.isArray(record[0])){
            record = [record]
        }
        record.forEach(function(r){
            if(memo[cellid].detector_based===undefined){
                memo[cellid].detector_based={//'n':r[unmapper.n]
                                            'n_mt':r[unmapper.n]
                                            ,'hh_mt':r[unmapper.hh]
                                            ,'nhh_mt':r[unmapper.not_hh]
                                            ,'lane_miles':r[unmapper.lane_miles]
                                            ,'miles':r[unmapper.miles]
                                           }
            }else{
                //memo[cellid].detector_based.n      += r[unmapper.n]
                memo[cellid].detector_based.n_mt   += r[unmapper.n]
                memo[cellid].detector_based.hh_mt  += r[unmapper.hh]
                memo[cellid].detector_based.nhh_mt += r[unmapper.not_hh]
                memo[cellid].detector_based.lane_miles += r[unmapper.lane_miles]
                memo[cellid].detector_based.miles += r[unmapper.miles]
            }
            return null
        })
        return null
    })
    if(cb !== undefined && typeof cb === 'function') cb(null,memo)
    return memo

}

exports.apply_fractions=apply_fractions
exports.apply_fractions_one_hour=apply_fractions_one_hour
exports.reduce=reduce
exports.reduce_one_hour=reduce_one_hour
