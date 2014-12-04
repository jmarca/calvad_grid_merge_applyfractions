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
for (var i=0,j=header.length; i<j; i++){
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

    task.accum = {} // reset the accumulator here
    var scale = task.scale

    //console.log(task.aadt_store[0])
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
            if(task.accum[timestamp]===undefined){
                task.accum[timestamp]={}
            }
            if(task.accum[timestamp][road_class]===undefined){
                task.accum[timestamp][road_class]={}
            }
            task.accum[timestamp][road_class]['sum_lane_miles']
             = aadt_record['sum_lane_miles']
        });
        _.each(nhh_vars,function(k2){
            // multiply by nhh
            _.each(task.fractions,function(record,timestamp){
                task.accum[timestamp][road_class][k2] = record.nhh * scale.nhh * aadt_record[k2]
            });
        });
        _.each(n_vars,function(k2){
            // multiply by n
            _.each(task.fractions,function(record,timestamp){
                task.accum[timestamp][road_class][k2] = record.n * scale.n * aadt_record[k2]
            });
        });
        _.each(hh_vars,function(k2){
            // multiply by hh
            _.each(task.fractions,function(record,timestamp){
                task.accum[timestamp][road_class][k2] = record.hh * scale.hh * aadt_record[k2]
            });
        });
    });

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
 * returns a modified task, with duplicate roads from detectors
 * removed from the hpms sums, so that you don't double count
 *
 * this function is synchronous, but you can call it like async if you
 * want.  if cb is undefined, it will return the reduced memo output
 *
 */
function reduce(memo,item,cb){
    // combine item.accum into memo

    // not doing speed or speed limit or whatever at the moment
    // hpms only has design speed and speed limit
    _.each(item.accum,function(roads,ts){
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
            })
        }
    });
    _.each(item.detector_data,function(record,ts){
        // could also insert speed here into to the sum by
        // multiplying by n to weight it, as I do elsewhere
        var detector_miles = record[unmapper.miles]
        if(memo[ts]===undefined){
            memo[ts]={}
        }
        if(memo[ts]['detector_based']===undefined){
            memo[ts]['detector_based']={'n':record[unmapper.n]
                                       ,'n_mt':record[unmapper.n]*detector_miles
                                       ,'hh_mt':record[unmapper.hh]*detector_miles
                                       ,'nhh_mt':record[unmapper.not_hh]*detector_miles
                                       ,'lane_miles':record[unmapper.lane_miles]
                                       }
        }else{
            memo[ts]['detector_based'].n      += record[unmapper.n]
            memo[ts]['detector_based'].n_mt   += record[unmapper.n]*detector_miles
            memo[ts]['detector_based'].hh_mt  += record[unmapper.hh]*detector_miles
            memo[ts]['detector_based'].nhh_mt += record[unmapper.not_hh]*detector_miles
            memo[ts]['detector_based'].lane_miles += record[unmapper.lane_miles]
        }
    });
    if(cb !== undefined && typeof cb === 'function') cb(null,memo)
    return memo

}

exports.apply_fractions=apply_fractions
exports.reduce=reduce
