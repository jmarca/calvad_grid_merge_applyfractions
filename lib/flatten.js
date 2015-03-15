var _ = require('lodash')

// the columns defines which columns get which bit of data
var columns = {'ts':0
              ,'roadway':1
               // hpms columns
              ,'sum_vmt':2
              ,'sum_single_unit_mt':3
              ,'sum_combination_mt':5
              ,'sum_lane_miles':7

               // detector-based columns
              ,'n_mt':2 // same as above
              ,'nhh_mt':4
              ,'hh_mt':6
              ,'lane_miles':7 // same as above
              }


function make_id(task){
    return [task.area_type,task.area_name,task.year].join('_')
}

/**
 * flatten out the sums into a giant data matrix, and save it
 *
 */

function flatten_records(task,cb){
    // make something like
    // area_type: county/basin/district
    // area_name: ..
    // year: ..
    // header: variable names
    // data:  data rows
    // grid_cells: [grid cells used]


    // fixme all of that should be refactored from arraifier too
    // and made into its own module

    task.data=[]
    var header = []
    header[columns.ts] = 'timestamp'
    header[columns.roadway] = 'roadway'
    header[columns.sum_vmt] = 'sum vehicle miles traveled'
    header[columns.sum_single_unit_mt] = 'sum single unit miles traveled'
    header[columns.sum_combination_mt] = 'sum combination miles traveled'
    header[columns.nhh_mt] = 'sum not heavy heavy-duty miles traveled'
    header[columns.hh_mt] = 'sum heavy heavy-duty miles traveled'
    header[columns.sum_lane_miles] = 'sum lane miles'
    task.header=header
    // assign an id here
    task._id = make_id(task)

    // console.log(task.result)

    _.each(task.result,function(roads,ts){
        _.each(roads,function(row,road_class){
            // fill in the data array
            var arr = []
            arr[columns.ts] = ts
            arr[columns.roadway]=road_class

                if(row === undefined){
                    console.log(JSON.stringify(roads))
                    console.log(ts,road_class)
                    throw new Error('die')
                }

            // either hpms or detector record the existence of the
            // column name in the columns hash map dictates what
            // column to plop the data
            _.forEach(columns,function(v,k){
                if(row[k] !== undefined) arr[v]=row[k]
                return null
            });

            task.data.push(arr)

            return null
        });
        return null
    });

    // get rid of result in task.
    delete task.result

    return cb(null,task)
}

exports.flatten_records=flatten_records
exports.make_id=make_id
