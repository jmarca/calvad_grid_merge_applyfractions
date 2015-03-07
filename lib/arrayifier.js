var _ = require('lodash')

function json2arrays(data){
    var col = 0;
    var header = {}
    var columns = {}
    columns.ts = col++
    columns.roadway = col++
    columns.sum_vmt = col++
    columns.n_mt  = columns.sum_vmt
    columns.sum_single_unit_mt = col++
    columns.nhh_mt = col++
    columns.sum_combination_mt = col++
    columns.hh_mt = col++
    columns.sum_lane_miles = col++
    columns.lane_miles = columns.sum_lane_miles

    // skip this one...it is confusing
    // columns.n

    header = [] // the column titles
    header[columns.ts] = 'timestamp'
    header[columns.roadway] = 'roadway'
    header[columns.sum_vmt] = 'sum vehicle miles traveled'
    header[columns.sum_single_unit_mt] = 'sum single unit miles traveled'
    header[columns.sum_combination_mt] = 'sum combination miles traveled'
    header[columns.nhh_mt] = 'sum not heavy heavy-duty miles traveled'
    header[columns.hh_mt] = 'sum heavy heavy-duty miles traveled'
    header[columns.sum_lane_miles] = 'sum lane miles'

    var tss = Object.keys(data)
    var A = []
    A.push(header)

    tss.forEach(function(ts){
        //var time = parseDate(ts)
        var tshash = data[ts]
        // has all the different roadway types.
        // split out
        var roadtypes = Object.keys(tshash)
        roadtypes.forEach(function(roadway){
            // filter this out now
            if(/total/.test(roadway)) return null

            var row = tshash[roadway]
            var arr = []
            arr[columns.ts]=ts
            arr[columns.roadway]=roadway
            _.forEach(columns,function(v,k){
                if(row[k] !== undefined) arr[v]=row[k]
                return null
            });
            A.push(arr)
            return null
        });
        return null
    })
    return A
}

module.exports = json2arrays
