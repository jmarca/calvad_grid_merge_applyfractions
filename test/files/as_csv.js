/*global console require __dirname */
var env = process.env
var fs = require('fs')
var csv = require('csv')
var _ = require('lodash')
var async = require('async')

/** one off program to convert couchdb reduce output to csv */

/** output format:
 *
 * "rows":[
 *           {"id":"100_223_2008-01-01 00:00",
 *            "key":"100_223_2008-01-01  00:00",
 *            "value":{"rev":"1-27fa901830976a0c2f0a0c9eb91dd287"},
 *            "doc":{
 *                "_id":"100_223_2008-01-01 00:00",
 *                "_rev":"1-27fa901830976a0c2f0a0c9eb91dd287",
 *                "i_cell":100,
 *                "j_cell":223,
 *                "aadt_frac":{
 *                    "n":0.018945000000000000034,
 *                    "hh":0.024503000000000000475,
 *                    "nhh":0.022984000000000000957
 *                },
 *                "ts":"2008-01-01 00:00",
 *                "geom_id":"100_223"
 *            }
 *          }
 *
**/
var csv_header = ['ts','geom_id','aadt_frac_n','aadt_frac_hh','aadt_frac_nhh']

function process(options,cb){
    var data = JSON.parse(options.filetext)
    var writestream = options.writestream
    function write_record(record,i){
        var doc = record.doc

        var dump = [doc.ts,doc.geom_id,doc.aadt_frac.n,doc.aadt_frac.hh,doc.aadt_frac.nhh]
        writestream.write(dump)

    }
    writestream.write(csv_header)
    _.each(data.rows,write_record)
    return cb(null,options)
}

function read_file(options,cb){
    fs.readFile(options.filename,'utf8',function(err,data){
        if(err){
            console.log(err)
            return cb(err)
        }
        options.filetext = data
        return cb(null,options)
    })
}

function writer(options,cb){
    var csv_writer = csv()
    csv_writer.pipe(options.output)
    options.writestream=csv_writer
    return cb(null,options)
}

function open_file_pipe(options,cb){
    options.output = fs.createWriteStream(__dirname+'/'+options.out_filename)
    return cb(null,options)
}


async.waterfall([function(cb){
                     var opts = {'filename':'100_223_2008_JAN.json'
                                ,'out_filename':'100_223_2008_JAN.csv'
                                }
                     return cb(null,opts)
                 }
                ,open_file_pipe
                ,writer
                ,read_file
                ,process]
               ,function(e,data){
                    if(e) console.log('died')
                    console.log('done')
                })
