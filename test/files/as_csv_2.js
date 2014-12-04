/*global console require __dirname */
var env = process.env
var fs = require('fs')
var csv = require('csv')
var _ = require('lodash')
var async = require('async')

/** one off program to convert couchdb reduce output to csv */


/** output format:
 *
 * {"rows":[
    {"key":["county_ALAMEDA_2007","detector based"]
    ,"value":{"sum":159681709280.36700439
             ,"count":365
             ,"min":95387288.181300014257
             ,"max":828443891.29530012608
             ,"sumsqr":75535532981144436736.0}},
    {"key":["county_ALAMEDA_2007","hpms based"]
    ,"value":{"sum":4942259078.8777313232
             ,"count":365
             ,"min":6935373.7235298110172
             ,"max":17003588.853768892586
             ,"sumsqr":68336882173927232.0}},
    {"key":["county_ALAMEDA_2008","detector based"]
    ,"value":{"sum":206498038832.03256226
             ,"count":366
             ,"min":299789291.99110001326
             ,"max":776954529.56499993801
             ,"sumsqr":1.1921140538726124749e+20}}
     ]}
 *
**/
var csv_header = ['county','year','type','vmt_sum','count','min','max','sumsqr']

function process(options,cb){
    var data = JSON.parse(options.filetext)
    var writestream = options.writestream
    var regex_key = /_(.*)_(\d{4})/
    function write_record(record,i){
        var match = regex_key.exec(record.key[0])
        var value = record.value
        var dump = [match[1],match[2],record.key[1]
                   ,value.sum,value.count,value.min,value.max,value.sumsqr]
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
                     var opts = {'filename':'county_reduce_daily_by_type.json'
                                ,'out_filename':'county_reduce_daily_by_type.csv'
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
