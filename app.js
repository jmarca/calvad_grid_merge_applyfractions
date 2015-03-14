/*global require process */
/**
 * calvad_merge_grids/app.js
 *
 * This program merges detector and hpms output in CalVAD
 *
 * Set the number of simultaneous R jobs by the option --num_jobs or
 * the environment variables NUM_R_JOBS
 *
 */

var util  = require('util')
var spawn = require('child_process').spawn
var path = require('path')
var fs = require('fs')
var queue = require('queue-async')
var _ = require('lodash')

var config_okay = require('config_okay')


var reduce = require('./lib/reduce')
var routes = require('./lib/routes.js')
var flatten_records = require('./lib/flatten').flatten_records
var cdb_interactions = require('calvad_grid_merge_couchdbquery')
var put_results_doc = cdb_interactions.put_results_doc

// make this a command line thing

var env = process.env

var rootdir = path.normalize(__dirname)
var hpmsfiles = [rootdir+'/public/hpms2007.json',
                 rootdir+'/public/hpms2008.json',
                 rootdir+'/public/hpms2009.json'
                ]

var optimist = require('optimist')
var argv = optimist
           .usage('merge the HPMS AADT values with hourly detector imputations/measurements using the hourly grid aadt fraction computed by the code in grid_data.\nUsage: $0')
           .options('j',{'default':1
                        ,'alias': 'jobs'
                        ,'describe':'How many simultaneous jobs to run.  try one, watch your RAM.  Default is one'
                        })
           .options('y',{'demand':true
                        ,'alias':'year'
                        ,'default':[2007,2008,2009]
                        ,describe:'One or more years to process.  Specify multiple years as --year 2007 --year 2008.  If you say nothing, 2007. 2008. and 2009 will be run'
                        })
           .options("h", {'alias':'help'
                         ,'describe': "display this hopefully helpful message"
                         ,'type': "boolean"
                         ,'default': false
                         })
           .options("hpms",{'alias':'hpmsfiles'
                        ,'describe':'previously formatted hpmsYEAR.json files.   Specify multiple files as --hpmsfiles hpms2007.json --hpmsfiles ../some/directory/hpms2008.json and so on.'
                        ,'default':hpmsfiles})
           .argv
;
if (argv.help){
    optimist.showHelp();
    return null
}




var options = {statedb:argv.statedb}
var years = _.flatten([argv.year])
hpmsfiles = _.flatten([argv.hpmsfiles])

var jobs = argv.jobs


var grid_records= require('calvad_areas').grid_records


var hpmsgrids = {}
function prepwork(cb){
    // preload the hpms files
    var q = queue()
    hpmsfiles.forEach(function(f){q.defer(fs.readFile,f,{'encoding':'utf8'})})
    q.awaitAll(function(e,data){
        // data is an array of file contents, one per file
        hpmsfiles.forEach(function(f,idx){
            var year = (/(\d\d\d\d)/.exec(f))[1]
            if(!year){ throw new Error('problem with file: '+f+'.  Must be in form "hpms2008.json"')}
            // rejigger the json data for faster lookups

            hpmsgrids[year] = routes.process_hpms_data(JSON.parse(data[idx]))

            return cb(null,hpmsgrids)
        })
        return null
    })
    return null
}

function reducing_code(tasks,reducing_callback){
    var options = _.clone(tasks[0].options)
    var area_type = tasks[0].area_type
    var area_name = tasks[0].area_name
    var year = tasks[0].year
    console.log(tasks[0])
    var grid_cells = _.pluck(tasks,'cell_id')

    if(hpmsgrids[year]===undefined){
        throw new Error('hpmsgrids not defined for ',year)
    }

    var handler = routes.fractions_handler(hpmsgrids[year])

    var finish_task = {'area_type':area_type
                      ,'area_name':area_name
                      ,'year':year
                      }
    console.log({'processing':finish_task
                ,'tasks.length':tasks.length})

    finish_task.grid_cells= grid_cells

    var outerq = queue(jobs)

    tasks.forEach(function(t){
        outerq.defer(handler,t)
    })
    outerq.awaitAll(function(e,results){

        // reduce the results
        finish_task.result =  _.reduce(results
                                      ,reduce.reduce
                                      ,{}
                                      );
        flatten_records(finish_task,function(e,t){
            console.log('going to save')
            put_results_doc({options:options
                            ,doc:t}
                           ,function(e,r){
                                console.log('done with put')
                                return reducing_callback(e)
                            })
            return null
        })

        return null
    })
    return null
}

function process_area_year(config,area_type,years,cb){
    // work on one thing at a time here. do multiple jobs inside loop

    var q = queue(1)

    years.forEach(function(yr){

        var tasks=
            _.map(grid_records,function(membership,cell_id){
                return {'cell_id':cell_id
                       ,'year':yr
                       ,'options':config
                       ,'area_type':area_type
                       ,'area_name':membership[area_type]
                       }
            });
        // how to skip tasks:
        // tasks = _.filter(tasks,function(t){
        //             if( t.area_name == 'ALAMEDA' ||
        //                 t.area_name == 'MENDOCINO'){
        //                 return false
        //             }
        //             return true
        //         })

        var grouped_tasks = _.groupBy(tasks,function(t){
                                return t.area_name
                            })

        _.each(grouped_tasks,function(tasks,group){
            console.log({'tasks.length':tasks.length})
            q.defer(reducing_code,tasks)
            return null
        });
        q.await(function(e){
            console.log('done with work')
            return cb(e)
        })
        return null
    });
    return null

}

config_okay('config.json',function(err,c){
    var q = queue(1)
    q.defer(prepwork)
    q.defer(function(cb){
        process_area_year(c,'county',years,cb)
    })
    return null
})

1;
