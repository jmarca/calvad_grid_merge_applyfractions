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
var flatten = require('./lib/flatten')
var flatten_records = flatten.flatten_records
var cdb_interactions = require('calvad_grid_merge_couchdbquery')
var put_results_doc = cdb_interactions.put_results_doc
var check_results_doc = cdb_interactions.check_results_doc
if(check_results_doc===undefined){
    croak;
}
var areatypes=['county','airbasin','airdistrict']

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
           .options("recheck", {'describe': "redo all of the area names, not just the ones that aren't stored yet"
                                ,'type': "boolean"
                                ,'default': false
                               })
           .options("hpms",{'alias':'hpmsfile'
                        ,'describe':'previously formatted hpmsYEAR.json files.   Specify multiple files as --hpmsfiles hpms2007.json --hpmsfiles ../some/directory/hpms2008.json and so on.'
                        ,'default':hpmsfiles})
           .options("area",{'alias':'areatype'
                        ,'describe':'which area types to process.  defaults to [airbasin,county,airdistrict].  Specify multiple area types as --areatype county --areatype airbasin.  The values should not by anything except values specified in calvad_areas::cellmembership'
                        ,'default':areatypes})
           .argv
;
if (argv.help){
    optimist.showHelp();
    return null
}


var recheck=argv.recheck

var options = {statedb:argv.statedb}
var years = _.flatten([argv.year])
hpmsfiles = _.flatten([argv.hpmsfile])
areatypes = _.flatten([argv.areatype])

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

    var gridq = queue(1)

    var memo = {}

    tasks.forEach(function(t){
        gridq.defer(function(cb){
            handler(t,function(e,t2){
                console.log('handled '+t2.cell_id)
                reduce.reduce(memo,t2,cb)
                return null
            });
            return null
        })
    })

    gridq.awaitAll(function(e,results){

        finish_task.result = memo
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

function process_area_year(config,area_type,yr,cb){
    // work on one thing at a time here. do multiple jobs inside loop

    console.log(area_type,yr)
    var groups = {}
    var tasks=
        _.map(grid_records,function(membership,cell_id){
            var t = {'cell_id':cell_id
                   ,'year':yr
                   ,'options':config
                   ,'area_type':area_type
                   ,'area_name':membership[area_type]
                   }
            groups[flatten.make_id(t)]=t.area_name
            return t
        });
    var grouped_tasks = _.groupBy(tasks,function(t){
                            return t.area_name
                        })

    console.log({'all areas':Object.keys(grouped_tasks)})

    // how to skip tasks:
    var qgroups = queue()

    if(recheck){
       qgroups.defer(function(cb){ return cb() })
    }else{
        _.forEach(groups,function(area_name,docid){
            // check if the doc is aready in the db
            var _c = {options:config}
            _c.doc={'id':docid}
            qgroups.defer(function(cb){
                cdb_interactions.check_results_doc(_c,function(e,r){
                    if(r){
                        // truthy r means we're done with this one
                        console.log('skipping ',docid)
                        delete grouped_tasks[area_name]
                    }
                    return cb()
                })
                return null
            })
            return null
        });
    }
    qgroups.await(function(e,res){
        console.log({'going to process ':Object.keys(grouped_tasks)})
        var q = queue(1)
        _.each(grouped_tasks,function(tasks,group){
            console.log({'tasks.length':tasks.length})
            q.defer(reducing_code,tasks)
            return null
        });
        q.await(function(e){
            console.log('done with work for ',area_type,yr)
            return cb(e)
        })
        return null
    })
}

var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/config.json'
config_okay(config_file,function(err,c){
    var q = queue(1)
    if(err){throw new Error(err)}
    q.defer(prepwork)
    years.forEach(function(yr){
        areatypes.forEach(function(areatype){
            console.log(areatype)
            q.defer(process_area_year,c,areatype,yr)
            return null
        })
        return null
    })
    return null
})

1;
