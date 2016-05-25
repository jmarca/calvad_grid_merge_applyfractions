# apply fractions to AADT data

Extracted from grid_merge.

# How to generate summations of HPMS+Detector-based

Assuming all of the prerequisites are completed, you need to run the
app.js function as follows:

```
node app.js -y 2012 -j 6 --recheck --area county > county.2012.out 2>&1
```

In this case, the year is 2012, the concurrency of node processes is
set to 6 (not the same as parallel jobs, but similar), the recheck
parameter is set to true, and the type of area to process is set to
county.


Of course, in order for that to run properly, you must first define
some other options in a file called `config.json`

For example (with usernames and passwords obviously needing to be changed):

```
{
    "couchdb": {
        "host": "127.0.0.1",
        "port":5984,
        "auth":{"username":"couchusername",
                "password":"my secret couchdb password"
               },
        "detector_display_db": "vdsdata%2fskimmed",
        "county_detector_collation_db": "calvad%2fcounty%2fdetectors",
        "grid_merge_couchdbquery_detector_db": "carb%2Fgrid%2Fstate4k",
        "grid_merge_couchdbquery_hpms_db": "carb%2Fgrid%2Fstate4k%2Fhpms",
        "grid_merge_couchdbquery_statedb":"vdsdata%2ftracking",
        "grid_merge_couchdbquery_put_db": "calvad%2Fhpms",
        "grid_display_db": "calvad%2Fhpms",
        "design":"detectors",
        "view":"fips_year"
    },
    "postgresql":{
        "username":"postgresql user to use",
        "host":"127.0.0.1",
        "port":5432,
        "detector_display_db":"osm2",
        "grid_merge_sqlquery_db":"spatialvds"
    }

}
```
