


var clarinet = require("clarinet")
var stringify = require("csv-stringify")


function make_csvstreamer(outputstream){
    var headerlen = 0
    var stringifier = stringify();
    stringifier.pipe(outputstream)

    var is_header = false
    var is_data = false
    var is_gridcells = false

    var array_depth = 0

    var error = false
    var parser = clarinet.createStream()
    var lines = 0

    parser.on('error', function (e) {
        // an error happened. e is the error.
        error = true
    });

    var collector;
    parser.on('key', function (key) {
        // got some value.  v is the value. can be string, double, bool, or null.
        // console.log('got ', key)
        if(key === 'header'){
            is_header = true
        }
        if(key==='data'){
            is_data = true
        }
        if(key === 'grid_cells'){
            is_gridcells = true
        }
        return null
    });


    parser.on('value',function(value){
        if(is_header || is_data || is_gridcells){
            collector.push(value)
        }
    });

    parser.on('openarray',function(value){
        array_depth++
        if(is_header || is_data){
            collector = []
        }
        if(is_gridcells){
            for(var i = 0;i<headerlen;i++){
                collector.push('') // empty items for the header values, because at this point, header does not contain gridcells
            }
        }
    });

    parser.on('closearray',function(value){
        array_depth--
        if(is_header){
            headerlen = collector.length
            collector.push('grid cells') // add an extra column heading for gridcells
        }
        if( collector.length > 0 ){
            stringifier.write(collector)
            collector = []
        }
        if(array_depth === 0){
            is_header = false
            is_data = false
            is_gridcells = false
        }

    });

    parser.on('end',function(){
        //console.log('parser end')
        stringifier.end()
    })


    return parser
}

module.exports = make_csvstreamer
