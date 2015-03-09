var should=require('should')

var make_csvstream = require('../lib/make_csvstream.js')

var fs = require('fs')
var filenum = 0

describe('make_csvstream',function(){

    var file = 'other_json_output'+filenum+'.csv'
    filenum++

    it('should dump json as csv',function(done){
        var inst = fs.createReadStream('./test/files/oldway.json',
                                       {'encoding':'utf8'})

        var outst = fs.createWriteStream(file,
                                         {'encoding':'utf8'})

        var parser = make_csvstream(outst)


        outst.on('finish',function(){
            outst.close()
            fs.readFile(file,{encoding:'utf8'},function(err,data){
                data.trim()
                var l = data.split(/\r?\n/);
                var len = l.length
                len.should.eql(87433)
                l.shift().should.eql("ts,road_class,vmt,lane_miles,single_unit_mt,combination_mt,grid cells")
                return done()
            })

        })

        inst.pipe(parser)

    })

});
