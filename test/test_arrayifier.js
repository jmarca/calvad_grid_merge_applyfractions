var should=require('should')

var arrayifier = require('../lib/arrayifier.js')
var fs = require('fs')
describe('arrayify',function(){

    it('should arrayify reduced json',function(done){
        fs.readFile('./test/files/hpms_output.json',
                    {'encoding':'utf8'},
                    function(err,data){
                        var big_json = JSON.parse(data)
                        var arr = arrayifier(big_json)
                        for (var i = 1; i<6; i++){
                            arr[i][0].should.eql("2008-01-01 00:00")
                        }
                        for (var i = 6; i<11; i++){
                            arr[i][0].should.eql("2008-01-01 01:00")
                        }
                        arr.should.have.length(43916)
                        return done()
                    })
    })

});
