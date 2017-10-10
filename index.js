var data = require('./lib/data');

exports.handler = function(event, context, callback) {
    data.getData()
    .catch(function(err){
        console.log('err');
        return callback(err);
    })
    .done(function(){
        return callback(null, 'Success');
    });
}