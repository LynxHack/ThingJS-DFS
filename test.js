//const DFS = require('./clientAPI.js');

//let dfs = new DFS();

//dfs.appendFile('testfile.txt', 'Hello World!', (err)=> {
//      console.log(err);
//      console.log('Finished');
//});

//dfs.readFile('testfile.txt', (err, data)=> {
//      console.log(err, data);
//});
//
var ThingsNode = require('./ThingsNode');

var node = new ThingsNode('mqtt://192.168.50.101', '2', 'storage', 'replicas');
