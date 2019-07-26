const DFS = require('./clientAPI.js');

let dfs = new DFS('mqtt://192.168.50.101');

for (var i=0; i < 10; i++){
        dfs.appendFile('testfile', 'Hello World!', (err)=> {
//              console.log(err);
//              console.log('Finished');
                if (err) throw err;
                        dfs.readFile('testfile', (err, data)=> {
//                      console.log(err);
                        console.log('Data: ', data);
                });
        });
}

//var ThingsNode = require('./ThingsNode');

//var node = new ThingsNode('mqtt://192.168.50.101', 'xkcd1', 'storage', 'replicas');
