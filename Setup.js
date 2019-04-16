var storage = require('node-persist');

const display_debug_messages = true;

storage.init({dir: '\storage2'}).then( async function(val)
{
    await storage.clear();
    console.log('storage initialized successfully');

    storage.setItem('file1',{location: 'C:/Users/effat/ThingsJS/node2FileDir/file1.txt', primaryNode: true, concurrentNodeID : '3' })
    storage.setItem('file2',{location: 'C:/Users/effat/ThingsJS/node2FileDir/file2.txt', primaryNode: false, concurrentNodeID : '3' })
    
}).then(function(){
    storage.init({dir: '\storage3'}).then( async function(val)
{
    //await storage.clear();
    console.log('storage initialized successfully');

    storage.setItem('file1',{location: 'C:/Users/effat/ThingsJS/node3FileDir/file1.txt', primaryNode: false, concurrentNodeID : '2' })
    storage.setItem('file2',{location: 'C:/Users/effat/ThingsJS/node3FileDir/file2.txt', primaryNode: true, concurrentNodeID : '2' })
    storage.setItem('file3',{location: 'C:/Users/effat/ThingsJS/node3FileDir/file3.txt', primaryNode: false, concurrentNodeID : '4' })
}).then(function(){
    storage.init({dir: '\storage4'}).then( async function(val)
    {
        await storage.clear();
        console.log('storage initialized successfully');
    
        storage.setItem('file3',{location: 'C:/Users/effat/ThingsJS/node4FileDir/file3.txt', primaryNode: true, concurrentNodeID : '3' })
        
    });
})
})

function debug(...args)
{
    if (display_debug_messages)
    {
        var length = args.length;
        for (var i=0; i< length; i++)
            console.log(args[i]);
    }  
}
console.log('setup script')
