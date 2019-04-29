var storage = require('node-persist');
var things = require('things-js');
var fs = require('fs');
const uuidv1 = require('uuid/v1');

var display_debug_messages = true;
debug('node\n');

function ThingsNode(pubsubURL, nodeID, storageDir, fileDir){

    if (storageDir == null) {storageDir = '/storage' + nodeID}
    if (fileDir == null) {fileDir = '/fileDir' + nodeID}

    this.fileMeta = new Map();
    this.id = nodeID;
    this.persistDir = storageDir;
    this.fileDir = fileDir + "/";
    this.pubsubURL = pubsubURL; //'mqtt://localhost'
    var self = this;
    this.metadata = [];
    this.cachedFiles = new Cache();

    return new Promise(function(resolve, reject){
        storage.init({dir: self.persistDir}).then( async function(val)
        {
            debug('storage initialized successfully');

            var size = await storage.length();
            var ctr = 0;
            storage.forEach(function(pair){
                //key is file virtual path (name)
                self.fileMeta.set(pair.key, self.parseMetadata(pair));
                ++ctr;
                if (ctr == size)
                {
                    resolve(true);
                }
            });
            
        });
    }).then(function(val){
                self.pubsub = new things.Pubsub(self.pubsubURL);
                var myNodeTopic = "'" + self.id + "'";
                debug('subscribing to self id: ', myNodeTopic);
                self.pubsub.subscribe(myNodeTopic, async function(request){
                    debug('handling request: ', request);
                    if (request.sender == 'master')
                    {
                        var myFileMeta = self.fileMeta.get(request.file);

                        if(myFileMeta != undefined) //we already have that file, just updating metadata
                        {
                            var nodePersistRecord = await storage.getItem(request.file);
                            if(myFileMeta.isPrimaryNode)
                            {
                                // We have been the primary node for that file, just update concurrent node
                                myFileMeta.concurrentNode = request.metadata.secondaryNode;
                            }
                            else
                            {
                                // we have been a secondary node for this file already, just update pri node
                                myFileMeta.concurrentNode = request.metadata.primaryNode;
                            }
                            nodePersistRecord.concurrentNodeID = myFileMeta.concurrentNodeID;
                            await storage.setItem(request.file, nodePersistRecord);
                        }
                        else
                        {
                            // We did not have that file, create metadata and copy file if required
                            var isPrimaryNode, concurrentNode, location; 
                            location = self.fileDir + request.file + ".txt";
                            if (self.id == request.metadata.primaryNode)
                            {
                                isPrimaryNode = true;
                                concurrentNode = request.metadata.secondaryNode;
                            }
                            else
                            {
                                isPrimaryNode = false;
                                concurrentNode = request.metadata.primaryNode;
                            }
                            // request to copy file only if this is a replication process.
                            if (request.type == 'replicate')
                            {
                                var copyRequest = 
                                {sender: 'dataNode', type: 'copyRequest', file: request.file, id: self.id};
                                var contactNodeTopic = "'" + concurrentNode + "'";
                                self.pubsub.publish(contactNodeTopic, copyRequest);
                            }

                            var myFileMeta = 
                                new FileMetadata(self.pubsub, self.id, location, isPrimaryNode, concurrentNode);
                            await storage.setItem(
                                request.file,
                                {location: location, 
                                primaryNode: isPrimaryNode, 
                                concurrentNodeID : concurrentNode });
                            if (request.type == 'newFile')
                            {
                                // Acknowledge to master that metadata has been created.
                                self.pubsub.publish(request.id, 'metadataCreated');
                            }
                        }
                        self.fileMeta.set(request.file, myFileMeta);
                        debug("updated metadata: ", myFileMeta);
                    }
                    else if (request.sender == 'clientAPI')
                    {
                        if ( (fileMetadata = self.fileMeta.get(request.file)) != undefined )
                        {
                            if (request.type == 'read')
                            {
                                var cachedReplica = self.cachedFiles.getCachedFile(request.file);
                                if (cachedReplica != null)
                                {
                                    var myResponse = {sender: 'dataNode', err: null, data: cachedReplica};
                                    self.pubsub.publish(request.id, myResponse);
                                    
                                }
                                else{
                                    fs.readFile(fileMetadata.location, function(err, data){
                                        // verify file integrity by recalculating checksum.
                                        if (calculateDataChecksum(data) == fileMetadata.checksum)
                                        {
                                            // Data integrity verified.
                                            var myResponse = {sender: 'dataNode', err: err, data: data.toString()};
                                            self.pubsub.publish(request.id, myResponse);
                                            self.cachedFiles.cacheFile(request.file, data.toString());
                                        }
                                        else
                                        {
                                            debug('Local file is corrupted, copying a valid replica');
                                            
                                            // Our copy is corrupted, contact concurrent node to get a correct copy.
                                            var copyRequest = 
                                                {sender: 'dataNode', type: 'copyRequest', file: request.file, id: self.id};
                                            debug('copy request: ', copyRequest);
                                            var contactNodeTopic = "'" + fileMetadata.concurrentNode + "'";
                                            self.pubsub.publish(contactNodeTopic, copyRequest);
    
                                            // Ask client to try the concurrent Node instead.
                                            var myResponse = 
                                                {sender: 'dataNode', status: 'alternativeNode', altNodeID: fileMetadata.concurrentNode};
                                                self.pubsub.publish(request.id, myResponse);
                                        }
                                        
                                    })
                                }

                            }
                            else if ( (request.type == 'append') || (request.type == 'write'))
                            {
                                self.cachedFiles.invalidateCache(request.file);
                                fileMetadata.addRequestTOQueue(request);
                            }
                        } 
                    }
                    else if (request.sender == 'dataNode')
                    {
                        if (request.type == 'copyRequest')
                        {
                            debug('dataNode copy request received ', request);
                            var requestedFileMeta = self.fileMeta.get(request.file);
                            var requestedFileLocation = requestedFileMeta.location;
                            fs.readFile(requestedFileLocation, function(err, data){
                                var copyResponse = 
                                {sender: 'dataNode', type: 'copyResponse', err:err, data: data, file: request.file};
                                var resNodeTopic = "'" + request.id + "'"; 
                                self.pubsub.publish(resNodeTopic, copyResponse);
                            })
                        }
                        else if (request.type == 'copyResponse')
                        {
                            debug('dataNode copyResponse received ', request);
                            var tempFileMeta = self.fileMeta.get(request.file);
                            var tempLocation = tempFileMeta.location;
                            
                            fs.writeFile(tempLocation, 
                                String.fromCharCode.apply(String, request.data.data).replace(/\0/g,''), 
                                function(err){
                                    if (err) console.log("ERROR on copyResponse: ", err);
                                    tempFileMeta.updateChecksum('write', request.data.data);
                            })
                            
                        
                        }
                        else if (request.type == 'replicateAppend')
                        {
                            self.cachedFiles.invalidateCache(request.file);
                            var tempFileMeta = self.fileMeta.get(request.file);
                            fs.appendFile(tempFileMeta.location, request.data, function(err){
                                if (err)
                                {
                                    console.log('ERROR on replicate Append');
                                } 
                                else 
                                {
                                    tempFileMeta.updateChecksum('append', request.data);
                                }
                                self.pubsub.publish(request.id, err);

                            })
                        }

                        else if (request.type == 'replicateWrite')
                        {
                            self.cachedFiles.invalidateCache(request.file);
                            var tempFileMeta = self.fileMeta.get(request.file);
                            fs.writeFile(tempFileMeta.location, request.data, function(err){
                                if (err)
                                {
                                    console.log('ERROR on replicate Append');
                                } 
                                else 
                                {
                                    tempFileMeta.updateChecksum('write', request.data);
                                }
                                self.pubsub.publish(request.id, err);

                            })

                        }
                    }
                }).then(function(topic){
                    debug('subscribed to topic: ', topic);
                })

                var myMetadata = {id: self.id, files: Array.from(self.fileMeta.keys()), metadata: Array.from(self.fileMeta.values())};
                self.pubsub.publish('nodeHandshake', myMetadata).then(function(){
                    //heartbeat Interval
                    setInterval(function(){
                        self.pubsub.publish('heartbeat', {id: self.id});
                    }, 3000)
                })    
            
            });
};
ThingsNode.prototype.constructor = ThingsNode;

ThingsNode.prototype.parseMetadata = function(pair){
    var location = pair.value.location;
    var isPrimaryNode = pair.value.primaryNode;
    var concurrentNodeID = pair.value.concurrentNodeID;

    var newMetadata = new FileMetadata(this.pubsub, this.id, location, isPrimaryNode, concurrentNodeID);
    newMetadata.calculateInitialChecksum();
    return newMetadata;
}

function FileMetadata(pubsub, id, location, isPrimaryNode=true, concurrentNode=null){
    this.id = id;  //device ID, also to be used as pubsub topic to communicate with that device
    this.isPrimaryNode = isPrimaryNode;  //by default the object is for a primary node record
    this.concurrentNode = concurrentNode;
    this.location = location;
    this.checksum = 0;
    this.pubsub = pubsub;
    this.queue = [];
}

FileMetadata.prototype.calculateInitialChecksum = function()
{
    self = this;
    //Read synchronusly here to avoid serving requests before checksum is calculated.
    var data = fs.readFileSync(self.location);

    for (let i = 0; i < data.length; i++)
    {
        self.checksum = self.checksum ^ data[i];
    }
    debug('calculated check sum for ', self.location, ' is ', self.checksum);
    return self.checksum;
    
}

FileMetadata.prototype.updateChecksum = function (type, data)
{
    var self = this;

    if (type == 'write')
    {
        // if it is a write operation, the file is beign replaced so checksum value is set to 0.
        self.checksum = 0;
    }

    for (let i = 0; i < data.length; i++)
    {
        self.checksum = self.checksum ^ data[i];
    }

    debug('updated checksum to ', self.checksum);
}
FileMetadata.prototype.addRequestTOQueue = function(request)
{
    this.queue.push(request);
    if (this.queue.length == 1)
    {
        this.next();
    }
    //else there is currently an operation being excuted,next will be called when its done
}

FileMetadata.prototype.next = function()
{
    var self = this;
    tempRequest = self.queue[0];
    switch(tempRequest.type)
    {
        case 'append':
        {
            fs.appendFile(self.location, tempRequest.data, function(err)
            {
                if (err)
                {
                    console.log("ERROR writing to file: ", self.location);
                    return;
                }
                self.updateChecksum('append', tempRequest.data);
                // Forward the request to secondary node, wait for acknowledgment
                var requestID = uuidv1();
                self.pubsub.subscribe(requestID, function(response){
                    var myResponse = {sender: 'dataNode', err: err};
                    self.pubsub.publish(tempRequest.id, myResponse);
                    self.pubsub.unsubscribe(requestID);
                })
                    .then(function(topic){
                        if (topic != requestID)
                        {
                            console.log(
                                'ERROR subscribing to secondary node request topic, cant receive acknowledgment');
                        }
        
                        var replicateAppendRequest = {
                            id: requestID, 
                            file: tempRequest.file, 
                            data: tempRequest.data, 
                            sender: 'dataNode', 
                            type: 'replicateAppend'};
        
                        var concurrentNodeTopic = "'" + self.concurrentNode + "'";
                        self.pubsub.publish(concurrentNodeTopic, replicateAppendRequest)
                    })
                self.queue.shift();
                if(self.queue.length > 0)
                {
                    self.next();
                }
            })
            break;
        }
        case 'write':
        {
            fs.writeFile(self.location, tempRequest.data, function(err)
            {
                if (err)
                {
                    console.log("ERROR writing to file: ", self.location);
                    return;
                }
                self.updateChecksum('write', tempRequest.data);
                // Forward the request to secondary node, wait for acknowledgment
                var requestID = uuidv1();
                self.pubsub.subscribe(requestID, function(response){
                    var myResponse = {sender: 'dataNode', err: err};
                    self.pubsub.publish(tempRequest.id, myResponse);
                    self.pubsub.unsubscribe(requestID);
                })
                    .then(function(topic){
                        if (topic != requestID)
                        {
                            console.log(
                                'ERROR subscribing to secondary node request topic, cant receive acknowledgment');
                        }
        
                        var replicateWriteRequest = {
                            id: requestID, 
                            file: tempRequest.file, 
                            data: tempRequest.data, 
                            sender: 'dataNode', 
                            type: 'replicateWrite'};
        
                        var concurrentNodeTopic = "'" + self.concurrentNode + "'";
                        self.pubsub.publish(concurrentNodeTopic, replicateWriteRequest)
                    })
                self.queue.shift();
                if(self.queue.length > 0)
                {
                    self.next();
                }
            })
            break;
        }
    }

}

function calculateDataChecksum(data)
{
    var checksum = 0;
    for (let i = 0; i < data.length; i++)
    {
        checksum = checksum ^ data[i];
    }
    return checksum;
}

function debug(...args)
{
    if (display_debug_messages)
    {
        var length = args.length;
        var message = '';
        for (var i=0; i< length; i++)
        {
            
            message += JSON.stringify(args[i]);
        }       
        
        console.log(message);
    }  
}

function Cache()
{
    this.files = new Map();
}

Cache.prototype.cacheFile = function(filename, content)
{

    if (this.files.size > 5)
    {
        // cache is full, eliminate a file stochastically 
        var randomIndex = Math.floor(Math.random()*5);
        var keysArray = Array.from(this.files.keys());
        this.files.delete(keysArray[randomIndex]);
        
    }

    this.files.set(filename, content);

}

Cache.prototype.getCachedFile = function(filename)
{
    var cachedFile = this.files.get(filename);

    if (cachedFile == undefined)
    {
        return null;
    }

    return cachedFile;
}

Cache.prototype.invalidateCache = function(filename)
{
    if (this.files.has(filename))
    {
        //console.log('invalidated cache!')
        this.files.delete(filename);
    }
}

module.exports = ThingsNode;
