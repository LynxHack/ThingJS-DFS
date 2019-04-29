var things = require('things-js');
const uuidv1 = require('uuid/v1');

const display_debug_messages = true;

//One object per file will be created by the Master node.
var MasterMetadata = function(fileName){
    this.primaryNode = null;
    this.secondaryNode = null;
}

MasterMetadata.prototype.setPrimaryNode = function(primaryNode){
    this.primaryNode = primaryNode;
}

MasterMetadata.prototype.setSecondaryNode = function(secondaryNode){
    this.secondaryNode = secondaryNode;
}

MasterMetadata.prototype.updateReplicaMetadata = function(candidateNodes, brokenNodeId){

    debug("updateReplicaMetadata ENTRY: ", candidateNodes, brokenNodeId);
    //The next lines exclude primary and secondary nodes from potential new nodes
    // (because it does not make sense to replicate a file twice on the same node)
    var primaryNodeIndex, secondaryNodeIndex;
    if ( (primaryNodeIndex = candidateNodes.indexOf(this.primaryNode)) > -1 )
    {
        candidateNodes.splice(primaryNodeIndex, 1);
    }

    if ( (secondaryNodeIndex = candidateNodes.indexOf(this.secondaryNode)) > -1 )
    {
        candidateNodes.splice(secondaryNodeIndex, 1);
    }
    debug('candidate nodes before election: ', candidateNodes);
    //Now randomly pick a node to take on the replica
    var newNode = candidateNodes[Math.floor(Math.random()*candidateNodes.length)];

    // Update the metadata for this node
    if (brokenNodeId == this.primaryNode) {this.primaryNode = newNode}
    else if (brokenNodeId == this.secondaryNode) {this.secondaryNode = newNode}
    debug('elected node is ', newNode);
    return newNode;
}

MasterMetadata.prototype.getAllNodes = function()
{
    var nodes = [];
    nodes.push(this.primaryNode, this.secondaryNode);
    return nodes;
}

function Node(id)
{
    this.nodeTopic = "'" + id + "'";
    this.files = [];
    this.available = true;
    this.timeStamp; //last time we heard a heartbeat from that node
}
Node.prototype.addFile = function(file)
{
    this.files.push(file);
}

Node.prototype.removeFile = function(file)
{
    this.files.splice(indexOf(file), 1);
}

function Master(pubsubURL){

    var self = this;
    this.pubsub = new things.Pubsub(pubsubURL);
    this.masterMetadataObjects = new Map();  //Maps file name/path to its master metadata
    this.dataNodes = new Map(); //Maps each data node ID to its Node metadata

    setInterval(function(){
        //debug('STARTING HEARTBEAT INTERVAL')
        var currentTime = (new Date()).getTime() / 1000;
    
        self.dataNodes.forEach(function(value, key) {
            if (value.available == true)
            {
                if( (currentTime - value.timeStamp) > 5)  //We have not heard from that node in 5 seconds
                {
                    value.available = false; // Mark as unavailable
                    self.dataNodes.set(key, value);
                    self.replicate(key, value.files);
                    debug('Node has been marked unavailable: ', key)
                }
            }
    
          });
    
    }, 5000);

    debug("Pubsub connected");
    self.pubsub.subscribe('nodeHandshake', function(message){self.handshakeClient(message)});
    self.pubsub.subscribe('metadata', function(metadataRequest){self.sendMetadata(metadataRequest)});
    self.pubsub.subscribe('heartbeat', function(message){self.processHeartbeat(message)});

    debug('Master node\n');
}

Master.prototype.handshakeClient = function(message){

    var self = this;
    debug('handshakeClientEntry');
    var newNode = new Node(message.id);

    //parse all files in that dataNode
    for (let index = 0; index< message.metadata.length; ++index)
    {
        var element = message.metadata[index];
        var file = message.files[index];
        newNode.addFile(file);
        newNode.timeStamp = (new Date()).getTime() / 1000;
        var metaEntry = self.masterMetadataObjects.get(file);

        if (metaEntry == undefined)
        {
            // First time master hears about that file/
            metaEntry = new MasterMetadata(file);
            if(element.isPrimaryNode) 
            {
                 metaEntry.setPrimaryNode(message.id)
                 metaEntry.setSecondaryNode(element.concurrentNode)
            }
            else 
            {
                metaEntry.setSecondaryNode(message.id)
                metaEntry.setPrimaryNode(element.concurrentNode)
            }
            self.masterMetadataObjects.set(file, metaEntry);
        }
        else
        {
            //cross validation
            if(element.isPrimaryNode) 
            {
                if(metaEntry.primaryNode != message.id){ debug('Warning: possible metada mismatch') }
                else if(metaEntry.secondaryNode != element.concurrentNode) { debug('Warning: possible metada mismatch')}
            }
            else
            {
                if(metaEntry.secondaryNode != message.id) { debug('Warning: possible metada mismatch') }
                else if(metaEntry.primaryNode != element.concurrentNode) { debug('Warning: possible metada mismatch') }
            }
            
        }
        
    }
    self.dataNodes.set(message.id, newNode);
    
    debug('ADDED NEW NODE', newNode);
}

Master.prototype.sendMetadata = function(metadataRequest)
{
    var self = this;
    var response = {};
    var metaEntry;

    response.sender = 'master';
    if (metadataRequest.type == 'read')
    {
        if ( (metaEntry = self.masterMetadataObjects.get(metadataRequest.file)) != undefined)
        {
            //debug("metaEntry: ", metaEntry);
            response.status = 'success';
            
            response.contactNodeID = metaEntry.secondaryNode;
            self.pubsub.publish(metadataRequest.id, response);
            //debug("response: ", response);
        }
    }
    else if ( (metadataRequest.type == 'append') || (metadataRequest.type == 'write'))
    {
        if ( (metaEntry = self.masterMetadataObjects.get(metadataRequest.file)) == undefined)
        {
            //New file will be created.
            createNewMetadata(metadataRequest.file)
                .then(function(metaEntry){
                    //debug('received both acknowledgments for metadata creation');
                    response.contactNodeID = metaEntry.primaryNode; //All appends/writes have to go through primary node
                    self.pubsub.publish(metadataRequest.id, response);
                })
        }
        else
        {
            response.contactNodeID = metaEntry.primaryNode; //All appends/writes have to go through primary node
            self.pubsub.publish(metadataRequest.id, response);
        }

    }
};

Master.prototype.createNewMetadata = function(newFile)
{
    var self = this;
    var primaryNode, secondaryNode;
    var potentialHosts = Array.from(self.dataNodes.keys());

    //select primary node
    primaryNode = potentialHosts[Math.floor(Math.random()*potentialHosts.length)];

    //remove the primary node from potential hosts before picking a secondary node
    potentialHosts.splice(potentialHosts.indexOf(primaryNode), 1);
    secondaryNode = potentialHosts[Math.floor(Math.random()*potentialHosts.length)];

    var newMetadata = new MasterMetadata(newFile);
    newMetadata.setPrimaryNode(primaryNode);
    newMetadata.setSecondaryNode(secondaryNode);

    //update the master's metadata
    self.masterMetadataObjects.set(newFile, newMetadata);

    var primaryNodeMeta = self.dataNodes.get(newMetadata.primaryNode);
    var secondaryNodeMeta = self.dataNodes.get(newMetadata.secondaryNode);

    primaryNodeMeta.addFile(newFile);
    secondaryNodeMeta.addFile(newFile);

    self.dataNodes.set(newMetadata.primaryNode, primaryNodeMeta);
    self.dataNodes.set(newMetadata.secondaryNode, secondaryNodeMeta);

    //notify dataNodes (hosts) to update their metadata. 
    var nodeIds = newMetadata.getAllNodes();

    //wait for nodes to ack they created metadata
    var requestID = uuidv1();
    var acknowledgments = 0;

    var promise = new Promise(function(resolve, reject){
        self.pubsub.subscribe(requestID, function()
        {
            acknowledgments++;
            if (acknowledgments == nodeIds.length)
            {
                self.pubsub.unsubscribe(requestID);
                resolve(newMetadata);
            }
        })
    })

    for (let j = 0; j<nodeIds.length; j++)
    {
        var tempNode = self.dataNodes.get(nodeIds[j]);
        // replicate set to false, as no file copying is needed. This is a newly created file.
        var request = {file: newFile, metadata: newMetadata, type: 'newFile', sender: 'master', id: requestID};
        self.pubsub.publish( (tempNode.nodeTopic), request);
    }
    
    return promise;
}

Master.prototype.processHeartbeat = function(message)
{
    var self = this;
    
    var currentNode = self.dataNodes.get(message.id);
    if (currentNode != undefined)
    {
        var date = new Date();
        currentNode.timeStamp = (date.getTime() / 1000);  //Time in seconds.
        self.dataNodes.set(message.id, currentNode);
        debug('processed heartbeat for dataNode: ', message.id, ' at time =', currentNode.timeStamp)
    }
    else
    {
        debug('ERROR: received heartbeat from an unidentified dataNode!')
    }
    
}

Master.prototype.replicate = function(brokenNodeId, files) //nodeId of node to be replicated and the files it has.
{
    var self = this;
    //Remove the broken node from node metadata map.
    self.dataNodes.delete(brokenNodeId);
    
    const len = files.length;
    for (let i=0; i<len; i++)
    {
        var metadata = self.masterMetadataObjects.get(files[i]);
        if (metadata == undefined)
        {
            debug(
                'ERROR: inconcistent master metadata for file:', 
                files[i], '/n Node:', 
                nodeId);

            continue;
        }
        // update the metadata of the file and rturn the nodeId for the new replica
        
        var newNode = metadata.updateReplicaMetadata(Array.from(self.dataNodes.keys()), brokenNodeId);
        self.masterMetadataObjects.set(files[i], metadata);

        // Update the metadata for the new node that will host the replica
        var newNodeMetadata = self.dataNodes.get(newNode);
        //debug('newNodeMetadata VALUE is: ', newNodeMetadata);
        newNodeMetadata.addFile(files[i]);
        //debug('NODE METADATA AFTER ADDFILE: ', newNodeMetadata);
        self.dataNodes.set(newNode, newNodeMetadata);
        
        //Notify impacted nodes to update their metadata (and replicate file in case of newNode)
        var nodeIds = metadata.getAllNodes();
        for (let j = 0; j<nodeIds.length; j++)
        {
            var tempNode = self.dataNodes.get(nodeIds[j]);
            //create a uuid so that nodes can confirm changes and add 'stable' flag to mastermeta
            var request = {file: files[i], metadata: metadata, type: 'replicate', sender: 'master'}
            self.pubsub.publish( (tempNode.nodeTopic), request);
        }
    }
    debug('MASTER METADATA after replication is: ', self.masterMetadataObjects);
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

module.exports = Master;
