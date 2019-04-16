var storage = require('node-persist');
var things = require('things-js');
const uuidv1 = require('uuid/v1');

const display_debug_messages = true;

var pubsubURL = process.argv.slice(2)[0]; //'mqtt://localhost'
var masterMetadataObjects = new Map();  //Maps file name/path to its master metadata
var dataNodes = new Map(); //Maps each data node ID to its Node metadata
debug('Master node\n');

var pubsub = new things.Pubsub(pubsubURL);

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

function handshakeClient(message){

    debug('handshakeClientEntry');
    var newNode = new Node(message.id);

    //parse all files in that dataNode
    for (let index = 0; index< message.metadata.length; ++index)
    {
        var element = message.metadata[index];
        var file = message.files[index];
        newNode.addFile(file);
        newNode.timeStamp = (new Date()).getTime() / 1000;
        var metaEntry = masterMetadataObjects.get(file);

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
            masterMetadataObjects.set(file, metaEntry);
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
    dataNodes.set(message.id, newNode);
    
    debug('ADDED NEW NODE', newNode);
}

function sendMetadata(metadataRequest)
{
    var response = {};
    var metaEntry;

    response.sender = 'master';
    if (metadataRequest.type == 'read')
    {
        if ( (metaEntry = masterMetadataObjects.get(metadataRequest.file)) != undefined)
        {
            //debug("metaEntry: ", metaEntry);
            response.status = 'success';
            
            response.contactNodeID = metaEntry.secondaryNode;
            pubsub.publish(metadataRequest.id, response);
            //debug("response: ", response);
        }
    }
    else if ( (metadataRequest.type == 'append') || (metadataRequest.type == 'write'))
    {
        if ( (metaEntry = masterMetadataObjects.get(metadataRequest.file)) == undefined)
        {
            //New file will be created.
            createNewMetadata(metadataRequest.file)
                .then(function(metaEntry){
                    //debug('received both acknowledgments for metadata creation');
                    response.contactNodeID = metaEntry.primaryNode; //All appends/writes have to go through primary node
                    pubsub.publish(metadataRequest.id, response);
                })
        }
        else
        {
            response.contactNodeID = metaEntry.primaryNode; //All appends/writes have to go through primary node
            pubsub.publish(metadataRequest.id, response);
        }

    }
};

function createNewMetadata(newFile)
{
    var primaryNode, secondaryNode;
    var potentialHosts = Array.from(dataNodes.keys());

    //select primary node
    primaryNode = potentialHosts[Math.floor(Math.random()*potentialHosts.length)];

    //remove the primary node from potential hosts before picking a secondary node
    potentialHosts.splice(potentialHosts.indexOf(primaryNode), 1);
    secondaryNode = potentialHosts[Math.floor(Math.random()*potentialHosts.length)];

    var newMetadata = new MasterMetadata(newFile);
    newMetadata.setPrimaryNode(primaryNode);
    newMetadata.setSecondaryNode(secondaryNode);

    //update the master's metadata
    masterMetadataObjects.set(newFile, newMetadata);

    var primaryNodeMeta = dataNodes.get(newMetadata.primaryNode);
    var secondaryNodeMeta = dataNodes.get(newMetadata.secondaryNode);

    primaryNodeMeta.addFile(newFile);
    secondaryNodeMeta.addFile(newFile);

    dataNodes.set(newMetadata.primaryNode, primaryNodeMeta);
    dataNodes.set(newMetadata.secondaryNode, secondaryNodeMeta);

    //notify dataNodes (hosts) to update their metadata. 
    var nodeIds = newMetadata.getAllNodes();

    //wait for nodes to ack they created metadata
    var requestID = uuidv1();
    var acknowledgments = 0;

    var promise = new Promise(function(resolve, reject){
        pubsub.subscribe(requestID, function()
        {
            acknowledgments++;
            if (acknowledgments == nodeIds.length)
            {
                pubsub.unsubscribe(requestID);
                resolve(newMetadata);
            }
        })
    })

    for (let j = 0; j<nodeIds.length; j++)
    {
        var tempNode = dataNodes.get(nodeIds[j]);
        // replicate set to false, as no file copying is needed. This is a newly created file.
        var request = {file: newFile, metadata: newMetadata, type: 'newFile', sender: 'master', id: requestID};
        pubsub.publish( (tempNode.nodeTopic), request);
    }
    
    return promise;
}

function processHeartbeat(message)
{
    
    var currentNode = dataNodes.get(message.id);
    if (currentNode != undefined)
    {
        var date = new Date();
        currentNode.timeStamp = (date.getTime() / 1000);  //Time in seconds.
        dataNodes.set(message.id, currentNode);
        debug('processed heartbeat for dataNode: ', message.id, ' at time =', currentNode.timeStamp)
    }
    else
    {
        debug('ERROR: received heartbeat from an unidentified dataNode!')
    }
    
}

debug("Pubsub connected");
pubsub.subscribe('nodeHandshake', handshakeClient);
pubsub.subscribe('metadata', sendMetadata);
pubsub.subscribe('heartbeat', processHeartbeat);

function replicate(brokenNodeId, files) //nodeId of node to be replicated and the files it has.
{
    //Remove the broken node from node metadata map.
    dataNodes.delete(brokenNodeId);
    //debug('dataNodes map is: ', dataNodes);
    const len = files.length;
    for (let i=0; i<len; i++)
    {
        var metadata = masterMetadataObjects.get(files[i]);
        if (metadata == undefined)
        {
            debug(
                'ERROR: inconcistent master metadata for file:', 
                files[i], '/n Node:', 
                nodeId);

            continue;
        }
        // update the metadata of the file and rturn the nodeId for the new replica
        //Array.from(dataNodes.keys())
        var newNode = metadata.updateReplicaMetadata(Array.from(dataNodes.keys()), brokenNodeId);
        masterMetadataObjects.set(files[i], metadata);

        // Update the metadata for the new node that will host the replica
        var newNodeMetadata = dataNodes.get(newNode);
        //debug('newNodeMetadata VALUE is: ', newNodeMetadata);
        newNodeMetadata.addFile(files[i]);
        //debug('NODE METADATA AFTER ADDFILE: ', newNodeMetadata);
        dataNodes.set(newNode, newNodeMetadata);
        
        //Notify impacted nodes to update their metadata (and replicate file in case of newNode)
        var nodeIds = metadata.getAllNodes();
        for (let j = 0; j<nodeIds.length; j++)
        {
            var tempNode = dataNodes.get(nodeIds[j]);
            //create a uuid so that nodes can confirm changes and add 'stable' flag to mastermeta
            var request = {file: files[i], metadata: metadata, type: 'replicate', sender: 'master'}
            pubsub.publish( (tempNode.nodeTopic), request);
        }
    }
    debug('MASTER METADATA after replication is: ', masterMetadataObjects);
}

setInterval(function(){
    //debug('STARTING HEARTBEAT INTERVAL')
    var currentTime = (new Date()).getTime() / 1000;

    dataNodes.forEach(function(value, key) {
        if (value.available == true)
        {
            if( (currentTime - value.timeStamp) > 5)  //We have not heard from that node in 5 seconds
            {
                value.available = false; // Mark as unavailable
                dataNodes.set(key, value);
                replicate(key, value.files);
                debug('Node has been marked unavailable: ', key)
            }
        }

      });

}, 5000);

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