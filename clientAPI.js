var things = require('things-js');
const uuidv1 = require('uuid/v1'); //Timestamp
const display_debug_messages = true;

function DFS(pubsubURL){    
    this.pubsub = new things.Pubsub(pubsubURL);
}
DFS.prototype.constructor = DFS;

DFS.prototype.readFile = function (path, arg1, arg2){

    var self = this;
    var requestID = uuidv1();
    var options, callback;
    if (typeof arg1 === 'function') {
        callback = arg1;
    }
    else if (typeof arg1 === 'object' && typeof arg2 === 'function')
    {
        options = arg1;
        callback = arg2;
    }
    else
    {
        throw 'readFile(path, [options], callback): Invalid parameters'
    }

    var metadataRequest = {id: requestID, file: path, type: 'read'};
    debug("metadata:", metadataRequest);
    //obtain metadata from master
    // The master node will use this channel to return metadata for this request
    self.pubsub.subscribe(requestID, function(response){

        var readRequest = {id: requestID, type: 'read', file: path, sender: 'clientAPI'};
        if (response.sender == 'master')
        {
            debug('master response:', response);
        
            var contactNodeTopic = "'" + response.contactNodeID + "'";
            //communicate with contact node
            self.pubsub.publish(contactNodeTopic, readRequest)
                .then(function(outcome){
                    if (outcome === true){ 
                        debug('published to data node successfully', 
                                                    JSON.stringify(response.contactNodeID));
                        }
                    else console.log('ERROR publishing to data node');
                })
        }
        else if(response.sender === 'dataNode')
        {
            if (response.status == 'alternativeNode')
            {
                // The node's copy is corrupted, resend the request to alternative node.
                var contactNodeTopic = "'" + response.altNodeID + "'";
                //communicate with contact node
                self.pubsub.publish(contactNodeTopic, readRequest);  
            }
            else
            {
                debug('dataNode response:', response);
                callback(response.status, response.data); 
                self.pubsub.unsubscribe(requestID); 
            }

        }

    })
        .then(function(){
        //Trigger metadata request to master
        self.pubsub.publish('metadata', metadataRequest);
        })
}

DFS.prototype.appendFile = function appendFile(path, data, arg1, arg2)
{
    var self = this;
    var requestID = uuidv1();

    var options, callback;
	if (typeof arg1 === 'function'){
		callback = arg1;
	}
	else if (typeof arg1 === 'object' && typeof arg2 === 'function'){
		options = arg1;
		callback = arg2;
	}
	else {
		throw "appendFile(path, data[,options], callback): You need to provide a callback function";
    }
    self.pubsub.subscribe(requestID, function(response){
        if (response.sender == 'master')
        {
            debug('master response:', response);
            var appendRequest = 
                {id: requestID, type: 'append', file: path, data: data, sender: 'clientAPI'};
            var contactNodeTopic = "'" + response.contactNodeID + "'";
            //communicate with contact node
            self.pubsub.publish(contactNodeTopic, appendRequest);
        }
        else if (response.sender == 'dataNode')
        {
            debug('dataNode response:', response);
            callback(response.status, response.data);   
            self.pubsub.unsubscribe(self.requestID);
        }
    })
        .then(function(){
            var metadataRequest = {id: requestID, file: path, type: 'append'};
            self.pubsub.publish('metadata', metadataRequest);
        })

}

DFS.prototype.writeFile = function writeFile(path, data, arg1, arg2)
{
    var self = this;
    var requestID = uuidv1();

    var options, callback;
	if (typeof arg1 === 'function'){
		callback = arg1;
	}
	else if (typeof arg1 === 'object' && typeof arg2 === 'function'){
		options = arg1;
		callback = arg2;
	}
	else {
		throw "writeFile(path, data[,options], callback): You need to provide a callback function";
    }
    
    self.pubsub.subscribe(requestID, function(response){
        if (response.sender == 'master')
        {
            debug('master response:', response);
            var writeRequest = 
                {id: requestID, type: 'write', file: path, data: data, sender: 'clientAPI'};
            var contactNodeTopic = "'" + response.contactNodeID + "'";
            //communicate with contact node
            self.pubsub.publish(contactNodeTopic, writeRequest);
            
        }
        else if (response.sender == 'dataNode')
        {
            debug('dataNode response:', response);
            callback(response.status, response.data);   
            self.pubsub.unsubscribe(self.requestID);
        }
    })
        .then(function(){
            var metadataRequest = {id: requestID, file: path, type: 'write'};
            self.pubsub.publish('metadata', metadataRequest);
        })

}

function debug(...args)
{
    if (display_debug_messages)
    {
        var length = args.length;
        for (var i=0; i< length; i++)
            console.log(args[i]);
    }  
}

module.exports = DFS;
