const things = require('things-js');
const uuidv1 = require('uuid/v1');

class Client{
    constructor(pubsubURL){
        if(!pubsubURL){
            pubsubURL = 'mqtt://192.168.50.101'
        };
        this.pubsub = new things.Pubsub(pubsubURL);
        this.clientid = uuidv1();
    }

    // sends a message out to a channel and awaits a reponse (for one time comm)
    // 
    mqttRequest(channel, message, recipient){
        return new Promise((resolve, reject) => {
            try{
                this.pubsub.subscribe(channel, (req) => {
                    if(req.recipient === recipient){ //only resolve once message is intended for this recipient
                        this.pubsub.unsubscribe(channel);
                        console.log("Received reply", req);
                        resolve(req);
                    }
                }).then(() => {
                    console.log("Making mqtt request");
                    console.log(channel, message, recipient);
                    this.pubsub.publish(channel, message);
                });
            }
            catch(err){
                reject(err);
            }
        })
    }

    // Read Operation
    async read(file){
        // obtain information from master regarding metadata
        var nodeInfo = await this.mqttRequest('client', {
            sender: this.clientid,
            recipient: 'master',
            data: null,
            file : file,
            type: 'read'
        }, this.clientid)

        // sends out read request to the primary node to read the information
        var result = await this.mqttRequest('store', {
            sender: this.clientid,
            recipient: nodeInfo.node.primary,
            data: null,
            file : file,
            type: 'read'
        }, this.clientid)

        console.log(result);
        return result;
    }

    // New Entry
    async write(file, data){
            // obtain information from master regarding metadata
            var nodeInfo = await this.mqttRequest('client', {
                sender: this.clientid,
                recipient: 'master',
                data: data,
                file : file,
                type: 'write'
            }, this.clientid)
    
            // sends out write request to the primary node to write the information
            var result = await this.mqttRequest('store', {
                sender: this.clientid,
                recipient: nodeInfo.node.primary,
                data: data,
                file : file,
                type: 'write'
            }, this.clientid)
    
            console.log(result);
            return result;
    }

    // Update
    async append(file, data){
        
    }

    // Delete Entry
    async delete(file){
        
    }
}

module.exports = Client;