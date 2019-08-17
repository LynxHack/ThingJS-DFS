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
                        resolve(req);
                    }
                }).then(() => {
                    this.pubsub.publish(channel, message);
                });
            }
            catch(err){
                reject(err);
            }
        })
    }

    // Read Operation
    async read(dir){
        // obtain information from master regarding metadata
        var nodeInfo = await this.mqttRequest('client', {
            sender: this.clientid,
            recipient: 'master',
            data: null,
            dir : dir,
            type: 'read'
        }, this.clientid)

        // sends out read request to the primary node to read the information
        var result = await this.mqttRequest('store', {
            sender: this.clientid,
            recipient: nodeInfo.primary,
            data: null,
            dir : dir,
            type: 'read'
        }, this.clientid)

        console.log(result);
        return result;
    }


    // New Entry
    async write(dir, data){
        
    }

    // Update
    async append(dir, data){
        
    }

    // Delete Entry
    async delete(dir){
        
    }
}

module.exports = Client;