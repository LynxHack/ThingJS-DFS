// Collection of utility functions here

// sends a message out to a channel and awaits a reponse (for one time comm)
// 

function mqttRequest(channel, message, recipient){
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

module.exports = {
    mqttRequest: mqttRequest,
}
