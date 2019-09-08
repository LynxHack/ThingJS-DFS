// Collection of utility functions here

// sends a message out to a channel and awaits a reponse (for one time comm)
// 

function mqttRequest(channel, pubsub, message, recipient){
    return new Promise((resolve, reject) => {
        try{
            pubsub.subscribe(channel, (req) => {
                if(req.recipient === recipient){ //only resolve once message is intended for this recipient
                    pubsub.unsubscribe(channel);
                    console.log("Received reply", req);
                    resolve(req);
                }
            }).then(() => {
                console.log("Making mqtt request");
                console.log(channel, message, recipient);
                pubsub.publish(channel, message);
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
