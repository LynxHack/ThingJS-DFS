var _storage = require('node-persist');
var things = require('things-js');
var fs = require('fs');
const uuidv1 = require('uuid/v1');

class ThingsNode{
    constructor(pubsubURL){
        this.pubsub = new things.Pubsub(pubsubURL);
        this.nodeID;
        this.storageDir;
        this.fileDir;
        this.initialize(this.pubsub);
    }
 
    // init connection with Master
    initialize(pubsub){
        //connect to thing-js pubsub
        pubsub.subscribe("init", (req) => {
            if(req.sender === "master"){
                this.nodeID = req.nodeID;
            }
        }).then("subscribed to pubsub");

        //Make first publication to master
        pubsub.publish("init", {
            sender: 'newnode', //not yet initialized
            message: 'Request Connection'
        })

        pubsub.subscribe("heartbeat", (req) => {
            if(req.sender === "master"){
                // respond back right away to indicate still alive
                pubsub.publish("heartbeat", {
                    sender: this.nodeID,
                    message: 'Still alive'
                })
            }
        });
    }
}

module.exports = ThingsNode;