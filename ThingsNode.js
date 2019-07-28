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

    initialize(pubsub){
        //connect to thing-js pubsub
        pubsub.subscribe("init", (req) => {
            console.log(req);
            console.log("slave subscribed to init channel");
        });

        pubsub.publish("init", {
            sender: this.nodeID, //not yet initialized
            message: 'Request Connection'
        })
    }
}

module.exports = ThingsNode;