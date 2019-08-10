const _storage = require('node-persist');
const uuidv1 = require('uuid/v1');
const things = require('things-js');
const fs = require('fs');


class ThingsNode{
    constructor(pubsubURL){
        this.pubsub = new things.Pubsub(pubsubURL);
        this.nodeID;
        this.storageDir;
        this.fileDir;
        this.init_client(this.pubsub);
    }
 
    // init connection with Master
    init_client(pubsub){
        this.init_client_handshake(pubsub);
        this.init_client_heartbeat(pubsub);
    }

    init_client_filestorage(pubsub){
        //TODO - Subscribe to file storage channel
        pubsub.subscribe("store", (req) => {
            if(req.sender !== "master"){return;}
            // TODO do something with the item to store
        })


        //TODO - Set up file storage on this node

        //TODO - add CRUD operations for storage (function modules)
    }

    init_client_handshake(pubsub){
        //connect to thing-js pubsub
        pubsub.subscribe("init", (req) => {
            if(req.sender !== "master"){return;}
            this.nodeID = req.nodeID;
        }).then("subscribed to pubsub");

        //Make first publication to master
        pubsub.publish("init", {
            sender: 'newnode', //not yet initialized
            message: 'Request Connection'
        })
        initheartbeat();
    }

    init_client_heartbeat(pubsub){
        pubsub.subscribe("heartbeat", (req) => {
            if(req.sender !== "master"){ return; }
            // respond back right away to indicate still alive
            pubsub.publish("heartbeat", {
                sender: this.nodeID,
                message: 'Still alive'
            })
        });
    }
}

module.exports = ThingsNode;