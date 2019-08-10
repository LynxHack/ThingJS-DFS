const dbs_store = require('./dbs_store');
const things = require('things-js');
class ThingsNode{
    constructor(pubsubURL){
        this.pubsub = new things.Pubsub(pubsubURL);
        this.nodeID;
        this.storageDir; //TODO
        this.fileDir; //TODO

        this.dfs;

        this.init_client(this.pubsub);
    }
 
    // init connection with Master
    init_client(pubsub){
        this.init_client_handshake(pubsub);
        this.init_client_heartbeat(pubsub);
        this.init_client_filestorage(pubsub);
    }

    init_client_filestorage(pubsub){
        this.dfs = new dbs_store();
        pubsub.subscribe("store", (req) => {
            if(req.sender !== "master"){return;}
            switch(req.action){
                case "create":
                    this.dfs.create(req.data); break;
                case "read":
                    this.dfs.read(req.data); break;
                case "update":
                    this.dfs.update(req.data); break;
                case "delete":
                    this.dfs.delete(req.data); break;
                case "default":
                    throw new Error("Unknown dfs request");
            }
        })
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