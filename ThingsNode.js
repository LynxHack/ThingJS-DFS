const dbs_store = require('./dbs_store');
const things = require('things-js');
class ThingsNode{
    constructor(pubsubURL){
        if(!pubsubURL){
            pubsubURL = 'mqtt://192.168.50.101'
        };
        this.pubsub = new things.Pubsub(pubsubURL);
        this.nodeID;
        this.storageDir; //TODO
        this.fileDir; //TODO

        this.dfs;

        this.init_slave(this.pubsub);
    }
 
    // init connection with Master
    async init_slave(pubsub){
        await this.init_slave_handshake(pubsub);
        await this.init_slave_heartbeat(pubsub);
        await this.init_slave_filestore(pubsub);
    }

    init_slave_handshake(pubsub){
        return new Promise((resolve, reject) => {
            try{
                //connect to thing-js pubsub
                pubsub.subscribe("init", (req) => {
                    if(req.sender !== "master"){return;}
                    this.nodeID = req.message;
                }).then(()=>{
                    resolve("subscribed");
                    console.log("subscribed to pubsub")
                });

                //Make first publication to master
                pubsub.publish("init", {
                    sender: 'newnode', //not yet initialized
                    message: 'Request Connection'
                })
            }
            catch(err){reject(err)}
        })
    }

    init_slave_heartbeat(pubsub){
        return new Promise((resolve, reject) => {
            try{
                pubsub.subscribe("heartbeat", (req) => {
                    if(req.sender !== "master"){ return; }
                    // respond back right away to indicate still alive
                    console.log("Received heartbeat request from master")
                    pubsub.publish("heartbeat", {
                        sender: this.nodeID,
                        message: 'Still alive'
                    })
                }).then(()=>{
                    resolve("success")
                })
            }
            catch(err){reject(err)}
        })
    }

    init_slave_filestore(pubsub){
        return new Promise((resolve, reject) => {
            try{
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
                        default:
                            throw new Error("Unknown dfs request");
                    }
                }).then(()=>{
                    resolve("success");
                })
            }
            catch(err){reject(err)}
        })
    }
}

module.exports = ThingsNode;