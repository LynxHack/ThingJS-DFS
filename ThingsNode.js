const dbs_store = require('./dbs_store');
const things = require('things-js');
class ThingsNode{
    constructor(pubsubURL){
        if(!pubsubURL){
            pubsubURL = 'mqtt://192.168.50.101'
        };
        this.pubsub = new things.Pubsub(pubsubURL);
        this.nodeID = "newnode";
        this.storageDir; //TODO
        this.fileDir; //TODO

        this.dfs;

        this.init_slave();
    }
 
    // init connection with Master
    async init_slave(){
        await this.init_slave_handshake();
        await this.init_slave_heartbeat();
        await this.init_chunkserver();
    }

    init_slave_handshake(){
        return new Promise((resolve, reject) => {
            try{
                //connect to thing-js pubsub
                this.pubsub.subscribe("init", (req) => {
                    if(req.sender !== "master"){return}

                    this.nodeID = req.message;
                    console.log("registered with master");
                    this.pubsub.unsubscribe("init");
                    clearInterval(timer);
                    resolve("registered");
                })

                //Make first publication to master
                var timer = setInterval(()=>{
                    console.log("Attempt connection with master");
                    this.pubsub.publish("init", {
                        sender: this.nodeID,
                        message: "Request connection"
                    })
		        }, 1000);
            }
            catch(err){reject(err)}
        })
    }

    init_slave_heartbeat(){
        return new Promise((resolve, reject) => {
            try{
                this.pubsub.subscribe("heartbeat", (req) => {
                    if(req.sender !== "master"){ return; }
                    // respond back right away to indicate still alive
                    console.log("Received heartbeat request from master")
                    this.pubsub.publish("heartbeat", {
                        sender: this.nodeID,
                        message: 'Still alive'
                    })
                }).then(()=>{
                    resolve(true)
                })
            }
            catch(err){reject(err)}
        })
    }

    // Listen to incoming mutation or read requests from clients
    async init_chunkserver(){
        try{
            this.dfs = new dbs_store('./');
            this.pubsub.subscribe("store", async (req) => {
                if(req.recipient !== this.nodeID){return}
                console.log("Received incoming request", req);
                switch(req.type){
                    case 'read':
                        var res = await this.dfs.read(req.data); 
                        this.pubsub.publish('store', { sender: this.nodeID, recipient: req.sender, data: res }); break;
                    case "write":
                        var res = await this.dfs.write(req.data); 
                        this.pubsub.publish('store', { sender: this.nodeID, recipient: req.sender, data: res }); break;
                    case "append":
                        var res = await this.dfs.append(req.data); 
                        this.pubsub.publish('store', { sender: this.nodeID, recipient: req.sender, data: res }); break;
                    case "delete":
                        var res = await this.dfs.delete(req.data); 
                        this.pubsub.publish('store', { sender: this.nodeID, recipient: req.sender, data: res }); break;
                    default:
                        throw new Error("Unknown dfs request");
                }
            })
        }
        catch(err){return err}
        return true;
    }
}

module.exports = ThingsNode;
