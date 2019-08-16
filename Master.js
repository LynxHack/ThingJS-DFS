const things = require('things-js');
const uuidv1 = require('uuid/v1');

class Master{
    constructor(pubsubURL){
        if(!pubsubURL){
            pubsubURL = 'mqtt://192.168.50.101'
        };
        this.pubsub = new things.Pubsub(pubsubURL);
        this.dataNodes = {}; //Maps each data node ID to its Node metadata    
        this.alivelist = {} //nodeID, bool

        this.init_master(this.pubsub);
    }

    async init_master(pubsub){
        await this.init_master_handshake(pubsub);
        await this.init_master_heartbeat(pubsub);
        await this.init_master_filestore(pubsub);
    }
    
    init_client_handshake(pubsub){
        return new Promise((resolve, reject) => {
            try{
                pubsub.subscribe('client', (req) => {
                    pubsub.publish('client', {
                        sender: 'master',
                        message: `client ${req.sender} Connected to master`
                    });
                    console.log(`client ${req.sender} connected to master`);
                }).then((topic) => { 
                    console.log(`subscribed to ${topic}`);
                    resolve("success");
                });
            }
            catch(err){ reject(err); }
        });
    }

    init_master_handshake(pubsub){
        return new Promise((resolve, reject) => {
            try{
                pubsub.subscribe('init', (req) => {
                    if(req.sender !== 'newnode'){return}
                    var newid = uuidv1();
                    pubsub.publish('init', {
                        sender: 'master',
                        message: newid
                    });
    
                    // Add to list of alive nodes
                    this.dataNodes[newid] = []; //modify to metadata object
                    this.alivelist[newid] = true; 
                    console.log(`Added node ${newid} to list of nodes`);
                }).then((topic) => { 
                    console.log(`subscribed to ${topic}`);
		    resolve(true);
                });
            }
            catch(err){reject(err)}
        });
    }

    // Initializes heartbeat
    init_master_heartbeat(pubsub){
        return new Promise((resolve, reject) => {
            try{
                pubsub.subscribe('heartbeat', (req) => {
                    if(req.sender === 'master'){return}
                    var nodeID = req.sender;
                    console.log(`Received heartbeat response from ${nodeID}`);
		    if(typeof this.alivelist[nodeID] !== "undefined"){
                        this.alivelist[nodeID] = true;
                    }
                }).then((topic) => {
                    console.log(`subscribed to ${topic}`)
                    this.checknodes(); 
                    resolve("success");
                })
            }
            catch(err){reject(err)}
        });
    }


    // Initialize channel for filestorage comm
    init_master_filestore(pubsub){
        return new Promise((resolve, reject) => {
            try{
                pubsub.subscribe("store", (req) => {
                    // listen to incoming message for whether action is successfull
                }).then((topic) => {
                    console.log(`subscribed to ${topic}`);
                    resolve("success");
                })
            }
            catch(err){reject(err)}
        })
    }
    
    // Set infinite loop of 4 second heartbeat monitoring of nodes
    checknodes(){
        // Get list of dead nodes
        console.log("Checking nodes ...")
        console.log(this.alivelist);
	var deadnodes = [];
        for(let node in this.alivelist){ 
            if(!this.alivelist[node]){
                console.log(`${node} is dead`);
                deadnodes.push(node);
            }
        }

        // do something to deadnodes' lost information
        this.reduplicate(deadnodes);

        // Reset state of alive list and send heartbeat request to all nodes
       Object.keys(this.alivelist).forEach(v => this.alivelist[v] = false);
       this.pubsub.publish('heartbeat', {sender: "master", message: "Check status"});

        // Validate in 4 seconds
        setTimeout(() => {this.checknodes()}, 4000);
    }

    // Takes in a list of nodes that would be dead, and reduplicates
    // its lost information to another available node
    reduplicate(nodes){
        // TODO
    }
}

module.exports = Master;


