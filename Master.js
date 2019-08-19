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

        this.metadata = {
            "testfile" : {
                primary: "primtestNode",
                secondary: ["test1", "test2"]
            }
        };

        this.init_master();
    }

    async init_master(){
        await this.init_client_listening();
        await this.init_master_handshake();
        await this.init_master_heartbeat();
    }
    

    // Communication between clients and master
    init_client_listening(){
        return new Promise((resolve, reject) => {
            try{
                this.pubsub.subscribe('client', (req) => {
                    if(req.recipient !== "master"){return}
                    var client = req.sender;
                    var file = req.file;
                    console.log("Request from", client);
                    if(req.type === "read"){
                        this.pubsub.publish('client', {sender: 'master', recipient: client, data: this.metadata[file]})
                    }
                    else if(req.type === "write" || req.type === "append"){
                        var resNode = this.metadata[file] ? this.metadata[file] : this.pickNode();
                        this.pubsub.publish('client', {sender: 'master', recipient: client, data: resNode})
                    }
                    else if(req.type === "delete"){
                        var resNode = this.metadata[file] ? this.metadata[file] : "error";
                        this.pubsub.publish('client', {sender: 'master', recipient: client, data: resNode})
                    }
                    else{
                        this.pubsub.publish('client', {sender: 'master', recipient: client, data: "Unknown command"})
                    }
                }).then((topic) => { 
                    console.log(`subscribed to ${topic}`);
                    resolve(true);
                });
            }
            catch(err){ reject(err); }
        });
    }



    // Initial Communication between slaves and master
    init_master_handshake(){
        return new Promise((resolve, reject) => {
            try{
                this.pubsub.subscribe('init', (req) => {
                    if(req.sender !== 'newnode'){return}
                    var newid = uuidv1();
                    this.pubsub.publish('init', {
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
    init_master_heartbeat(){
        return new Promise((resolve, reject) => {
            try{
                this.pubsub.subscribe('heartbeat', (req) => {
                    if(req.sender === 'master'){return}
                    var nodeID = req.sender;
                    console.log(`Received heartbeat response from ${nodeID}`);

		            if(typeof this.alivelist[nodeID] !== "undefined"){
                        this.alivelist[nodeID] = true;
                    }
                }).then((topic) => {
                    console.log(`subscribed to ${topic}`)
                    this.checknodes(); 
                    resolve(true);
                })
            }
            catch(err){reject(err)}
        });
    }

    // Given a list of nodes, pick a primary and two secondary nodes
    //TODO pick node based on amount of storage
    pickNode(){
        var numNodes = this.dataNodesObject.keys(this.dataNodes).length;
        var i = Math.floor(numNodes * Math.random()); //pick random node between 0 to len - 1
        return this.dataNodes[i];
    }


    // Takes in a list of nodes that would be dead, and reduplicates
    // its lost information to another available node
    reduplicate(nodes){
        // TODO find new nodes to reduplicate lost information
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
                delete this.dataNodes[node]; //delete the node from existing list to pick from
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
}

module.exports = Master;


