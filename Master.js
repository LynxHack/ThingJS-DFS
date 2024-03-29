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
        this.numreplica = 2; //preset value
        this.metadata = {};

        this.init_master();
    }

    async init_master(){
        await this.init_client_listening();
        await this.init_master_handshake();
        await this.init_master_heartbeat();
    }
    
    
    // Given a list of nodes, pick a primary and two secondary nodes
    //TODO pick node based on amount of storage
    pickNode(nodes){
        var i = Math.floor(nodes.length * Math.random()); //pick random node between 0 to len - 1
        return i;
    }


    // Communication between clients and master
    init_client_listening(){
        return new Promise((resolve, reject) => {
            try{
                this.pubsub.subscribe('client', (req) => {
                    if(req.recipient !== "master"){return}
                    var nodelist = Object.keys(this.dataNodes)
                    var client = req.sender;
                    var file = req.file;
                    var data = req.data;
                    console.log("Request from", client);
                    switch(req.type){
                        case "read":
                            this.pubsub.publish('client', {sender: 'master', recipient: client, node: this.metadata[file]});
                            console.log("Master: Performing read file operation on", )
                            break;
                        case "append":
                        case "write":
                            var primaryindex = this.metadata[file] ? this.metadata[file].primary : this.pickNode(nodelist);
                            var primary = nodelist.splice(primaryindex, 1)[0];
                            console.log(primary)
			    var secondary = this.metadata[file] ? this.metadata[file].secondary : [];
                            // fill up to up to preset number of replicas
                            for(let i = 0; i < this.numreplica && nodelist.length; i++){
                                var tmp = nodelist.splice(this.pickNode(nodelist))[0];
                                secondary.push(tmp);
                            }
                            var resNode = {
                                primary: primary,
                                secondary: secondary
                            }

                            this.metadata[file] = resNode;
                            console.log("Master: Performing write file operation on", resNode)
                            this.pubsub.publish('client', {sender: 'master', recipient: client, node: resNode})
                            break;
                        case "delete":
                            var resNode = this.metadata[file] ? this.metadata[file] : "error";
                            console.log("Master: performing delete file operation on", resNode)
                            this.pubsub.publish('client', {sender: 'master', recipient: client, node: resNode})
                            break;
                        default:
                            this.pubsub.publish('client', {sender: 'master', recipient: client, node: "Unknown command"});
                            break;

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


