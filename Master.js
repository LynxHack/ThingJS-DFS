var things = require('things-js');
const uuidv1 = require('uuid/v1');

class Master{
    constructor(pubsubURL){
        this.pubsub = new things.Pubsub(pubsubURL);
        this.dataNodes = {}; //Maps each data node ID to its Node metadata    
        this.init_master(this.pubsub);

        this.alivelist = {} //nodeID, bool
    }

    init_master(pubsub){
        await this.init_master_handshake(pubsub);
        await this.init_master_filestore(pubsub);
        await this.init_master_heartbeat(pubsub);
    }
    
    init_master_handshake(pubsub){
        return new Promise((resolve, reject) => {
            pubsub.subscribe('init', (req) => {
                var newid = uuidv1();
                pubsub.publish('init', {
                    sender: 'master',
                    nodeID = newid,
                    message: "Acknowledged by master"
                });

                // Add to list of alive nodes
                this.dataNodes[newid] = []; //modify to metadata object
                this.alivelist[newid] = true;
                
            }).then((topic) => { 
                console.log(`subscribed to ${topic}`)
                resolve("success");
            });
        });
    }

    // Initialize channel for filestorage comm
    init_master_filestore(pubsub){
        // TODO subscribe to file storage
        return new Promise((resolve, reject) => {
            pubsub.subscribe("store", (req) => {
                // listen to incoming message for whether action is successfull
            }).then((topic) => {
                console.log(`subscribed to ${topic}`)
                resolve("success")
            })
        })
    }

    // Initializes heartbeat
    init_master_heartbeat(pubsub){
        return new Promise((resolve, reject) => {
            pubsub.subscribe('heartbeat', (req) => {
                        var nodeID = req.sender;
                        if(this.alivelist[nodeID]){
                            alivelist[nodeID] = true;
                        }
                    }).then((topic) => {
                        console.log(`subscribed to ${topic}`)
                        checknodes(); 
                        resolve("success")
                    })
            });
        }

    // Function checks nodes in 4 second intervals
    // Set infinite loop of 4 second heartbeat monitoring of nodes
    checknodes(){
        var deadnodes = [];
        for(node in this.alivelist){ 
            if(!this.alivelist[node]){
                deadnodes.push(node);
            }
        }
        // do something to deadnodes' lost information
        reduplicate(deadnodes);

        setTimeout(function () {
            for (node in this.alivelist){ this.alivelist[node] = false; } //initiate all to false
            this.pubsub.publish('heartbeat', {
                sender: "master",
                message: "Are you alive"
            })
            checknodes();
        }, 4000);
    }

    // Takes in a list of nodes that would be dead, and reduplicates
    // its lost information to another available node
    reduplicate(nodes){
        // TODO
    }
}

module.exports = Master;


