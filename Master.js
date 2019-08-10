var things = require('things-js');
const uuidv1 = require('uuid/v1');

class Master{
    constructor(pubsubURL){
        this.pubsub = new things.Pubsub(pubsubURL);
        this.dataNodes = new Map(); //Maps each data node ID to its Node metadata    
        this.initialize(this.pubsub);

        this.alivelist = {} //nodeID, bool
    }
 
    initialize(){
        return new Promise((resolve, reject) => {
            this.pubsub.subscribe('init', (req) => {
                var newid = uuidv1();
                this.pubsub.publish('init', {
                    sender: 'master',
                    nodeID = newid,
                    message: "Acknowledged by master"
                });
            }).then((topic) => { 
                console.log(`subscribed to ${topic}`)
            });
        });
    }

    // Heartbeats code
    initheartbeat(){
        this.pubsub.subscribe('heartbeat', (req) => {
            var nodeID = req.sender;
            if(this.alivelist[nodeID]){
                alivelist[nodeID] = true;
            }
        }).then(checknodes()); //start checking nodes once master is successfully set up with heartbeat subscription
    }


    // Set infinite loop of 4 second heartbeat monitoring of nodes
    checknodes(){
        var deadnodes = [];
        for(node in this.alivelist){ 
            if(!this.alivelist[node]){
                deadnodes.push(node);
            }
        }
        setTimeout(function () {
            for (node in this.alivelist){ this.alivelist[node] = false; } //initiate all to false
            this.pubsub.publish('heartbeat', {
                sender: "master",
                message: "Are you alive"
            })

            checknodes();
        }, 4000);
    }
}

module.exports = Master;


