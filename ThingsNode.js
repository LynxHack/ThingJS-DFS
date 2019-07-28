var _storage = require('node-persist');
var things = require('things-js');
var fs = require('fs');
const uuidv1 = require('uuid/v1');

export default class ThingsNode{
    constructor(pubsubURL, nodeID, storageDir, fileDir){
        this.pubsub = new things.Pubsub(pubsubURL);
        this.nodeID = nodeID;
        this.storageDir = storageDir;
        this.fileDir = fileDir;

    }

    async initialize(pubsub){
        //connect to thing-js pubsub
        pubsub.subscribe("init", (req) => {
            console.log(req);
            console.log("slave subscribed to init channel");
        });
    }
}