"use strict";

var things = require('things-js');
const uuidv1 = require('uuid/v1');

export default class Master{
    constructor(pubsubURL){
        this.pubsub = new things.Pubsub(pubsubURL);
        this.masterMetadataObjects = new Map();  //Maps file name/path to its master metadata
        this.dataNodes = new Map(); //Maps each data node ID to its Node metadata    
        this.initialize(this.pubsub);
    }

    initialize(pubsub){
        this.pubsub.subscribe('init', (req) => {
            console.log(req);
            console.log("received an incoming request from a slave node");
        });
        // this.pubsub.subscribe('heartbeat', (req) => {
        //     self.processHeartbeat(message)
        // });
    }
}
