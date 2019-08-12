const things = require('things-js');
const uuidv1 = require('uuid/v1');

class Client{
    constructor(pubsubURL){
        if(!pubsubURL){
            pubsubURL = 'mqtt://192.168.50.101'
        };
        this.pubsub = new things.Pubsub(pubsubURL);
        this.clientid = uuidv1();

        await init_client(pubsub);
    }

    // Connect with master to begin communication
    init_client(pubsub){
        return new Promise((resolve, reject) => {
            pubsub.subscribe('client', (req) => {
                if(req.sender === 'master'){
                    console.log(req.message);
                    console.log("Connected to master successfully")
                    pubsub.unsubscibe('client');
                    resolve("Connected to master successfully")
                }
            });

            pubsub.publish('client', {
                sender: client + `clientid`,
                type: 'Connect',
                message: `client${clientid} connecting to master`
            });
        })
    }

    
    // New Entry
    write(dir, data){
        this.pubsub.publish('client', {
            sender: client + `clientid`,
            data: data,
            dir : dir,
            type: 'write',
            message: `write file`
        });
    }

    // Read Operation
    read(dir){
        this.pubsub.publish('client', {
            sender: client + `clientid`,
            // data: data,
            dir : dir,
            type: 'read',
            message: `client${clientid} connecting to master`
        });
    }

    // Update
    append(dir, data){
        this.pubsub.publish('client', {
            sender: client + `clientid`,
            data: data,
            dir : dir,
            type: 'append',
            message: `client${clientid} connecting to master`
        });
    }

    // Delete Entry
    delete(dir){
        pubsub.publish('client', {
            sender: client + `clientid`,
            // data: data,
            dir : dir,
            type: 'delete',
            message: `client${clientid} connecting to master`
        });
    }
}

module.exports = Client;