const storage = require("node-persist");

class dbs_store{
    constructor(){
        await storage.init({
            dir: './dfs_store',
            stringify: JSON.stringify,
            parse: JSON.parse,
            encoding: 'utf8',
            logging: false,  // can also be custom logging function
            ttl: false, // ttl* [NEW], can be true for 24h default or a number in MILLISECONDS or a valid Javascript Date object
            expiredInterval: 2 * 60 * 1000, // every 2 minutes the process will clean-up the expired cache
            // in some cases, you (or some other service) might add non-valid storage files to your
            // storage dir, i.e. Google Drive, make this true if you'd like to ignore these files and not throw an error
            forgiveParseErrors: false
        });
    }

    // Adds data information to database
    create(data){
        for(key in data){
            await this.storage.setItem(key, data[key]);
        }
        return true;
    }


    // Returns request data (data is a key)
    read(data){
        return storage.getItem(data);
    }

    // Update a specific key
    update(data){
        for(key in data){
            await this.storage.setItem(key, data[key]);
        }
        return true;
    }

    // Delete (TODO, modify to set for removal so can be recovered)
    delete(data){
        await this.storage.removeItem(data);
        return true;
    }

}

module.exports = dbs_store;