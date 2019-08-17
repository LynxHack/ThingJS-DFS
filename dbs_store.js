const storage = require("node-persist");

class dbs_store{
    constructor(directory){
        return (async () => {
            await storage.init({
                dir: directory,
                stringify: JSON.stringify,
                parse: JSON.parse,
                encoding: 'utf8',
                logging: false,  // can also be custom logging function
                ttl: false, // ttl* [NEW], can be true for 24h default or a number in MILLISECONDS or a valid Javascript Date object
                expiredInterval: 2 * 60 * 1000, // every 2 minutes the process will clean-up the expired cache
                // in some cases, you (or some other service) might add non-valid storage files to your
                // storage dir, i.e. Google Drive, make this true if you'd like to ignore these files and not throw an error
                forgiveParseErrors: false
            }).then(()=>{
                console.log("Storage Initiate")
            });
            return true;
        })();
    }


    // Returns request data (data is a key)
    async read(data){
        var res = await storage.getItem(data)
        return res;
    }

    // Adds data information to database
    async write(data){
        for(key in data){
            console.log(key, data[key])
            await storage.setItem(key, data[key])
        }
        console.log("New file stored", data);
        return true;
    }

    // Update a specific key, TODO: guarantee atomicity by rolling back if fails partially
    async append(data){
        for(key in data){
            await storage.setItem(key, data[key]);
        }
        return true;
    }

    // Delete TODO, modify to set for removal so can be recovered)
    async delete(data){
        var res = await storage.removeItem(data);
        console.log(res)
        return res.removed;
    }
}

module.exports = dbs_store;