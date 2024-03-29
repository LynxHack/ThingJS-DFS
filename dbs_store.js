const storage = require("node-persist");

class dbs_store{
    constructor(directory){
        this.directory = directory ? directory : './'
        storage.init({
            dir: this.directory,
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
            console.log("DBS Storage Initiated")
        });

        
        // return (async () => {
        //     await storage.init({
        //         dir: this.directory,
        //         stringify: JSON.stringify,
        //         parse: JSON.parse,
        //         encoding: 'utf8',
        //         logging: false,  // can also be custom logging function
        //         ttl: false, // ttl* [NEW], can be true for 24h default or a number in MILLISECONDS or a valid Javascript Date object
        //         expiredInterval: 2 * 60 * 1000, // every 2 minutes the process will clean-up the expired cache
        //         // in some cases, you (or some other service) might add non-valid storage files to your
        //         // storage dir, i.e. Google Drive, make this true if you'd like to ignore these files and not throw an error
        //         forgiveParseErrors: false
        //     }).then(()=>{
        //         console.log("DBS Storage Initiated")
        //     });
        //     return true;
        // })();
    }


    // Returns request data (data is a key)
    async read(file){
        var res = await storage.getItem(file);
        console.log("DBS file read", file, res);
        return res;
    }

    // Adds data information to database
    async write(file, data){
        await storage.setItem(file, data);
        console.log("DBS New file stored", file, JSON.stringify(data));
        return true;
    }

    // Update a specific key, TODO: guarantee atomicity by rolling back if fails partially
    async append(file, data){
        await storage.updateItem(file, data);
        console.log("DBS New file updated", file, JSON.stringify(data));
        return true;
    }

    // Delete TODO, modify to set for removal so can be recovered)
    async delete(file){
        var res = await storage.removeItem(file);
        console.log("File removed", res);
        return res;
    }
}

module.exports = dbs_store;