var Client = require('../Client.js');
var test = new Client();
test.write('testfile', {
    message: "You are doing a great job! Keep up the good work!",
    status: 200
}).then(()=>{
    console.log("completed write operation");
});