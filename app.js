var express         = require('express');
var app             = express();
const { Client }    = require('@sap/xb-msg-amqp-v100');
var bodyParser      = require('body-parser')
var activeMQConnect = null;
var connectionState = false;
var reconnectCount  = 1;
var xsenv           = require("@sap/xsenv");
var maxReconnectCount = process.env.MAX_RECONNECT_COUNT;
var PORT            = process.env.PORT || 5000;
var timeInterval    = process.env.RECONNECTION_INTERVAL;

app.use(bodyParser.urlencoded({ extended: false }))
app.use(bodyParser.json())
xsenv.loadEnv();

function setOptions() {
    console.log("<<< Inside setOptions method---")
    var creds = xsenv.serviceCredentials({ tag: 'messaging-service', name : "activemq"});
    console.log(creds);
    var temp = creds.url.split("//");
    var temp1 = temp[1].split(":");
    
    var options = {};
    options.net = {
        host :  temp1[0],
        port :  temp1[1]
    };

    return options;
}


app.listen(PORT,function(){
   
   console.log("<<<<server started at port---",PORT)
   activeMQConnect = new Client();
   activeMQConnect.connect(setOptions());
   activeMQConnect
    .on('connected',(destination, peerInfo) => {
        console.log('<<<< connected', peerInfo.description);
        connectionState = true;
        console.log("<<<<< state in connected--",connectionState);
        reconnectCount = 1;
    })
    .on('error', (error) => {
        console.log("<<<< error is---",error.message);
        connectionState = false;
        console.log("<<<< state in error---",connectionState)
    })
    .on('reconnecting', (destination) => {
        console.log('<<<<reconnecting, using destination ' + destination);
        connectionState = true;
        console.log("<<<< state in reconnecting--",connectionState)

    })
    .on('disconnected', (hadError, byBroker, statistics) => {
        console.log('<<<< disconnected',hadError,byBroker,statistics);
        reconnectCount++;
        connectionState = false;
        console.log("<<<< state in disconnected--",connectionState)

        if(!connectionState && reconnectCount<maxReconnectCount) {
            var retryConnection = setInterval(function() {
                console.log("<<<< inside interval--",reconnectCount)
                activeMQConnect.connect();
                if(connectionState || reconnectCount>maxReconnectCount) {
                    console.log("<<< clearing setInterval--",reconnectCount)
                    clearInterval(retryConnection);
                }
            },timeInterval)
        }
        

        

    })
})

//api to Generate messages in message broker
app.post("/api/writeMessages", function(req, res) {
    console.log("<<<<<< Object in request body to write in message broker",JSON.stringify(req.body));
    console.log("<<<<<<< connection value is",connectionState)
    var msg = req.body;
    if(!msg.message || !Object.keys(msg.message).length) {
        res.send({code:201,message:"message is required and must have some value"})
    }
    if(!connectionState) {                                      
        res.send({code:201,message:"Activemq server is down"})  // sending error response if activemq server is not up
    }
    generateMsg(msg).then(function(msgRes) {
        console.log("<<<<<message generated successfully")
        return res.send({code:200,message:msgRes,data:msg.message})
    }).catch(function(err) {
        console.log("<<<<< error is in generateMsg method",err)
        return res.send({code:201,message:"Error in genreting message",data:{}})
    })
})

function generateMsg(params) {
    return new Promise(function(resolve, reject) {
        console.log("inside generateMsg---->",params);
        var payload     = Buffer.from(JSON.stringify(params.message));
        var senderName  = params.serviceName || "sender";
        var queueName   = params.queueName || "queue";
        var message = { 
            payload : payload,
                target: {
                    header: {
                        durable: params.persistence || true,
                        priority: params.priority || 2,
                        ttl: params.ttl || null, 
                    },
                },
                properties: {
                    msgType:"test"
                },
                done: () => {
                    console.log("Message was published");
                    return resolve({success : true});
                },
                failed: (err) => {
                    console.log("Message publishing failed,", err);
                    return reject({success : false, msg : "Error in genereting message"});
                }
            }     
        //Data to be write in message broker
        const stream = activeMQConnect.sender(senderName).attach(queueName);
        stream.write(message); 
        stream.end();
    })
}





