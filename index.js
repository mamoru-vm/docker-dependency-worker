const DDP = require('ddp');            // meteor DDP protocol
const DDPlogin = require('ddp-login'); // login to meteor and establish authenticated session
const Docker = require('dockerode-promise');   // docker API 
const Job = require('meteor-job');     // meteor job, to subscribe to job collection and DO WORK on a job queue created by the meteor server 
const net = require('net');            // to check if meteor is running before trying to connect
const stream = require('stream');
const Promise = require('bluebird');
const streamToPromise = require('stream-to-promise');

// change 'manger' to 'localhost' if testing outside of container
const meteorIP = (typeof process.env.WORKER_USERNAME != 'undefined') ? 'manager' : 'localhost'; // from ExtraHosts in workerConfig           
const meteorPort = '3000';        

var ddp = new DDP({
                   host : meteorIP,
                   port : meteorPort,
                   autoReconnectTimer: 1500,
                   ssl: false
                  });   // initalize ddp library - default localhost:3000
// this will likely talk to meteor directly even if behind a proxy - likely will never need SSL or to change port
// prod env will require nginx container, a self signed cert, directing 443: on the host, back into meteor on 3000

// initalize meteor-job library ontop of ddp lib
Job.setDDP(ddp);                                
 
// connect to local docker.sock passed from host into container as a volume 
var docker = new Docker();                     

                                       
// worker username credentials to authenticated to meteor to sub to jobs
// passed to worker container via Env
const workerUsername = process.env.WORKER_USERNAME || "worker";        
const workerPass = process.env.WORKER_PASSWORD || "aStrongPass";

/// REGISTER DDP EVENT HANDLERS
ddp.on('socket-close', function(code, message) {
  console.log("Close: %s %s", code, message);
});

ddp.on('socket-error', function(error) {
  console.log("Error: %j", error);
});
/////////////////////////////////// END

/////////////////////////////////////////////
// Actual Job that gets run
////////////////////////////////////////////////////////////////////////////
function mainJob (job, cb) {
  const friendlyName = job.data.name
  const containerConfig = job.data.config
  const labelKey = Object.keys(job.data.config.Labels)[0]
  const labelFilter = `${labelKey}=${job.data.config.Labels[labelKey]}`

    // what is actually happening woo promises
    docker.listContainers({all:true, filters:{"label":[labelFilter]}})
    .then(testIfContainerExits)
    .then(checkContainerStatus)
    .then(handleContainer)
    .catch(logAndFail)


    function logAndFail(err){
      job.log(err, {echo:true, level:"warning"})
      job.fail()
      cb()
    }

  function handleContainer(result){
    job.log("got result! Id: " + result.Id + " running: " + result.running, {echo:true});
    var container = docker.getContainer(result.Id);
    if(result.running){
      inspectAndDone();
    } else {
      container.start()
      .then((data)=>{
        inspectAndDone();
      });
    }

  function inspectAndDone(){
    container.inspect().then((info)=>{
          job.done({
                    Name: friendlyName,
                    Id: info.Id,
                    State: info.State,
                    IP: info.NetworkSettings.IPAddress,
                    Ports: Object.keys(info.Config.ExposedPorts),
                    Env: createEnvObj(info.Config.Env) // created k:v for envionrment variables instead of ["V=X"]
                })
      cb()
    }).catch((err)=>{
          job.log("ERROR: " + err, {echo:true, level:"warning"});
          job.fail(err)
          cb()
    })
  }
  
  function createEnvObj(envArray){
        var envObj = {}
        for(var i=0;i<envArray.length;i++){
          var tmpArr = envArray[i].split('=')
          envObj[tmpArr[0]] = tmpArr[1]
        }
        return envObj
  }
 
  }


  function testIfContainerExits(containers){
    return new Promise((resolve,reject)=>{
      if(containers.length != 0){
        resolve(containers[0])
      } else {
        resolve(false);
      }
    })
  }


function checkContainerStatus(container){
  return new Promise((resolve, reject)=>{
    const upRegex = /^Up .*$/
    if(container && upRegex.test(container.Status)){
      job.log("do nothing, container is UP!", {echo:true});
      resolve({running:true, Id: container.Id});
    } else if(container && !upRegex.test(container.Status)) { // container exists but is not up...
        job.log("need to start container, starting...", {echo:true});
        resolve({running:false, Id: container.Id});
    } else if(!container){
      // pull just in case, if it is already local wont throw anything...
      docker.pull(containerConfig.Image)
      .then((stream)=>{
        // just care that stream completed before resolving...
        return streamToPromise(stream)
      })
      // image was pulled
      .then((result)=>{
        job.log("pulled container image: " + containerConfig.Image, {echo:true});
        // a big buffer of everything streamed during pull
      //console.log(result.toString()); 
      // create container 
      docker.createContainer(containerConfig)
      .then((cont)=>{
        job.log("created container!: "+ cont.id, {echo:true});
        resolve({running: false, Id: cont.id})
      })
      })
      .catch((err)=>{
        console.log(err);
        reject(err);
      })
    }

  })
} 

}

/// Connect to meteor server over DDP, 
function runMainWorker (address, port) {
  ddp.connect(function (err) {
    if (err) {
      console.log(err);
      //back off for a sec and start check
      setTimeout(testMeteorReady(address, port), 2500);
      return; 
    } else {
      DDPlogin.loginWithUsername(ddp, workerUsername, workerPass, {retry: 1}, function (err, userInfo) {
          if (err){
            console.log(err);
            return;
          } else {
            console.log("Successfully authenticated to meteor!")
    
        //start processing jobs on the Q
         var workers = Job.processJobs('depJobs', 'containerCheck', mainJob);


        };
      });
    }   
  });
}




function testMeteorReady(address, port){
  // leep trying to connect over TCP3000 before attempting to login to meteor...
  // this is defensive in the intent of being able to run this script somewhat independendly of meteor without it crashing
  var connectInterval = setInterval(()=>{
    const client = net.connect({host:address, port: port}, () => { //'connect' listener
      clearInterval(connectInterval)
      console.log(`${address} tcp/${port} is open, meteor is ready`);
      console.log('closing and starting meteor worker in 5 seconds');
      client.destroy()
      // wait 5 seconds before actually trying to autenticated
      setTimeout(runMainWorker(address,port)
        , 5000);
         
    });

    client.on('error',(err)=>{
      client.destroy()
      // do nothing
      //console.log(err);
    });
  }, 1000)
}


// this is to keep DDP client from hammering meteor server with requests meteor will eventually recieve?
// check if meteor connection is open over TCP before trying any ddp calls
// this is is blocking, once DDP connects, it is blocking - could deamonize this and log to file
testMeteorReady(meteorIP, meteorPort);