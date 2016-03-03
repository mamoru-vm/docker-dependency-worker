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

function getContainers(containerLabelorId, opts){
  if(opts.by == "label"){
    const labelFilter = `mamoru=${containerLabelorId}`
    return new Promise((resolve,reject)=>{
    docker.listContainers({all:true, filters:{"label":[labelFilter]}}).then((containers)=>{
        if(containers != 0){
          //only one container should be labeled as such, so return the one
          resolve(containers[0])
        } else {
          // the container does not exist.
          resolve(false)
        }
      })
    })
  } else if(opts.by == "Id") {
    return new Promise((resolve, reject)=>{
      console.log("checking container by ID");
      container = docker.getContainer(containerLabelorId);
      container.inspect()
      .then((details)=>{
        resolve(details);
      })
      .catch((err)=>{
        resolve(false);
      })
    })

  }
}

function createEnvObj(envArray){
      var envObj = {}
      for(var i=0;i<envArray.length;i++){
        var tmpArr = envArray[i].split('=')
        envObj[tmpArr[0]] = tmpArr[1]
      }
      return envObj
}


function checkCJob(job){
    const friendlyName = job.data.label
    var jobResponse = {up:false, containerId: null, details:null} // base response if container doesnt exist
    job.log(`checking ${friendlyName}...`, {echo:true});
   // find container by unique label, return status
    const filterType = (job.data.Id == null) ? "label" : "Id"
    const filterStr = (job.data.Id == null) ? job.data.label : job.data.Id
    //can return one of three options - {up:false, Id: null}, {up:false, Id: id}, {up:true, Id: id}
    getContainers(filterStr, {by:filterType})
    .then(
      (container)=>{
        return new Promise((resolve,reject)=>{
          if(container){
            const upRegex = /^Up .*$/
            var containerStatus = container.Status || ""
            jobResponse.containerId = container.Id   // container exists, set Id
            job.log(`container ${friendlyName} exists`, {echo:true})
            if(upRegex.test(container.Status) || container.State.Running){
              job.log(`container ${friendlyName} is UP`, {echo:true})
              jobResponse.up = true // container is up
              // keep promise chain going to inspect running container
              resolve(docker.getContainer(jobResponse.containerId))
            } else {
            // send to catch cause there is nothing to inspect
             job.log(`response: ${jobResponse}`, {echo:true}) 
             resolve(docker.getContainer(jobResponse.containerId))
           // job.done(jobResponse)         // job done
            }
          } else {
            //send to catch cause there is nothing to inspect
            job.log(`response: ${jobResponse}`, {echo:true}) 
            reject(jobResponse)
          }
      })
    })      
    .then(
      (cont)=>{
        job.log(`getting container details`, {echo:true})
        cont.inspect()
        .then(
          (data)=>{
          jobResponse.containerId = data.Id
          jobResponse.details = {
            State: data.State ,
            IP: data.NetworkSettings.IPAddress,
            Ports: Object.keys(data.Config.ExposedPorts),
            Env: createEnvObj(data.Config.Env) // created k:v for envionrment variables instead of ["V=X"]
          }
          job.log(`job completed`, {echo:true})
          job.done(jobResponse)         // job done
        })
        .catch((err)=>{
          job.log(`JOB ERR: ${err}`, {level:"warning", echo:true});
          job.fail(err);
        })
    })
    .catch(
      (downResponse)=>{
        job.log(`job completed`, {echo:true})
        job.done(downResponse)         // job done. container exists, but is not up...
    })
}

function createCJob(job){
 docker.pull(job.data.config.Image)
 .then((stream)=>{
    // just care that stream completed before resolving...
    return streamToPromise(stream)
 })
 .then(
  (result)=>{
    job.log(`pulled container image: ${job.data.config.Image}`, {echo:true});
    docker.createContainer(job.data.config)
    .then(
      (container)=>{
      job.log(`created container: ${container.id}`, {echo:true});
      job.done({up: false, Id: container.id})
    })
    .catch((err)=>{
        job.log(`JOB ERR: ${err}`, {level:"warning", echo:true});
        job.fail(err);
    })
 });
}

function startCJob(job){
 var container = docker.getContainer(job.data.containerId);
  job.log(`starting container: ${container.id}`, {echo:true});
  container.start()
  .then(
    ()=>{
      job.log(`inspecting container: ${container.id}`, {echo:true});
      container.inspect()
      .then(
        (info)=>{
          var resp = {
            State: info.State,
            IP: info.NetworkSettings.IPAddress,
            Ports: Object.keys(info.Config.ExposedPorts),
            Env: createEnvObj(info.Config.Env) // created k:v for envionrment variables instead of ["V=X"]
          }
          console.log("jobs DONE")
          job.done({up: true, Id: info.id, details: resp });
        }).catch((err)=>{
          job.log(`JOB ERR: ${err}`, {level:"warning", echo:true});
          job.fail(err);
          })
      })
}

function stopCJob(job){
 var container = docker.getContainer(job.data.containerId)
  container.stop()
  .then(
    ()=>{
      job.log(`stopped container: ${container.id}`, {echo:true});
        job.done({up: false, Id: container.id});
  })
  .catch((err)=>{
  job.log(`JOB ERR: ${err}`, {level:"warning", echo:true});
  job.fail(err);
})
}

function removeCJob(job){
 var container = docker.getContainer(job.data.containerId);
 job.log(`removing container: ${container.id}`, {echo:true});
 container.remove()
 .then(
    ()=>{
      job.log(`removed container: ${container.id}`, {echo:true});
      job.done({up: false, Id: null});
    })
    .catch((err)=>{
  job.log(`JOB ERR: ${err}`, {level:"warning", echo:true});
  job.fail(err);
})
}

function restartCJob(job){
 var container = docker.getContainer(job.data.containerId);
  container.restart()
  .then(
    ()=>{
      job.log(`restarted container: ${container.id}`, {echo:true});
      job.done();
    }).catch((err)=>{
  job.log(`JOB ERR: ${err}`, {level:"warning", echo:true});
  job.fail(err);
})
}

/////////////////////////////////////////////
// Actual Job that gets run
////////////////////////////////////////////////////////////////////////////
function mainJob (job, cb) { 
  const jobType = job.data.type
  //job.done is run within each *CJob function
  if (jobType == "check") {     // check container by label OR Id
    checkCJob(job);
  } else if (jobType == "create") { // create container from Config
    createCJob(job);
  } else if (jobType == "start") { // stop container by ID
    startCJob(job);
  } else if (jobType == "stop") { // stop container by ID
    stopCJob(job);
  } else if (jobType == "remove") { // remove container by ID
    removeCJob(job);
  } else if (jobType == "restart") { // restart container by ID
    restartCJob(job);
 }
  //callback run once after conditional is complete...
  cb()

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


