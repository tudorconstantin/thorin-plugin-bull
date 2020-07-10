'use strict';
const path = require('path'),
  fs = require('fs'),
  Queue = require('bull');
// initJobEntry = require('./lib/jobEntry');
/**
 * Created by Tudor on 09-Jul-2020.
 *
 * The Queue component is used to schedule various actions that will be executed at specific points in time.
 */
module.exports = function (thorin, opt, pluginName) {
  opt = thorin.util.extend({
    logger: pluginName || 'jobs',
    path: 'app/jobs',
    enabled: true,
    debug: true,
    redisUrl: 'redis://localhost:6379',
    defaultProcesses: 2,
    numProcesses: {
      helloWorld: 5,
    },
    jobs: {},
  }, opt);
  const pluginObj = {},
    async = thorin.util.async,
    REGISTERED_QUEUES = {};
  // JobEntry = initJobEntry(thorin, opt);

  /*
   * Register a new job.
   * */
  // pluginObj.addJob = function (jobName, _opt) {
  //   if (typeof REGISTERED_QUEUES[jobName] !== 'undefined') {
  //     throw new thorin.error('JOBS.EXISTS', 'The job ' + jobName + ' is already registered.');
  //   }
  //   let jobConfig = thorin.util.extend(opt[jobName] || {}, _opt);
  //   let jobObj = new JobEntry(jobName, jobConfig);
  //   REGISTERED_QUEUES[jobName] = jobObj;
  //   process.nextTick(() => {
  //     // at this point, check any previous timers.
  //     jobObj.start(undefined, true);
  //   });
  //   return jobObj;
  // }

  /*
   * Returns a specific job by its name.
   * */
  pluginObj.getQueue = function (queueName) {
    return REGISTERED_QUEUES[queueName] || null;
  }

  /*
   * Stops all jobs.
   * */
  pluginObj.stopJobs = function StopAllJobs(fn) {
    let calls = [];
    Object.keys(REGISTERED_QUEUES).forEach((name) => {
      calls.push(pluginObj.stopJob.bind(pluginObj, name));
    });
    async.series(calls, (e) => fn && fn(e));
  }
  /* Stops a single job. */
  pluginObj.stopJob = function StopJob(name, fn) {
    if (typeof REGISTERED_QUEUES[name] === 'undefined') return fn && fn();
    REGISTERED_QUEUES[name].stop(fn);
  }

  const dirWalk = function (dir) {
    let results = [];
    const list = fs.readdirSync(dir);
    list.forEach(function (file) {
      file = dir + '/' + file;
      const stat = fs.statSync(file);
      if (stat && stat.isDirectory()) {
        /* Recurse into a subdirectory */
        results = results.concat(dirWalk(file));
      } else {
        /* Is a file */
        results.push(file);
      }
    });
    return results;
  }
  /*
   * Setup the job plugin
   * */
  pluginObj.setup = function (done) {
    const SETUP_DIRECTORIES = ['app/jobs'];
    for (let i = 0; i < SETUP_DIRECTORIES.length; i++) {
      try {
        thorin.util.fs.ensureDirSync(path.normalize(thorin.root + '/' + SETUP_DIRECTORIES[i]));
        const processorFiles = dirWalk(path.normalize(thorin.root + '/' + SETUP_DIRECTORIES[i]));
        for (const fileName of processorFiles){
          const processorName = path.basename(fileName, path.extname(fileName));
          if(REGISTERED_QUEUES[processorName]){
            throw new Error(`Job ${REGISTERED_QUEUES[processorName]} already exists`);
          }
          REGISTERED_QUEUES[processorName] = new Queue(processorName, opt.redisUrl);
          REGISTERED_QUEUES[processorName].process(opt.numProcesses[processorName] || opt.defaultProcesses, fileName);
        }

      } catch (e) {
      }
    }
    done();
  };

  /*
   * Run the job plugin, loading up all jobs.
   * */
  pluginObj.run = function (done) {
    if (!opt.enabled) return done();
    // thorin.loadPath(opt.path);
    pluginObj.setup(() => { });
    done();
  }


  /* Export the Job class */
  pluginObj.Queue = Queue;
  pluginObj.options = opt;
  return pluginObj;
};
module.exports.publicName = 'jobs';