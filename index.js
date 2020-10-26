'use strict';
const path = require('path'),
  fs = require('fs'),
  Queue = require('bull');
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
    redis: {
      prefix: '{jobs}',
      clustered: false,
      host: 'localhost',
      port: 6379,
      password: null,
    },
    defaultProcesses: 2,
    jobsProcesses: {
      helloWorld: 5,
    },
  }, opt);

  let erroredQueue = {
    name: '',
    err: null,
  };
  process.on('unhandledRejection', (reason, p) => {
    const logger = thorin.logger(erroredQueue.name, 'unnamedQueue');
    if(opt.required){
      logger.error(`Exiting: ${reason}`);
      return thorin.exit();
    }
  });
  const pluginObj = {},
    async = thorin.util.async,
    REGISTERED_QUEUES = {};

  /*
   * Returns a specific job by its name.
   * */
  pluginObj.getQueue = function (queueName) {
    return REGISTERED_QUEUES[queueName] || null;
  }

  /*
   * Returns an array containing all queue names ()
   * */
  pluginObj.getQueueNames = function () {
    return Object.keys(REGISTERED_QUEUES);
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

  const _getConnOpts = function (redisOpts) {

    if (!redisOpts.clustered) {
      return {
        redis: {
          host: redisOpts.host || 'localhost',
          port: redisOpts.port || 6379,
          password: redisOpts.password || null,
        },
        prefix: redisOpts.prefix || '{worker}',
      }
    }

    const Redis = require('ioredis');
    const client = new Redis.cluster([{ host: redisOpts.host }], { dnsLookup: (address, callback) => callback(null, address) });
    const subscriber = new Redis.cluster([{ host: redisOpts.host }], { dnsLookup: (address, callback) => callback(null, address) });
    let opts = {
      createClient: function (type) {
        switch (type) {
          case 'client':
            return client;
          case 'subscriber':
            return subscriber;
          default:
            return new Redis.cluster([{ host: redisOpts.host }], { dnsLookup: (address, callback) => callback(null, address) });
        }
      }
    }
    return opts;

  }
  /*
   * Setup the job plugin
   * */
  pluginObj.setup = function (done) {
    const SETUP_DIRECTORIES = ['app/jobs'];
    try {
      for (let i = 0; i < SETUP_DIRECTORIES.length; i++) {
        thorin.util.fs.ensureDirSync(path.normalize(thorin.root + '/' + SETUP_DIRECTORIES[i]));
        const processorFiles = dirWalk(path.normalize(thorin.root + '/' + SETUP_DIRECTORIES[i]));
        for (const fileName of processorFiles) {
          const processorName = path.basename(fileName, path.extname(fileName));
          if (REGISTERED_QUEUES[processorName]) {
            throw new Error(`Job ${REGISTERED_QUEUES[processorName]} already exists`);
          }

          REGISTERED_QUEUES[processorName] = new Queue(processorName, _getConnOpts(opt.redis));
          REGISTERED_QUEUES[processorName]
            .on('error', function (error) {
              const logger = thorin.logger(this.name);
              logger.warn(`Error in jobQueue: ${error.message} (${process.pid})`);
              erroredQueue.name=this.name;
              erroredQueue.err = error;
              return Promise.reject(error);
            });
          REGISTERED_QUEUES[processorName].process(opt.jobsProcesses[processorName] || opt.defaultProcesses, fileName);
        }
      }
      return done();
    } catch (e) {
      return done(e);
    }
  };

  /*
   * Run the job plugin, loading up all jobs.
   * */
  pluginObj.run = function (done) {
    if (!opt.enabled) return done();
    pluginObj.setup(function (err) {
      if (err) {
        const logger = thorin.logger('thorin-plugin-bull');
        logger.warn(`Could not initiate redis connection`, err);
        if (opt.required) {
          thorin.exit();
        }
      }
      done();
    });
  }


  /* Export the Job class */
  pluginObj.Queue = Queue;
  pluginObj.options = opt;
  return pluginObj;
};
module.exports.publicName = 'jobs';

