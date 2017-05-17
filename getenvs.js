module.exports = (function(ns) {

  var appConfigs = require('./appConfigs.js');
  var secrets = require ('./secrets');
  var redis = require ('ioredis');
    
  ns.init = function() {
    appConfigs.load({
      PORT: process.env.PORT || 8080,
      IP: process.env.IP || "0.0.0.0"
    });

    // need env variables
    return {
      redisPort: appConfigs.get("REDIS_PORT"),
      redisIp: appConfigs.get("REDIS_IP"),
      redisPass: appConfigs.get("REDIS_PASS"),
      effexMasterSeed: appConfigs.get("EFFEX_MASTER_SEED"),
      effexAlgo: appConfigs.get("EFFEX_ALGO"),
      expressPort: appConfigs.get("PORT"),
      expressHost: appConfigs.get("IP"),
      adminKey: appConfigs.get("EFFEX_ADMIN"),
      socketPort:appConfigs.get("SOCKET_PORT"),
      socketPassTimeout:appConfigs.get("SOCKET_PASS_TIMEOUT"),
      socketPass:appConfigs.get("SOCKET_PASS")
    };
  };
  
  /**
   * attach a handle to a db
   * @param {string} db name
   * @param {object} env my env object
   * @return {object} a redis handle
   */
  ns.redisConf = function (db,env) {

    var p = {
      db: secrets.db[db],
      password: env.redisPass,
      port: env.redisPort,
      host: env.redisIp,
      showFriendlyErrorStack: true
    };

    try {
      var red = new redis(p);
      red.on("error", function(error) {
        console.log('failed to open redis', error);
        process.exit(2);
      });
      return red;
    }
    catch (err) {
      console.log('failed to get redis handle', err);
      process.exit(1);
    }
  };

  return ns;
})({});
