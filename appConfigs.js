/*
 * use nconf to keep config files in
 */

var nconf = require('nconf');
var AppConfigs = (function(ns) {

    ns.load = function (appDefaults) {
      nconf.defaults(appDefaults)
        .file({ file: 'config.json' })
        .env()
        .load();
      return nconf;        
    };
    
    ns.get = function (name) {
      return nconf.get(name);
    };
    
    ns.getConf = function () {
      return nconf;
    };
    return ns;
})({});

module.exports = AppConfigs;