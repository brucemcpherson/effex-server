/**
 * does the main stuff for the app
 */
var appConfigs = require('./appConfigs.js');
var RateManager = require('./ratemanager.js');
var axios = require ('axios');

var Process = (function(ns) {
  var env_;
  var crypto = require('crypto');
  var coupon_, redisAlias_, redisClient_,
    redis, lucky_, redisRate_, redisStats_, redisBosses_,
    redisAccounts_, redisIntent_, redisWatchable_,
    redisApp_, redisSubscribe_, redisLog_, redisWatchLog_;

  // used to keep track of sessions looking for push notifications
  ns.sessions = {};

  // app options
  ns.settings = {
    watchable: {
      events: {
        "expired": "expire",
        "del": "remove",
        "set": "update"
      }
    },
    errors: {
      NOT_FOUND: 404,
      BAD_REQUEST: 400,
      UNAUTHORIZED: 401,
      FORBIDDEN: 403,
      CONFLICT: 409,
      INTERNAL: 500,
      OK: 200,
      CREATED: 201,
      ACCEPTED: 202,
      NO_CONTENT: 204
    },
    rateManagers: {},
    keyLength: 14,
    allowAccessorChanges: false,
    days: 28,
    accountPrefix: "ac-",
    itemPrefix: "it-",
    statPrefix: "sw-",
    aliasPrefix: "ap-",
    intentPrefix: "in-",
    watchablePrefix: "wt-",
    logPrefix: "lg-",
    watchLogPrefix: "wl-",
    logLifetime: 60 * 60 * 24, // last for a day
    watchLogLifetime: 60 * 60 * 28, // last for a day and a bit
    intentLifetime: 20000, // intents last for 20 seconds
    db: {
      app: 1,
      client: 0,
      stats: 3,
      rate: 2,
      bosses: 4,
      accounts: 5,
      subscribe: 0
    },
    statsSeconds: 15 * 60 // samples are every 15 mins
  };

  ns.registerBoss = function(pack) {

    var exp = new Date(pack.validtill).getTime() - new Date().getTime();
    return redisBosses_.set(pack.key, pack.accountId, "EX", exp > 0 ? 10 + Math.round(exp / 1000) : 10)
      .then(function(result) {
        return pack;
      });

  };


  /**
   * get any boss keys belonging to a particular account
   */
  ns.getBosses = function(req) {

    // unpack the params
    var params = paramSquash_(req);

    // set up default response
    var pack = ns.errify(true, ns.settings.errors.OK, 'boss keys', {
      ok: true,
      accountId: params.accountid,
      keys: []
    });

    // check we have the account id
    ns.errify(pack.accountId, ns.settings.errors.BAD_REQUEST, "accountid required", pack);
    pack = ns.checkAdmin(params.admin, pack);
    if (!pack.ok) {
      return Promise.resolve(pack);
    }
    // get any matching bosses
    return redisBosses_.keys("b*" + pack.accountId + "-*")
      .then(function(result) {
        pack.keys = result;
        // generate coupons for each
        pack.coupons = pack.keys.map(function(d) {
          return ns.getCouponPack(d, {});
        });
        return pack;
      });

  };

  /**
   * remove boss keys
   */
  ns.removeBosses = function(req) {

    // unpack the params
    var params = paramSquash_(req);
    var keys = params.data.keys;

    // set up default response
    var pack = ns.errify(true, ns.settings.errors.OK, 'boss keys', {
      ok: true,
      accountId: params.accountid,
      keys: keys
    });

    ns.errify(keys, ns.settings.errors.BAD_REQUEST, "no keys supplied to delete", pack);
    pack = ns.checkAdmin(params.admin, pack);
    if (!pack.ok) {
      return Promise.resolve(pack);
    }
    if (!Array.isArray(keys)) {
      pack.keys = [keys];
    }

    return redisBosses_.del(pack.keys)
      .then(function(result) {
        return ns.errify(result === pack.keys.length, ns.settings.errors.NOT_FOUND, "only found " + result + " keys to delete", pack);
      });


  };


  /**
   * prune any boss keys belonging to a particular account
   */
  ns.pruneBosses = function(req) {

    return ns.getBosses(req)
      .then(function(pack) {
        if (pack.ok && pack.keys.length) {
          return redisBosses_.del(pack.keys)
            .then(function(result) {
              return ns.errify(result === pack.keys.length, ns.settings.errors.INTERNAL, "not all bosses deleted", pack);
            });
        }
        else {
          return pack;
        }
      });

  };

  /**
   * delete an account
   */
  ns.deleteAccount = function(req) {
    var params = paramSquash_(req);
    var pack = ns.errify(true, ns.settings.errors.OK, 'deleted', {
      ok: true,
      accountId: params.accountid
    });
    ns.errify(pack.accountId, ns.settings.errors.BAD_REQUEST, "accountid required", pack);
    pack = ns.checkAdmin(params.admin, pack);

    if (pack.ok) {
      return redisAccounts_.del(pack.accountId)
        .then(function(result) {
          pack = ns.errify(result === 1, ns.settings.errors.NOT_FOUND, "failed deleting account", pack);
          return pack.ok ? ns.pruneBosses(req) : pack;
        });

    }
    else {
      return Promise.resolve(pack);
    }

  };


  /**
   * check an boss key exists
   */
  ns.checkBoss = function(pack, bosskey) {

      if (pack.ok) {
        return redisBosses_.exists(bosskey)
          .then(function(result) {
            return ns.errify(result, ns.settings.errors.NOT_FOUND, "bosskey " + bosskey + " doesn't exist", pack);
          });
      }
      else {
        return Promise.resolve(pack);
      }
    }
    /**
     * check an account exists
     * @param {string} [apikey] if specified, then make sure it matches the uid in the account
     */
  ns.checkAccount = function(pack, apikey) {

    if (pack.ok) {
      return ns.getAccount(pack.accountId)
        .then(function(result) {
          ns.errify(result, ns.settings.errors.NOT_FOUND, "account doesn't exist", pack);
          if (pack.ok) {
            var ob = JSON.parse(result);
            ns.errify(ob.active, ns.settings.errors.NOT_FOUND, "accout is not active", pack);
            ns.errify(!apikey || apikey === ob.authid, ns.settings.errors.FORBIDDEN, "apikey not valid for this account", pack);
            if (!pack.ok) {
              pack.key = "";
              pack.validtill = "";
            }
          }
          return pack;
        });
    }
    else {
      return Promise.resolve(pack);
    }
  };

  ns.checkAdmin = function(key, pack) {
    return ns.errify(key === env_.adminKey, ns.settings.errors.UNAUTHORIZED, "You need to provide an admin key for this operation", pack || {
      ok: true
    });
  };

  /**
   * register an account in redis
   */
  ns.registerAccount = function(req) {
    var params = paramSquash_(req);
    params.data = params.data || {};

    var pack = ns.errify(true, ns.settings.errors.CREATED, 'created', {
      ok: true,
      accountId: params.accountid,
      authId: params.data.authid,
      modified: new Date().getTime(),
      active: params.data.active
    });

    ns.errify(pack.authId, ns.settings.errors.BAD_REQUEST, "authid required", pack);
    ns.errify(pack.accountId, ns.settings.errors.BAD_REQUEST, "accountid required", pack);
    ns.checkAdmin(params.admin, pack);

    if (!pack.ok) {
      return Promise.resolve(pack);
    }

    return redisAccounts_
      .set(pack.accountId, encryptText_(params.data, env_.effexMasterSeed + ns.settings.accountSeed + pack.accountId))
      .then(function(result) {
        return pack;
      })
      .catch(function(err) {
        return ns.errify(false, ns.settings.errors.INTERNAL, err, pack);
      });
  };

  /**
   * get the stats for an account
   */
  ns.statsGet = function(req) {
    // the keys to write this against
    var params = ns.squashParams(req);
    var pack = ns.checkAdmin(params.admin);
    if (!pack.ok) {
      return Promise.resolve(pack);
    }
    var accountId = params.accountid;
    var start = params.start;
    var finish = params.finish;
    var key = ns.settings.statPrefix + (accountId || "*") + "*";
    return redisStats_.keys(key)
      .then(function(keys) {
        // all the matching keys for the acccountid/ or all of them
        // get them all at once
        return Promise.all(keys.map(function(d) {
            // this is a specific key, so return the hash but modified
            return redisStats_.hgetall(d)
              .then(function(item) {
                item.accountId = accountId;
                return Object.keys(item)
                  .reduce(function(p, c) {
                    p[c] = parseInt(item[c], 10);
                    if (isNaN(p[c])) {
                      p[c] = item[c];
                    }
                    return p;
                  }, {});
              });
          }))
          .then(function(pr) {
            return ns.errify(true, ns.settings.errors.OK, "", {
              chunks: pr.filter(function(d) {
                return d.start >= start && d.start <= finish;
              }),
              ok: true,
              accountId: accountId,
              start: start,
              finish: finish
            });
          });
      });

  };

  /**
   * create stats window an write
   */
  ns.statify = function(accountId, couponCode, type, volume) {

    //the stats measurement window
    var w = Math.floor(new Date().getTime() / (ns.settings.statsSeconds * 1000));

    // the keys to write this against
    var shuffly = lucky_.shuffle(couponCode, accountId);
    var key = ns.settings.statPrefix + accountId + "-" + shuffly + "-" + w;

    // update the stats for this window
    return Promise.all([
      redisStats_.hmset(key, "seconds", ns.settings.statsSeconds,
        "start", ns.settings.statsSeconds * 1000 * w, "coupon", couponCode, "accountId", accountId),
      redisStats_.hincrby(key, type, 1),
      volume ? redisStats_.hincrby(key, type + "size", volume) : Promise.resolve()
    ]);

  };

  /**
   * convert a public key to a private
   */
  function getAccountPrivateKey_(publicKey) {
    return ns.settings.accountPrefix + publicKey;
  }

  /**
   * get a given account
   */
  ns.getAccount = function(accountId) {



    return redisAccounts_.get(accountId)
      .then(function(result) {
        return Promise.resolve(result ? decryptText_(result, env_.effexMasterSeed + ns.settings.accountSeed + accountId) : result);
      })
      .catch(function(err) {
        return ns.errify(false, ns.settings.errors.INTERNAL, err, {
          accountId: accountId
        });
      });
  };


  /*
   * get a unique key
   * the public key is a shuffled version of the private key
   * that needs a seed plus the accountid to reconstitute
   * @param {object} pack the request pack
   * @return {object} the string priv & pub
   */
  ns.getNewKey = function(pack) {

    var coupon = ns.makeCoupon({
      accountid: pack.accountId,
      plan: pack.plan,
      type: 'item',
      seconds: pack.lifetime
    });


    ns.errify(coupon && coupon.ok, ns.settings.errors.INTERNAL, "failed to generate item id", pack);
    var publicKey = coupon.code || "";
    return {
      private: ns.getPrivateKey(pack.accountId, publicKey),
      public: publicKey
    };
  };

  /*
   * convert public to private
   * @param {string} accountId
   * @param {string} publicKey the string public
   * @return {string} the private
   */
  ns.getPrivateKey = function(accountId, public) {
    return ns.settings.itemPrefix + "-" + coupon_.sign(public, env_.effexMasterSeed + ns.settings.itemSeed).toString('base64EncodeWebSafe');
  };


  /**
   * needs to be called before anything works
   */
  ns.init = function() {

    appConfigs.load({
      PORT: process.env.PORT || 8080,
      IP: process.env.IP || "0.0.0.0"
    });
    
    // get the secrets from config file
    var secrets = appConfigs.get("secrets");

    // move them into the settings
    Object.keys(secrets).forEach (function (d) {
      ns.settings[d] = secrets[d];
    });
    
    
    // need env variables
    Process.env = {
      redisPort: appConfigs.get("REDIS_PORT"),
      redisIp: appConfigs.get("REDIS_IP"),
      redisPass: appConfigs.get("REDIS_PASS"),
      effexMasterSeed: appConfigs.get("EFFEX_MASTER_SEED"),
      effexAlgo: appConfigs.get("EFFEX_ALGO"),
      expressPort: appConfigs.get("PORT"),
      expressHost: appConfigs.get("IP"),
      adminKey: appConfigs.get("EFFEX_ADMIN")
    };
    env_ = Process.env;

    // check we got them all
    if (Object.keys(env_).some(function(d) {
        return !env_[d];
      })) {
      console.log('missing required env variables', env_);
      process.exit(1);
    }

    console.log('looking for redis at:', env_.redisIp, ":", env_.redisPort);
    console.log('running on:', env_.expressHost, ":", env_.expressPort);

    // need stuff for decoding coupons (apikeys)
    var c = require("./coupon.js");
    coupon_ = new c(env_.effexAlgo);

    lucky_ = require("./lucky.js");

    // and for connecting to redis
    redis = require('ioredis');


    // set up the base connecting object
    function redisConf_(db) {
      var p = {
        db: ns.settings.db[db],
        password: env_.redisPass,
        port: env_.redisPort,
        host: env_.redisIp,
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
    }

    redisRate_ = redisConf_('rate');
    redisStats_ = redisConf_('stats');
    redisBosses_ = redisConf_('bosses');
    redisAccounts_ = redisConf_('accounts');
    redisApp_ = redisConf_('app');
    redisClient_ = redisConf_('client');
    redisSubscribe_ = redisConf_('subscribe');

    // just put support items in the same db
    redisIntent_ = redisApp_;
    redisWatchable_ = redisApp_;
    redisAlias_ = redisApp_;
    redisLog_ = redisApp_;
    redisWatchLog_ = redisRate_;
    
    // set up keyspace event monitoring - watching out for del, expired and set
    redisSubscribe_.config('set', 'notify-keyspace-events', 'Exg$')
      .then(function() {
        // subscribe to  datbase changes
        return redisSubscribe_.psubscribe('__keyevent@' + ns.settings.db.client + '__:*');
      })
      .then(function() {
        console.log('subscribed');
        redisSubscribe_.on('pmessage', function(pattern, channel, message) {
          var method = channel.replace(/.*__:/, '');
          // if its one we care about
          if (ns.settings.watchable.events[method]) {
            // the default 'who' is system reported - redis
            ns.logEvent(method, message, "redis");
          }
        });
      });


    // need a rate manager for each possible plan

    var rm = RateManager;
    ns.settings.rateManagers = Object.keys(ns.settings.plans)
      .reduce(function(p, c) {
        p[c] = new rm(ns.settings.plans[c].limiters, redisRate_);
        return p;
      }, {});

    return ns;
  };

  /**
   * this removes watching
   * @param {object} params the parameters
   */
  ns.unWatchUrlCallback = function (params) {
    
    // first check that we have a valid watchable
    var wPack = ns.getCouponPack (params.watchable, params);
    if (!wPack.ok) return Promise.resolve (wPack);
   
    var touched = Object.keys(ns.sessions)
      .filter (function (d) {
        // find all the watchers that have this key
        return Object.keys(ns.sessions[d].watchables)
          .some (function (e) {
            return e === params.watchable;
          });
      })
      .map (function(d) {
        if (Object.keys(ns.sessions[d].watchables).length === 1 ) {
          // its the only one so delete it
          delete ns.sessions[d].watchables;
        }
        else {
          // just delete this watchable
          delete ns.sessions[d].watchables[params.watchable];
        }
        return d;
      });
    
    wPack.code = ns.settings.errors.NO_CONTENT;
    ns.errify(touched.length,ns.settings.errors.NOT_FOUND,"no matching watchables active",wPack);
    return Promise.resolve (wPack);
  };

  ns.watchUrlCallback = function (params) {
    
    // first check that we have a valid watchable
    var wPack = ns.getCouponPack (params.watchable, params);
    if (!wPack.ok) return Promise.resolve (wPack);
   
    // now set up the socket key
    ns.errify ( params.data.url && params.data.method , ns.settings.errors.BAD_REQUEST , "url and method needed", wPack);
    if (!wPack.ok) return Promise.resolve (wPack);
    
     
    // maybe first time for this combinatin of url/method   
    var sessionKey = params.data.method + "-" + params.data.url;
    if (!ns.sessions[sessionKey]) {
      ns.sessions[sessionKey] = {
        url:params.data.url,
        method:params.data.method,
        watchables:{}
      };
    }

    // set up this specific watchable against this url          
    ns.sessions[sessionKey].watchables[params.watchable] = {
      session:params.session,
      created:new Date().getTime(),
      nextEvent:params.data.nextEvent || new Date().getTime(),
      message:params.data.message || "",
      stopped:false
    }; 
   
    return Promise.resolve (wPack);
  };

    
  /**
   * emits any outstanding events
   * @param {string} sxKey the provate encoded data item key
   * @param {number} logTime the official time of the event
   */
  ns.emitter = function(sxKey,logTime) {

    // for a particular sx, emit any outstanding events
    var wKey = ns.settings.watchablePrefix + sxKey + "*";
    var wob,wall;
    
    // get any fully matching lkeys
    return redisWatchable_.keys(wKey)
      .then(function(w) {
        wall = w[0];
        return wall ? redisWatchable_.get(wall) : null;
      })
      .then(function(result) {
        // i suppose this is possible if the item expires just here, but its not an emergency
        if (!result) {
          console.log('failed getting watchable for emit data', wall);
          return Promise.resolve(null);
        }
        else {
          // this is the watch item details
          wob = obify_(result, {});

          // this is item's the private key
          var s = wall.split(".");
          var privateKey = s[1];
          var method = s[2];

          // now we need the zvalues for this item
          var lkey = ns.settings.logPrefix + privateKey + "." + method;
          return redisLog_.zrangebyscore(lkey, 0, Infinity);
        }
      })
      .then(function(r) {

        // check which sessions care about this item
        if (!r)return Promise.resolve (null);
        var zValues = r.map(function(d) {
          return parseInt(d, 10);
        });

        Object.keys(ns.sessions).forEach(function(d) {
          var session = ns.sessions[d];
          var wat = session && session.watchables[sxKey];

          // if this session cares, it will have a watch key
          if (wat) {
            
            // filter out the emits according to the last event they saw
            var emits = zValues.filter(function(f) {
              return f >= wat.nextEvent;
            });
            
            // send it
            if (emits.length) {
              
              var packet = {
                id:wob.id,
                alias:wob.alias || "",
                value:emits,
                key:sxKey,
                event:wob.event,
                session:wat.session,
                message:wat.message || ""
              };
              
              var watchLog = {
                id:packet.id,
                alias:packet.alias,
                key:packet.key,
                event:packet.event,
                session:packet.session,
                state:"pending"
              };
              
              if (session.socket) {
                // doing push
                watchLog.type = "push";
                session.socket.emit(sxKey, packet);
                watchLog.state = "emitted";
                ns.logWatchLog(sxKey, wob.key , logTime, watchLog);
                
              }
              
              else if (session.url) { 
                // doing url push
                var url = session.url + (session.url.indexOf("?") !== -1 ? "&" : "?") + "watchable=" + sxKey;
                var data = session.method === "POST" || session.method === "PUT" || session.method === "PATCH" ? packet : null;
                if (!data) url += ("&=" + encodeURIComponent (JSON.stringify(packet)));
                var ac = {
                  method: session.method,
                  url: url
                };
                if (data) ac.data = data;
                watchLog.type="url";
                watchLog.method=ac.method;
                watchLog.url=url;
                
                axios (ac)
                .then (function (result) {
                  watchLog.state = "emitted";
                  ns.logWatchLog(sxKey, wob.key , logTime, watchLog);
                })
                .catch(function(err) {
                  watchLog.state="failed";
                  watchLog.error = err.response.statusText;
                  watchLog.code=err.response.status;
                  ns.logWatchLog(sxKey, wob.key , logTime, watchLog);
                });
                
              }
              else {
                console.log ('invalid emit instructions', wat);
                watchLog.state = "failed";
                watchLog.error = "invalid emit instructions";
                watchLog.code = 500;
                ns.logWatchLog(sxKey, wob.key , logTime, watchLog);
              }
              wat.nextEvent = emits[emits.length - 1] + 1;

            }
          }
        });

      })
      .catch(function(err) {
        // just log
        console.log(err, " failure servicing emitter for ", sxKey);
      });


  };
  
 /**
   * watchlog .. useful for finding errors and tracking
   * @param {string} sxKey the watch key
   * @param {string} accessKey the guy who wrote it
   * @param {number} [since=0] the start time 
   * @return {Promise} the result
   */
  ns.getWatchLog = function(sxKey , accessKey, since) {
    since = since || 0;

    var wkey = ns.settings.watchLogPrefix + [sxKey, accessKey].join(".") + "*";

    // get all the logs for this key
    return redisWatchLog_.keys(wkey)
      .then (function(keys) {
      
        return Promise.all (keys.map(function (key) {
          return redisWatchLog_.get (key)
            .then ( function (r) { return JSON.parse (r); });
        }));
      })
      .then (function (results){
        var f = results.filter (function (d) {
          return d.logTime >= since;    
        });
        return f;
      })
      .then (function (results) {
        return {
          ok: results.length ? true : false,
          code: results.length ? 200 : 404,
          error: results.length ? "" : "no log watch items found",
          value:results,
          watchable: sxKey,
          reader: accessKey,
          watchableState: ns.getCouponPack(sxKey, {}).error,
          accessKeyState: ns.getCouponPack(accessKey, {}).error
        };
      });
  };

 /**
   * watchlog .. useful for finding errors and tracking
   * @param {string} sxKey the watch key
   * @param {string} accessKey the guy who wrote it
   * @param {number} logTime when it happemed
   * @param {object} packet the data to log
   * @return {Promise} the result
   */
  ns.logWatchLog = function(sxKey , accessKey, logTime, packet) {
    // expiry times for watchlog are fixed
    // a watch packet
    // key - the sxkey + the key that created it (for security) + the of the event + some random to avoid clashes at same ms.
    var key = ns.settings.watchLogPrefix + [sxKey, accessKey,logTime,Math.round(Math.random()*1000)].join(".");
    return redisWatchLog_.set (key , JSON.stringify({packet:packet, logTime:logTime}), "EX", ns.settings.watchLogLifetime);
  };
  /**
   * logs an event against a data item
   * @param {string} method the method reported by redis (set/del/expired)
   * @param {string} key the provate encoded data item key
   * @param {string} who which session is provoking the event.. not implemented for now - maybe later
   * @param {string} [logTime=now] the logtime if required
   */
  ns.logEvent = function(method, key, who, logTime) {
    // expiry times for log event are not related to data lifetime
    // they are the maximum subscription history time

    logTime = logTime || new Date().getTime();
    
    // log events are keyed  by their private item key
    var lkey = ns.settings.logPrefix + key + "." + method;

    // kick off the search for anybody watching this - its key will contain the item's private key
    var wKey = ns.settings.watchablePrefix + "*" + key + "*" + method;

    // add an observation for this log entry
    var zadd = redisLog_.zadd(lkey, logTime, logTime)
      .then(function(result) {
        // set an expire time some standard amount from now .. dont care to track this
        Promise.all ( [redisWatchable_.keys (wKey) ,redisLog_.expire(lkey, ns.settings.logLifetime)])
        .then (function (pt) {
          var keys = pt[0];
          
          keys.forEach (function (k) {
            var sxKey = k.split(".")[0].replace(ns.settings.watchablePrefix,"");
            ns.emitter (sxKey,logTime);
          });
        });
      });


    return zadd;
  };

  /**
   * set a value
   * @param {string} key the key
   * @param {string} value the value
   * @param {number} lifetime the expiry time
   * @return {Promise}
   */
  ns.write = function(key, value, lifetime) {
    var text = encryptText_(value, key);
    return lifetime ? redisClient_.set(key, text, "EX", lifetime) : redisClient_.set(key, text);
  };

  /**
   * ping the service
   * @return {Promise}
   */
  ns.ping = function() {
    return redisClient_.ping()
      .then(function(result) {
        return {
          ok: true,
          value: result,
          code: ns.settings.errors.OK
        };
      })
      .catch(function(err) {
        return ns.errify(false, 503, err, {});
      });
  };

  /**
   * return the quotas
   * @return {Promise}
   */
  ns.getQuotas = function() {
    return Promise.resolve({
      quotas: ns.settings.plans,
      ok: true,
      code: ns.settings.errors.OK
    });
  };

  /**
   * create an intent
   * @param {object} pack the pack so far
   * @return {promise}
   */
  ns.createIntent = function(pack) {

    if (pack.intention) {
      ns.errify(pack.intention === "update", ns.settings.errors.BAD_REQUEST, "invalid intention parameter " + pack.intention, pack);
      if (pack.ok) {
        // the key is the item id - there can only be one intent per id 
        // the body is the intent authorization + the reader key (which will later be used for checking)
        // intents are free from a quota point of view hence no statify

        // need to generate a coupon
        var seed = ns.settings.seeds.filter(function(d) {
          return d.type === "intent" && d.plan === pack.plan;
        })[0];

        if (ns.errify(seed, ns.settings.errors.INTERNAL, "couldnt find an intent seed for the plan", pack).ok) {
          var auth = coupon_.generate(
            seed.value,
            new Date().getTime() + ns.settings.intentLifetime,
            seed.name,
            parseInt(pack.accountId, 32)
          );
          var key = ns.settings.intentPrefix + pack.id;
          // let it live for a few extra seconds, since its the coupon that will be used 
          // to check for expiration to avoid an unnecessary caceh access
          return redisIntent_.set(key, auth + "," + pack.reader, "EX", 10 + Math.round(ns.settings.intentLifetime / 1000))
            .then(function(result) {
              pack.intent = auth;
              pack.intentExpires = Math.round(ns.settings.intentLifetime / 1000);
              return pack;
            });

        }
      }
    }
    return Promise.resolve(pack);

  };

  /**
   * get items since last time
   * @param {object} params the params
   * @return {Promise} the result
   */
  ns.getWatched = function(params) {

    // this validates the watchable key
    var wPack = ns.getCouponPack(params.watchable, params);
    if (!wPack.ok) return Promise.resolve(wPack);
    var since = params.since || 0;

    // get whats being watched
    var key = ns.settings.watchablePrefix + wPack.key + ".*";
    return redisWatchable_.keys(key)
      .then(function(keys) {
        ns.errify(keys.length, ns.settings.errors.NOT_FOUND, "no keys found to match " + key, wPack);

        ns.errify(keys.length === 1, ns.settings.errors.INTERNAL, "ambigous watchable keys", wPack);
        if (wPack.ok) {
          return redisWatchable_.get(keys[0])
            .then(function(result) {
              // check we have it
              ns.errify(result, ns.settings.errors.INTERNAL, "missing watchable", wPack);
              return {
                pack: wPack,
                content: obify_ (result , {}) || {},
                key: keys[0]
              };
            })
            .then(function(result) {
              var pack = result.pack;
              pack.id = result.content.id;
              pack.alias = result.content.alias || "";
              pack.event = result.content.event;
              var key = result.key;
              if (!pack.ok) return pack;

              // now we have the watchable and all is ok - we can get the logs
              var s = key.split(".");
              var itemKey = s[1];
              var method = s[2];
              var logKey = ns.settings.logPrefix + itemKey + "." + method;
              return redisLog_.zrangebyscore(logKey, since, Infinity)
                .then(function(result) {

                  pack.value = (result || []).map(function(d) {
                    return parseInt(d, 10);
                  });
                  return pack;
                });
            });
        }
        else {
          return wPack;
        }
      })
      .catch(function(err) {
        ns.errify(false, ns.settings.errors.INTERNAL, err, wPack);
      });

  };



  /**
   * create a watchable 
   * @param {object} params the params
   * @return {promise}
   * /watch/key/item/event
   */
  ns.createWatchable = function(params) {

    // this validates the access key
    var keyPack = ns.getCouponPack(params.key, params);

    // and we'll use that to fetch the existing record (to make sure we can read it)
    keyPack.reader = keyPack.key;
    keyPack.id = params.id;

    if (!keyPack.ok) return Promise.resolve(keyPack);

    // the keys have been validated and the pack contains the item validation

    // need to ensure this guy can read the key
    return dealWithAlias_(keyPack, params)
      .then(function(pack) {
        return ns.get(pack, keyPack.key);
      })
      .then(function(pack) {
        if (!pack.ok) return Promise.resolve(pack);

        // need to generate a coupon
        var seed = ns.settings.seeds.filter(function(d) {
          return d.type === "watchable" && d.plan === pack.plan;
        })[0];

        if (!ns.errify(seed, ns.settings.errors.INTERNAL, "couldnt find a watchable seed for the plan", pack).ok) return Promise.resolve(pack);

        // watchables lifetime is based on the thing they are watching
        var ex = pack.alias ? new Date(keyPack.validtill) : new Date(pack.validtill);
        
        // just in case we have multiple watches on the same access key/item, we can let the watchable live for up to an hour extra
        var randomTime = Math.round(Math.random() * 360000);
        var vill = ex.getTime() + randomTime;
        
        // keep it in the store for a little extra to allow for any delays
        var life = Math.ceil((vill - new Date().getTime())/1000) + 30;  
        
        // now generate an id for this watchable
        pack.watchable = coupon_.generate(
          seed.value,
          vill,
          seed.name,
          parseInt(pack.accountId, 32)
        );

        // all this stuff needs to go in the key for searching on
        // fix this to translate to private key for item
        pack.event = params.event;

        // get the redis equivalent method (for example update (efx) === set (redis))
        var redisEvent = Object.keys(ns.settings.watchable.events).filter(function(k) {
          return ns.settings.watchable.events[k] === pack.event;
        })[0];
        ns.errify(redisEvent, ns.settings.errors.BAD_REQUEST, "invalid event", pack);
        if (!pack.ok) return Promise.resolve(pack);

        // generate the key that can be used when events happen to see if they are interesting
        var key = ns.settings.watchablePrefix + [pack.watchable, ns.getPrivateKey(pack.accountId, pack.id), redisEvent].join(".");
        
        // next we need to write the item to the watchables store
        return redisWatchable_.set(key, JSON.stringify({
            created: new Date().getTime(),
            key: pack.reader,
            alias: pack.alias,
            id: pack.id,
            event: pack.event
          }), "EX", life)
          .then(function(result) {
            pack.code = ns.settings.errors.CREATED;
            pack.error = '';
            // do a check to ensure that it was actually written
            return ns.errify(result, ns.settings.errors.INTERNAL, "couldnt write watchable", pack);
          });


      });


  };

  /**
   * remove a value if allowed
   * @param {object} pack the pack so far
   * @param {string} couponKey for stats
   * @return {Promise} the updated pack
   */
  ns.remove = function(pack, couponKey, params) {

    return ns.settings.rateManagers[pack.plan].getSlot(pack.accountId)
      .then(function(passed) {
        rlify_(passed, pack);
        if (pack.ok) {
          return dealWithAlias_(pack, params)
            .then(function(pack) {
              return ns.read(ns.getPrivateKey(pack.accountId, pack.id));
            })
            .then(function(result) {
              //parse it
              var ob = obify_(result, pack);
              ns.statify(pack.accountId, couponKey, "remove", 0);
              ns.errify(ob, ns.settings.errors.NOT_FOUND, "item cannot be removed as it does not exist", pack);
              // make sure we can touch it
              ns.errify(ob && ob.accountId && pack.accountId === ob.accountId, ns.settings.errors.INTERNAL, "account id mismatches key", pack);
              if (pack.ok && canWrite_(pack, ob) && ob.owner === pack.writer) {
                // all is good
                return ns.checkAccount(pack)
                  .then(function(result) {
                    return result.ok ?
                      ns.del(ns.getPrivateKey(pack.accountId, pack.id))
                      .then(function(dr) {
                        ns.errify(dr, ns.settings.errors.INTERNAL, "failed to delete item", pack);
                        if (pack.ok) {
                          pack.code = ns.settings.errors.NO_CONTENT;
                        }
                        return pack;
                      }) : result;
                  });
              }
              else if (ob) {

                ns.errify(false, ns.settings.errors.FORBIDDEN, "only the owner can remove an item", pack);

              }
              else {
                ns.errify(false, ns.settings.errors.NOT_FOUND, "unable to delete - item was missing", pack);
              }
              return pack;
            });
        }
        else {
          return pack;
        }
      })
      .catch(function(err) {
        return Promise.resolve(ns.errify(false, ns.settings.errors.INTERNAL, (err ? err.toString() : "") + ":caught an error:" + (pack.error || ""), pack));
      });
  };

  /**
   * get a value if allowed
   * @param {object} pack the pack so far
   * @return {Promise} the updated pack
   */
  ns.get = function(pack, couponKey) {

    pack.value = null;

    return ns.settings.rateManagers[pack.plan].getSlot(pack.accountId)
      .then(function(passed) {
        rlify_(passed, pack);
        if (pack.ok) {
          return ns.read(ns.getPrivateKey(pack.accountId, pack.id))
            .then(function(result) {

              //parse it
              var ob = obify_(result, pack);
              ns.statify(pack.accountId, couponKey, "get", result ? result.length : 0);

              // make sure we can touch it
              if (pack.ok && canRead_(pack, ob)) {
                // all is good
                pack.value = ob.value;
                pack.code = ns.settings.errors.OK;
                pack.modified = ob.modified;
                pack.session = ob.session;

                // create an intent if required
                return ns.createIntent(pack);
              }

              else if (ob) {
                ns.errify(false, ns.settings.errors.FORBIDDEN, "you are not allowed to read this data", pack);
                ns.errify(ob.accountId && pack.accountId === ob.accountId, ns.settings.errors.INTERNAL, "item account id mismatches key", pack);
              }
              else {
                ns.errify(false, ns.settings.errors.NOT_FOUND, "item is missing", pack);
              }
              return pack;
            });
        }
        else {
          return pack;
        }
      })
      .catch(function(err) {
        return Promise.resolve(ns.errify(false, ns.settings.errors.INTERNAL, (err ? err.toString() : "") + ":caught an error:" + (pack.error || ""), pack));
      });

  };

  /**
   * keep a running pack status
   */
  function rlify_(passed, pack) {
    if (pack.ok && !passed.ok) {
      pack.ratelimitFails = passed.failures;
      ns.errify(false, 429, "limit(s) exceeded", pack);
    }
    return pack;
  }

  /**
   * set a value if allowed
   * @param {object} pack the pack so far
   * @return {Promise} the updated pack
   */
  ns.set = function(pack, value, couponKey) {

    var now = new Date().getTime();
    
    // get the plan info
    var plan = ns.settings.plans[pack.plan];
    ns.errify(plan, ns.settings.errors.INTERNAL, "cant find plan info for plan:" + pack.plan, pack);
    // set the default lifetime
    if (pack.ok) {
      // the lifetime will be determined by either
      // - the life of the key creating it
      // - the given time (to the max of the key creating it)
      // - the plan lifetime
      
      if (pack.ok && !pack.id && pack.writer) {
        pack.lifetime = pack.lifetime || plan.lifetime;
        ns.errify(pack.lifetime <= plan.maxLifetime || plan.maxLifetime === 0, ns.settings.errors.BAD_REQUEST,
          "max lifetime for your plan is " + pack.plan.maxLifetime, pack);
        
        // decode the writer so we can workout the item lifetime
        var cp = ns.getCouponPack(pack.writer, {});
        
        // double check writer is ok
        ns.errify (cp.ok, ns.settings.errors.BAD_REQUEST,cp.error, pack);
        // use the min of max lifetime or the key life or the given number
        if (pack.ok) {
          var life = Math.round((new Date(cp.validtill).getTime() - now)/1000);
          pack.lifetime = Math.min(pack.lifetime, plan.maxLifetime, life);
        }
      }
    }
    
    ns.errify (pack.id || pack.lifetime > 0, ns.settings.errors.INTERNAL , "couldnt calculate lifetime", pack);
    
    // if we're doing an update - changing the lifetime is not allowed
    ns.errify (!(pack.id && pack.lifetime), ns.settings.errors.BAD_REQUEST , "an update cant change the lifetime", pack);
    
    // but we can calculate the lifetime remaining for an update from its key
    if (pack.id) {
      var cd = ns.getCouponPack(pack.id, {});
      
      // double check item is ok
      ns.errify (cd.ok, ns.settings.errors.BAD_REQUEST,cd.error, pack);
      
      // use the lifetime calculated from its key - now as the new lifetime
      if (pack.ok) {
        pack.lifetime = Math.round((new Date(cd.validtill).getTime() - now)/1000);
      }
        
    }
    // go away if we didnt make it past those gates
    if (!pack.ok) {
      return Promise.resolve(null);
    }

    function checkIntention_(pr) {

      var ob = pr.ob;
      var pack = pr.pack;

      //--no need to check of there's no ob (its new)
      if (!ob || !pack.ok) return Promise.resolve({
        ob: ob,
        pack: pack
      });

      //---if there's an intent, we can check right now if its expired without bothering going to cache
      if (pack.intent) {
        var couponPack = ns.getCouponPack(pack.intent, {});
        ns.errify(couponPack.code !== ns.settings.errors.UNAUTHORIZED, couponPack.code, "intent key has expired - cant update", pack);
        ns.errify(couponPack.ok, ns.settings.errors.CONFLICT, "intent key is invalid - cant update", pack);
      }
      if (!pack.ok) return Promise.resolve({
        ob: ob,
        pack: pack
      });

      //---now we need to go to the lock store
      var key = ns.settings.intentPrefix + pack.id;
      return redisIntent_.get(key)
        .then(function(result) {
          if (result) {

            //-- if we get here then there's a lock. need to make sure its the same as the intended
            var who = pack.intent + "," + pack.updater;

            //-- possiblly still hanging around in cache even though the key has actually expired so ignore it
            ns.errify(result === who, ns.settings.errors.CONFLICT, "item is locked by another key", pack);

            //-- we can even advise the amount of time to wait to try again if i
            if (!pack.ok) {
              var lockPack = ns.getCouponPack(result.split(",")[0], {});
              pack.intentExpires = Math.max(0, lockPack.ok ? Math.ceil((new Date(lockPack.validtill).getTime() - new Date().getTime()) / 1000) : 0);
              if (!pack.intentExpires) {
                pack.error = "";
                pack.ok = true;
              }

            }


          }

          // if there was no lock, but there was an intent, myabe its been used up and deleted
          else {
            ns.errify(!pack.intent, ns.settings.errors.CONFLICT, "Intent key has already been used", pack);

          }

          return {
            ob: ob,
            pack: pack
          };
        });
    }

    // two step .. get item if exists, create or update
    return getExistingThing_(pack)
      .then(function(result) {
        // first we need to check that there's not a lock
        return checkIntention_(result);
      })
      .then(function(result) {
        return writeOb_(result);
      })
      .then(function(pack) {
        if (!pack.ok || !pack.intent) return pack;

        // now we need to delete the intent since its now been used
        var key = ns.settings.intentPrefix + pack.id;
        return redisIntent_.del(key)
          .then(function(result) {
            // im not going to fail if the delete didnt happen
            // as it may have simply expired in the meantime
            // but i will write to the log file in case
            if (!result) console.log('should have been able to delete intent', key);
            return Promise.resolve(pack);
          });
      })
      .then(function(pack) {
        // maybe there are aliase required, but only allowed if there's a writer key as well
        return pack.alias && pack.writer && pack.ok ? ns.multipleAlias(pack) : pack;
      })
      .catch(function(err) {
        return Promise.resolve(ns.errify(false, ns.settings.errors.INTERNAL, err.toString(), pack));
      });



    function writeOb_(pr) {

      var pack = pr.pack;
      var oldOb = pr.ob;

      // but dont bother if something has gone wrong
      if (!pack.ok) {
        return Promise.resolve(pack);
      }

      // the base item      
      var ob = oldOb || {};
      var writer = oldOb ? oldOb.writer : pack.writer;

      if (!writer) {
        console.log('should have been a writer', pack, oldOb);
      }

      // the value has some control stuff around it
      var data = {
        value: value,
        modified: new Date().getTime(),
        accountId: pack.accountId,
        writer: writer,
        session: pack.session
      };

      // the owner will be the writer for a new thing,
      // or the oiginal for an old thing
      data.owner = ob.owner || writer;

      // maybe they are changing - this would have errored if non owner tried to change
      if (pack.updaters || ob.updaters) {
        data.updaters = pack.updaters || ob.updaters;
      }

      if (pack.readers || ob.readers) {
        data.readers = pack.readers || ob.readers;
      }

      // if its the owner then report the current readers and updaters

      if (!pack.updater) {
        pack.readers = data.readers;
        pack.updaters = data.updaters;
      }

      // write it out.
      var s = JSON.stringify(data);


      // allow 1k of overage forcontrol stuff
      ns.errify(plan.maxSize > s.length + 1024, ns.settings.errors.BAD_REQUEST, "max size for your plan is " + plan.maxSize, pack);
      if (!pack.ok) {
        return Promise.resolve(pack);
      }

      // if after all that we're still good to go, we wont count this as an access, since we did it with the read
      // but we will check the write quota
      return ns.settings.rateManagers[pack.plan]
        .getSlot(pack.accountId, "quota", s.length)
        .then(function(passed) {
          rlify_(passed, pack);
          pack.size = s.length;
          return pack.ok ?
            ns.write(ns.getPrivateKey(pack.accountId, pack.id), s, pack.lifetime).then(function(result) {
              return pack;
            }) :
            pack;
        })
        .then(function(result) {
          if (pack.ok) {
            ns.statify(pack.accountId, couponKey, "set", s.length);
            pack.code = ns.settings.errors.CREATED;
          }
          return pack;
        });

    }


  };

  function getExistingThing_(pack) {

    // this would be new item since no id is specified
    if (!pack.id) {
      // generate a new key
      var kob = ns.getNewKey(pack);
      pack.id = kob.public;
      // whoever writes this new item will become its owner
      return Promise.resolve({
        pack: pack,
        ob: null
      });
    }


    // see if there's limitation
    return ns.settings.rateManagers[pack.plan]
      .getSlot(pack.accountId)
      .then(function(passed) {
        rlify_(passed, pack);
        if (pack.ok) {
          return ns.read(ns.getPrivateKey(pack.accountId, pack.id))
            .then(function(result) {
              // convert to an object
              var ob = obify_(result, pack);

              // do a string of error checks - pack gets updated
              ns.errify(ob, ns.settings.errors.NOT_FOUND, 'item missing ' + pack.id, pack);

              ns.errify(ns.settings.allowAccessorChanges || !(pack.readers || pack.updaters),
                ns.settings.errors.FORBIDDEN, "changing readers or updaters is not allowed", pack);

              ns.errify(!ob || (pack.writer === ob.owner || !(pack.readers || pack.updaters)),
                ns.settings.errors.FORBIDDEN, "only the owner can change the readers or updaters", pack);

              ns.errify(!ob || (ob.accountId && pack.accountId === ob.accountId), ns.settings.errors.INTERNAL, "item account id mismatches key", pack);
              if (pack.ok) {
                if (canWrite_(pack, ob)) {
                  pack.modified = ob.modified;
                }
                else {
                  ns.errify(false, ns.settings.errors.FORBIDDEN, "you are not allowed to write to this data", pack);
                }
              }
              return {
                pack: pack,
                ob: ob
              };
            });
        }
        else {
          return {
            pack: pack,
            ob: null
          };
        }
      });
  }

  /**
   * get a value
   * @param {string} key the key
   * @return {Promise}
   */
  ns.read = function(key) {

    return redisClient_.get(key)
      .then(function(result) {
        return Promise.resolve(result ? decryptText_(result, key) : result);
      });

  };

  /**
   * remove a value
   * @param {key} key the key
   * @return {Promise}
   */
  ns.del = function(key) {
    return redisClient_.del(key);
  };

  /**
   * find the seed info for a given code
   */
  function findSeed_(code) {
    return ns.settings.seeds.filter(function(d) {
      return code ? d.name === code.slice(0, d.name.length) : false;
    })[0];
  }

  /**
   * decode a coupon code
   * @param {string} code the code
   * @return {object} the decoded coupon
   */
  ns.decodeCoupon = function(code, seed) {
    return coupon_.decode(seed, code);
  };

  /**
   * exposes function to combine all param sources
   * @param {request} req the request
   * @return {object} the combined params
   */
  ns.squashParams = function(req) {
    return paramSquash_(req);
  };
  /**
   * generate a coupon code
   * @param {request} req request
   * @return {object} the pack with the coupon
   */
  ns.getCoupon = function(req) {

    var params = paramSquash_(req);
    return ns.makeCoupon(params);
  };

  /**
   * make a coupon
   * @param {object} params needs .type , .plan , .days | .seconds , accountid
   * @return {object} result pack
   */
  ns.makeCoupon = function(params) {

    // find the seed with the matching type & plan thats an api key
    var seed = ns.settings.seeds.filter(function(d) {
      return d.type === params.type && d.plan === params.plan;
    })[0];

    var pack = ns.errify(seed, ns.settings.errors.BAD_REQUEST, "no matching plan and type for coupon", {
      ok: true
    });

    pack.lockValue = (params.lock || "");

    if (pack.ok) {

      var nDays = params.days ? parseInt(params.days, 10) : 0;
      var nSeconds = params.seconds ? parseInt(params.seconds, 10) : 0;
      var now = new Date();

      var target = nSeconds ?
        coupon_.addDate(now, "Seconds", nSeconds).getTime() :
        coupon_.addDate(now, "Date", nDays || ns.settings.days).getTime();

      pack.code = coupon_.generate(seed.value + pack.lockValue, target, seed.name + params.accountid, parseInt(params.accountid, 32));


    }

    return pack;

  };

  /**
   * generate a pack for a coupon
   * @param {string} code the coupon code
   * @param {object} params the params
   * @return {object} the pack
   */
  ns.getCouponPack = function(code, params) {
    var seed = findSeed_(code) || {};
    var pack;
    try {
      var coupon = ns.decodeCoupon(code, seed.value + (params.unlock || "")) || {};
      pack = {
        ok: true,
        key: coupon.coupon,
        validtill: coupon.expiry ? new Date(coupon.expiry).toISOString() : "",
        type: seed.type,
        plan: seed.plan,
        accountId: coupon.extraDays ? coupon.extraDays.toString(32) : "unknown"
      };
      if (!coupon.valid) {
        ns.errify(false, ns.settings.errors.BAD_REQUEST, "key is invalid - maybe it needs an unlock parameter", pack);
      }
      else if (coupon.expired) {
        ns.errify(false, ns.settings.errors.UNAUTHORIZED, "key has expired", pack);
      }
      else {
        pack.code = ns.settings.errors.OK;
      }
    }
    catch (err) {
      pack = ns.errify(false, ns.settings.errors.BAD_REQUEST, "key is invalid", {});
    }

    return pack;

  };

  /**
   *  for convenience, params and query and body will be treated as one
   *  like that i can use post or get for most things
   *  as some clients cant post
   */
  function paramSquash_(req) {

    function clone_(ob, init) {
      return ob ?
        Object.keys(ob)
        .reduce(function(p, c) {
          p[c] = ob[c];
          return p;
        }, init) : {};
    }

    // order of precendence
    return clone_(req.params, clone_(req.query, clone_(req.body, {})));

  }

  /**
   * handles the setting of an item from the url request
   * @param {request} req
   * @return {Promise}
   */
  ns.reqSet = function(req) {

    // get and validate the apikey
    var params = paramSquash_(req);
    var s = params.data;
    var data;
    // lets make that into an object if poss
    try {
      data = JSON.parse(s);
    }
    catch (err) {
      data = s;
    }


    var pack = ns.getCouponPack(params.writer || params.updater, params);

    // get rid of invalid apikey
    if (!pack.ok) {
      return Promise.resolve(pack);
    }

    // get rid of no writers
    if (pack.type !== 'writer' && (pack.type !== 'updater' || !params.id)) {
      ns.errify(false, ns.settings.errors.UNAUTHORIZED, (pack.id ?
          "You need a writer or updater key to update items-" : "You need an updater key to update items-") +
        pack.type, pack);
      return Promise.resolve(pack);
    }

    var couponKey = pack.key;

    // reset the pack to what we might need for this
    pack = {
      writer: params.writer,
      updater: params.updater,
      ok: true,
      id: params.id,
      plan: pack.plan,
      accountId: pack.accountId,
      session: params.session || ""
    };


    if (params.alias) {
      pack.alias = params.alias;
    }

    if (params.intent) {
      pack.intent = params.intent;
      ns.errify(pack.updater, ns.settings.errors.BAD_REQUEST, "if intent is specified you need to provide an updater key", pack);
    }
    if (!pack.ok) {
      return Promise.resolve(pack);
    }

    // check we have something to write, could be in post or params
    var value = data;

    // if theres no data
    if (typeof value === typeof undefined) {
      return Promise.resolve(ns.errify(false, ns.settings.errors.BAD_REQUEST, "You need to provide some data", pack));
    }

    // can specify readers
    if (params.readers) {
      // need to validate these are keys that can read
      pack.readers = params.readers.split(",");
      ns.errify(pack.readers.every(function(d) {
        var seed = findSeed_(d) || {};
        var coupon = ns.decodeCoupon(d, seed.value);
        return coupon.valid && !coupon.expired;
      }), ns.settings.errors.ACCEPTED, "warning:reader keys not validated-they may be locked", pack);
      // it was just a warning
      pack.ok = true;
    }

    // and also updaters
    if (params.updaters) {
      pack.updaters = params.updaters.split(",");
      ns.errify(pack.updaters.every(function(d) {
        var seed = findSeed_(d) || {};
        var coupon = ns.decodeCoupon(d, seed.value);
        return coupon.valid && !coupon.expired && seed.type === "updater";
      }), ns.settings.errors.ACCEPTED, "warning:updater keys not validated-they may be locked", pack);
      // it was just a warning
      pack.ok = true;
    }
    pack.lifetime = params.lifetime ? parseInt(params.lifetime, 10) : 0;

    // now we can set it, but first we have to check that the account is good and if there's an alias, the item exists
    return Promise.all([ns.checkAccount(pack), pack.id ? dealWithAlias_(pack, params) : Promise.resolve(pack)])
      .then(function(results) {

        return results.every(function(d) {
            return d.ok
          }) ?
          ns.set(pack, value, couponKey) :
          results.filter(function(d) {
            return !d.ok;
          })[0];
      });

  };

  /**
   * handles the getting of an item from the url request
   * @param {request} req
   * @return {Promise}
   */
  ns.reqGet = function(req) {
    var params = paramSquash_(req);

    var s = params.data;
    var data;
    // lets make that into an object if poss
    try {
      data = JSON.parse(s);
    }
    catch (err) {
      data = s;
    }


    // get and validate the apikey
    var pack = ns.getCouponPack(params.reader, params);

    // get rid of invalid apikey
    if (!pack.ok) {
      return Promise.resolve(pack);
    }

    var couponKey = pack.key;
    pack = {
      reader: params.reader,
      ok: true,
      id: params.id,
      accountId: pack.accountId,
      plan: pack.plan
    };
    if (params.intention) {
      pack.intention = params.intention;
    }

    // check we have an id
    if (!pack.id) {
      return Promise.resolve(ns.errify(false, ns.settings.errors.BAD_REQUEST, "You need to supply an id", pack));
    }

    // check we dont have a data packet
    if (data) {
      return Promise.resolve(ns.errify(false, ns.settings.errors.BAD_REQUEST, "Dont include data for reading. For writing, specify a writer key, not a reader key", pack));
    }

    // now we can set it
    return Promise.all([ns.checkAccount(pack), dealWithAlias_(pack, params)])
      .then(function(results) {
        return results.every(function(d) {
          return d.ok;
        }) ? ns.get(pack, couponKey) : results[0].ok ? results[1] : results[0];
      });
  };

  function dealWithAlias_(pack, params) {
    // check the id is valid

    return new Promise(function(resolve, reject) {
      // nothing to do
      if (!pack.id) {
        resolve(pack);
      }
      else {
        var idPack = ns.getCouponPack(pack.id, params);
        if (!idPack.ok) {
          // maybe its an alias
          var key = ns.settings.aliasPrefix + (pack.reader || pack.writer || pack.updater) + "-" + pack.id;
          redisAlias_.get(key)
            .then(function(result) {
              if (result) {
                pack.alias = pack.id;
                pack.id = decryptText_(result, key);
              }
              resolve(pack);
            });
        }
        else {
          resolve(pack);
        }
      }
    });
  }
  /**
   * handles the removing of an item
   * @param {request} req
   * @return {Promise}
   */
  ns.reqRemove = function(req) {
    var params = paramSquash_(req);
    var data = params.data;

    // get and validate the apikey
    var pack = ns.getCouponPack(params.writer, params);

    // get rid of invalid apikey
    if (!pack.ok) {
      return Promise.resolve(pack);
    }

    var couponKey = pack.key;
    pack = {
      writer: params.writer,
      ok: true,
      id: params.id,
      accountId: pack.accountId,
      plan: pack.plan
    };

    // check we have an id
    if (!pack.id) {
      return Promise.resolve(ns.errify(false, ns.settings.errors.BAD_REQUEST, "You need to supply an id", pack));
    }

    // check we dont have a data packet
    if (data) {
      return Promise.resolve(ns.errify(false, ns.settings.errors.BAD_REQUEST, "Dont include data when removing", pack));
    }

    // now we can remove it
    return ns.remove(pack, couponKey, params);

  };

  function getCipher_(privateKey) {
    return crypto.createCipher(ns.settings.cryptoAlgo, env_.effexMasterSeed + ns.settings.cryptoSeed + privateKey);
  }

  function getDecipher_(privateKey) {
    return crypto.createDecipher(ns.settings.cryptoAlgo, env_.effexMasterSeed + ns.settings.cryptoSeed + privateKey);
  }

  function encryptText_(text, privateKey) {
    if (typeof text === 'object') {
      text = JSON.stringify(text);
    }
    if (typeof text === typeof undefined) {
      text = '';
    }
    var cipher = getCipher_(privateKey);
    var crypted = cipher.update(text, 'utf8', 'base64');
    crypted += cipher.final('base64');
    return crypted;
  }

  function decryptText_(encrypted, privateKey) {
    var decipher = getDecipher_(privateKey);
    var decrypted = decipher.update(encrypted, 'base64', 'utf8');
    decrypted += decipher.final('utf8');
    return decrypted;
  }

  function findAk_(apiSeed, type) {
    // find the matching access key
    return ns.settings.seeds.filter(function(d) {
      return type === d.type && apiSeed.plan === d.plan;
    })[0];
  }

  /**
   * asking the api to reigister an alias for a given key
   * @param {object} pack
   * @return {Promise}
   */

  ns.multipleAlias = function(pack) {

    // we have to create one for each key in the pack, including the writer
    var proms = ['updaters', 'readers']
      .reduce(function(p, c) {
        (pack[c] || [])
        .forEach(function(d) {
          // clone the template
          var w = JSON.parse(JSON.stringify(p[0]));
          w.key = d;
          p.push(w);
        });
        return p;
      }, [{
        id: pack.id,
        writer: pack.writer,
        alias: pack.alias,
        key: pack.writer
      }])
      .map(function(d) {
        return ns.createAlias(d);
      });

    // wait for all that happen and check
    return Promise.all(proms)
      .then(function(pa) {
        // need to check for errors in any of that
        var errors = pa.filter(function(d) {
          return !d.ok;
        });

        return errors.length ? errors[0] : pack;
      });
  };




  /**
   * asking the api to reigister an alias for a given key
   * @param {request} req
   * @return {Promise}
   */
  ns.registerAlias = function(req) {
    return ns.createAlias(paramSquash_(req));
  };

  /**
   * asking the api to reigister an alias for a given key
   * @param {object} params
   * @return {Promise}
   */
  ns.createAlias = function(params) {

    // check all the keys make sense
    var pack = ns.getCouponPack(params.writer, params);
    if (!pack.ok) return Promise.resolve(pack);

    return ns.checkAccount(pack)
      .then(function(pack) {

        if (!pack.ok) return pack;

        var keyPack = ns.getCouponPack(params.key, params);
        if (!keyPack.ok) return keyPack;

        var idPack = ns.getCouponPack(params.id, params);
        if (!idPack.ok) return idPack;

        // now figure out expiration
        var nDays = params.days ? parseInt(params.days, 10) : 0;
        var nSeconds = params.seconds ? parseInt(params.seconds, 10) : 0;

        // if nDays are specified then use that otherwise use the date of the key
        var maxTime = new Date(keyPack.validtill).getTime();
        var now = new Date();
        var target = Math.min(nDays ? coupon_.addDate(now, "Date", nDays).getTime() :
          (nSeconds ? coupon_.addDate(now, "Seconds", nSeconds).getTime() : maxTime), maxTime);

        // make a new pack
        var aliasPack = {
          type: "alias",
          plan: pack.plan,
          lockValue: (params.lock || ""),
          ok: true,
          validtill: new Date(target).toISOString(),
          key: keyPack.key,
          alias: params.alias,
          id: params.id,
          accountId: keyPack.accountId,
          writer: pack.key
        };

        // write to store
        var key = ns.settings.aliasPrefix + aliasPack.key + "-" + aliasPack.alias;
        var text = encryptText_(aliasPack.id, key);

        // first we need to see if there is already one
        return redisAlias_.get(key)
          .then(function(result) {

            // deal with consequences of old one being replaced
            if (result) {
              var ob = decryptText_(result, key);

              if (ob) {
                // now we need to change any watchers of previous item using this alias
                var obKey = ns.getPrivateKey(aliasPack.accountId, ob);

                var wKey = ns.settings.watchablePrefix + "*" + obKey + "*";


                // but we dont need to block progress with this or bother the user if it fails
                redisWatchable_.keys(wKey)
                  .then(function(wKeys) {
                    // this will be all watchers of this item 

                    return Promise.all(wKeys.map(function(d) {
                        // get them and convert to an ob;
                        return redisWatchable_.get(d).then(function(r) {
                          return {
                            data: obify_(r, {}),
                            oldKey: d
                          };
                        });
                      }))
                      .then(function(obs) {
                        // just keep the ones that match this key and alias combination
                        obs.filter(function(d) {
                            return d.data && d.data.key === aliasPack.key && d.data.alias === aliasPack.alias;
                          })
                          .forEach(function(d) {
                            // finally, change the watching key to the new one
                            var nKey = d.oldKey.replace(obKey, ns.getPrivateKey(aliasPack.accountId, aliasPack.id));
                            redisWatchable_.rename(d.oldKey, nKey).then(function(r) {

                            });
                          });
                      });
                  });
              }
              else {
                console.log("problem with obifying", result);
              }
            }
            // now we can continue with writing the new one
            return redisAlias_.set(key, text, "EX", Math.round(target / 1000));
          })
          .then(function(result) {
            ns.statify(aliasPack.accountId, aliasPack.key, "set", text.length);
            aliasPack.code = ns.settings.errors.CREATED;
            return aliasPack;
          });








        return promAlias;
      });

  };

  /**
   * asking the api to c a key and validate the apikey
   * @param {request} req
   * @return {Promise}
   */
  ns.reqGetKey = function(req) {
    var params = paramSquash_(req);
    var pack = ns.getCouponPack(params.bosskey, params);

    if (pack.ok) {
      var seed = findSeed_(pack.key);
      ns.errify(seed, ns.settings.errors.INTERNAL, "cant find seed for key", pack);

      ns.errify(seed.type === "boss" && seed.boss, ns.settings.errors.INTERNAL, "wrong type of boss key", pack);

      // the api key was fine, check that that mode was good
      ns.errify(seed.boss && seed.boss.indexOf(params.mode) !== -1,
        ns.settings.errors.BAD_REQUEST, "your boss key doesn't allow you to generate " + params.mode + " keys", pack);

      ns.errify(pack.accountId && pack.accountId !== "undefined", ns.settings.errors.INTERNAL, "account id is missing", pack);

      // now we can generate access keys
      if (pack.ok) {

        var ak = findAk_(seed, params.mode);
        ns.errify(ak, ns.settings.errors.INTERNAL, "cant find key to swap for boss key", pack);
        ns.errify(!(params.days && params.seconds), ns.settings.errors.BAD_REQUEST, "choose either seconds or days for key duration", pack);

        if (pack.ok) {

          var nKeys = params.count ? parseInt(params.count, 10) : 1;
          var nDays = params.days ? parseInt(params.days, 10) : 0;
          var nSeconds = params.seconds ? parseInt(params.seconds, 10) : 0;

          // if nDays are specified then use that otherwise use the date of the api key 
          var maxTime = new Date(pack.validtill).getTime();
          var now = new Date();

          var target = nDays ? coupon_.addDate(now, "Date", nDays).getTime() :
            (nSeconds ? coupon_.addDate(now, "Seconds", nSeconds).getTime() : maxTime);

          // make sure it doesnt extend beyond end of apikey
          if (target > maxTime) {
            target = maxTime;
          }

          // now the pack is going to talk about the generated keys
          pack = {
            type: ak.type,
            plan: ak.plan,
            lockValue: (params.lock || ""),
            ok: true,
            validtill: new Date(target).toISOString(),
            keys: [],
            accountId: pack.accountId
          };

          for (var i = 0; i < nKeys; i++) {

            // this makes the keys all a little different
            var aBitRandom = Math.max(now.getTime(), target - lucky_.getRandBetween(0, 1000));

            pack.keys.push(
              coupon_.generate(ak.value + pack.lockValue, aBitRandom, ak.name, parseInt(pack.accountId, 32))
            );
          }
        }
      }
    }
    // now we need to check that all that is ok to do . ie. the account is operational
    return ns.checkBoss(pack, params.bosskey)
      .then(function(pack) {
        return ns.checkAccount(pack);
      })
      .then(function(result) {
        if (!result.ok) {
          result.keys = [];
        }
        return result;
      });

  };

  /**
   * decide whether a given writer can write
   * @param {object} pack the given params
   * @param {object} ob the retrieved item
   * @return {boolean}
   */
  function canWrite_(pack, ob) {

    var can =
      (!ob && pack.writer) || // we have a pack writer and there is no previous .. ie.. writing for the first time
      (ob && pack.updater && ((pack.updater === ob.owner) || ob.updaters && ob.updaters.indexOf(pack.updater) != -1)) || // the updater is the owner, or allowed
      (pack.writer === ob.owner); /// the owner can do what he wants     

    return can;

  }

  /**
   * decide whether a given reader can read
   * @param {object} pack the given params
   * @param {object} ob the retrieved item
   * @return {boolean}
   */
  function canRead_(pack, ob) {
    return ob && (pack.reader === ob.owner || pack.reader === ob.writer ||
      (ob.updaters && ob.updaters.indexOf(pack.reader) !== -1) ||
      (ob.readers && ob.readers.indexOf(pack.reader) !== -1));
  }

  /**
   * objectify the retrieved item
   * @param {string} str the retrieved item
   * @param {object} pack update this with the obifying result
   * @return {object} the result
   */
  function obify_(str, pack) {
    var ob = null;
    try {
      ob = str ? JSON.parse(str) : null;
    }
    catch (err) {
      ns.errify(false, ns.settings.errors.INTERNAL, "data in exchange was invalid" + str, pack);
    }
    return ob;
  }

  /**
   * register an error if condition is false and ok is not already false
   * @param {boolean|*} test the thing to test - anything truthy
   * @param {number} code the error code to assign
   * @param {string} error the error message
   * @param {object} pack the package
   * @return {object} the pack
   */
  ns.errify = function(test, code, error, pack) {

    // if the test is not truthy then its an error
    if (!test) {
      // but only if its ok right now
      if (pack.ok) {
        pack.ok = false;
        pack.code = code;
        pack.error = error;
      }
    }
    return pack;
  };

  return ns;

})({});


module.exports = Process;
