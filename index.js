/**
 * example urls
 * get baseurl for API info
 */
// restart redis - sudo service redis-server start
// these can be in config.json (better) or as environment variablesvi set in ~/.bashrc

//export REDIS_PASS=<redis password>
//export REDIS_PORT=<redis port>
//export REDIS_IP=<ip number of redis server>
//export EFFEX_MASTER_SEED=<some string to use for encryption>
//export EFFEX_ALGO=<some string to use to generate coupons>
//export EFFEX_ADMIN=<some admin key that can be used by a client to accss admin functions>


var express = require('express');
var app = express();
var cors = require('cors');
var bodyParser = require('body-parser');
//var morgan = require("morgan");
var appConfigs = require('./appConfigs.js');

var env_;

var App = (function(nsa) {

  // this is a middleware to handle promises in the router
  function prommy(req, res, next) {
    res.prom = function(prom, contentType) {
      prom.then(function(result) {
          doRes (result, contentType, 200);
        })
        .catch(function(error) {
          doRes (error , contentType , 500);
        });
    };
    next();
    
    function doRes (message,contentType, status) {
      if (contentType) {
        res.set('Content-Type', contentType);
        res.status(status).send('ok');            
      }
      else {
        res.json(message);
      }
    }
  }
  

  /**
   * call to kick off the routing listeners
   */
  nsa.init = function() {

    // does the interactions and runs the server
    // pick up ports from c9 env variables
    app.use(cors());
    
    app.use(bodyParser.json()); // to support JSON-encoded bodies
    app.use(bodyParser.urlencoded({ // to support URL-encoded bodies
      extended: false
    }));
    //app.use(morgan('combined'));
    app.use(prommy);
    
    app.listen(env_.expressPort, env_.expressHost); //, process.env.IP);

    // appengine health check
    app.get('/_ah/health', function(req, res) {
      res.prom(
        Process.ping()
        .then (function (result){
          if (!result.ok) throw 'bad';
          return 'ok';
        }),"text/plain");
    });
    
    // appengine start
    app.get('/_ah/start', function(req, res) {
      // nothing to do here
      res.prom(Promise.resolve('ok'),"text/plain");
    });
    
    // appengine stop
    app.get('/_ah/stop', function(req, res) {
      // no need to close redis specifically
      res.prom(Promise.resolve('ok'),"text/plain");
    });
    
    
    app.get('/info', function(req, res) {
      res.prom(Promise.resolve({
        ok: true,
        code: 200,
        info: {
          api:'effex-api',
          version:'1.01'   
        }}));
      });
    
          
    // respond with api help when request with no stuff is made
    app.get('/', function(req, res) {
      res.prom(Promise.resolve({
        ok: true,
        code: 200,
        info: {
          registerAlias: "/:writerkey/:key/alias/:alias/:id - creates an alias that can be used by this key for data items",
          getKey: "/:bosskey/:mode - mode can be reader or writer - returns a key that can be used",
          getValue: "/reader/:readerkey/:id - returns a value for the given id (GET)",
          insertValue: "/writer/:writerkey - with the data parameter (GET) or post body (POST)",
          insertValueWithAlias: "/writer/:writerkey/alias/:alias - with the data parameter (GET) or post body (POST) and assign alias",
          updateValue: "/updater/:updaterkey/:id - with the data parameter (GET) or post body (POST)",
          remove: "/writer/:writerkey/:id - remove the item (DELETE)",
          validate: "/validate/:key - validates any key",
          ping: "/ping - checks the service is alive",
          info: "/info - get service info",
          quotas: "/quotas - get all the service quotas",
          parameters: {
            general: "callback=jsonpcallback",
            data: "data=some data - normally in POST body, but for convenience can do GET as well",
            readers: "readers=comma,sep,list,of keys that can read",
            updaters: "updaters=comman,sep,list,of keys that can update",
            lifetime: "lifetime=inseconds",
            seconds: "seconds=number of a seconds a generated key should last for",
            days: "days=number of days a generated key shoud last for",
            count: "count=number of keys to generate",
            lock: "lock=some code that would be needed to use this key",
            unlock: "unlock=the code that this key was locked with",
            apikey: "needed for creating a bosskey - will be checked against account for validity"
          }
        }
      }));
    });

    //---this is admin and will be hidden in the final thing
    // get a boss key for an account
    app.get("/admin/account/:accountid/:type/:plan", function(req, res) {

      var params = Process.squashParams(req);
      var pack = Process.getCoupon(req);
      
      // since we've just created this, then push in the lock code as the unlock code to decode it
      params.unlock = params.lock;

      var registered = new Promise(function(resolve, reject) {

        // must have a uid
        pack = Process.errify(params.apikey, 401, "an apikey is needed to create a boss key", pack);
        pack = Process.checkAdmin (params.admin, pack);
        
        if (!pack.ok) {
          resolve(pack);
        }
        else {
          // populate with info for the pack
          pack = Process.getCouponPack(pack.code, params);

          // make sure its a valid/active account
          Process.checkAccount(pack, params.apikey)
            .then(function(result) {
              if (result.ok) {
                return Process.registerBoss(pack)
                  .then(function(result) {
                    resolve(result);
                  });
              }
              else {
                result.key = "";
                resolve(result);
              }
            });
        }
      });


      // when all thats over we can dispatch the result
      res.prom(registered);


    });

    app.get("/ping", function(req, res) {
      res.prom(Process.ping());
    });

    app.get("/quotas", function(req, res) {
      res.prom(Process.getQuotas());
    });

    // authid is a parameter in the post body
    app.post("/admin/register/:accountid", function(req, res) {
      res.prom(Process.registerAccount(req));
    });

    // delete an account
    app.delete("/admin/remove/:accountid", function(req, res) {
      res.prom(Process.deleteAccount(req));
    });

    // prune any boss keys associated with an account .. in other words, delete boss keys for an account
    app.delete("/admin/prune/:accountid", function(req, res) {
      res.prom(Process.pruneBosses(req));

    });

    // prune any boss keys associated with an account .. in other words, delete boss in the payload
    app.put("/admin/bosses", function(req, res) {
      res.prom(Process.removeBosses(req));
    });

    // get any boss keys associated with an account 
    app.get("/admin/bosses/:accountid", function(req, res) {
      res.prom(Process.getBosses(req));
    });

    // get all  the stats for a particular user
    app.get("/admin/stats/:accountid", function(req, res) {
      res.prom(Process.statsGet(req));
    });

    app.get("/admin/account/:accountid", function(req, res) {
      var pack = Process.checkAdmin (req.params.admin);
      if (!pack.ok) {
        res.prom(Promise.resolve (pack));
      }
      else {
        res.prom(Process.getAccount(req.params.accountid));
      }
    });

    // get all  the stats for all users
    app.get("/admin/stats", function(req, res) {
      res.prom(Process.statsGet(req));
    });

    //-- validates and reports on a key 
    app.get("/validate/:bosskey", function(req, res) {
      var params = Process.squashParams(req);
      res.prom(Promise.resolve(Process.getCouponPack(params.bosskey, params)));
    });

    // this is an update 
    app.route("/updater/:updater/:id")
      .get(function(req, res) {
        res.prom(Process.reqSet(req));
      })
      .post(function(req, res) {
        res.prom(Process.reqSet(req));
      });


    // this is delete
    app.route("/writer/:writer/:id")
      .delete(function(req, res) {
        res.prom(Process.reqRemove(req));
      });

    // this is a new record
    app.route("/writer/:writer")
      .get(function(req, res) {
        res.prom(Process.reqSet(req));
      })
      .post(function(req, res) {
        res.prom(Process.reqSet(req));
      });

    // this is a new record with an alias
    app.route("/writer/:writer/alias/:alias")
      .get(function(req, res) {
        res.prom(Process.reqSet(req));
      })
      .post(function(req, res) {
        res.prom(Process.reqSet(req));
      });
      
    // this is a reader
    app.route("/reader/:reader/:id")
      .get(function(req, res) {
        res.prom(Process.reqGet(req));
      });


    // this is asking for a reader or writer key to be generated, depending on the type of apikey
    app.get("/:bosskey/:mode", function(req, res) {
      res.prom(Process.reqGetKey(req));
    });
    
    // this is asking for an alias to be registered for 
    app.get("/:writer/:key/alias/:alias/:id", function(req, res) {
      res.prom(Process.registerAlias(req));
    });
    
    // the request was a mess
    app.use(function(req, res) {
      res.prom (Promise.resolve({
        ok: false,
        code: 404,
        error: "api url construction is unrecognizable:" + (req && req.headers ? req.headers.referer : "unknown")
      }));

    });

  };

  return nsa;
})({});

/**
 * does the main stuff for the app
 */
var Process = (function(ns) {

  var crypto = require('crypto');
  var coupon_, redisClient_, redis, lucky_, redisAdmin_, redisRate_, redisStats_, redisBosses_, redisAccounts_, redisAlias_;

  ns.settings = {

    plans: {
      a: {
        maxSize: .5 * 1000 * 1024,
        maxLifetime: 3 * 60 * 60,
        lifetime: 60 * 60,
        limiters: {
          burst: { // burst of up to 2 a second
            seconds: 10, // period over which to measure
            rate: 20 // how many to allow in that period
          },
          minute: { // its 50 in a minute
            seconds: 60, // period over which to measure
            rate: 50 // how many to allow in that period
          },
          day: { // its 2000 a day 
            seconds: 60 * 60 * 24, // period over which to measure
            rate: 2000 // how many to allow in that period
          },
          dailywrite: { //its 10mb a day
            seconds: 60 * 60 * 24,
            rate: 1000 * 1024 * 10,
            type: "quota" // means this is a quota rather than a limitation
          }
        }
      },
      b: {
        maxSize: 1 * 1000 * 1024,
        maxLifetime: 24 * 60 * 60,
        lifetime: 60 * 60,
        limiters: {
          burst: { // burst of up to 10 a second
            seconds: 10, // period over which to measure
            rate: 50 // how many to allow in that period
          },
          minute: { // its 200 a minute
            seconds: 60, // period over which to measure
            rate: 200 // how many to allow in that period
          },
          day: { // its 20000 a day 
            seconds: 60 * 60 * 24, // period over which to measure
            rate: 20000 // how many to allow in that period
          },
          dailywrite: { //its 100mb a day
            seconds: 60 * 60 * 24,
            rate: 1000 * 1024 * 100,
            type: "quota" // means this is a quota rather than a limitation
          }
        }
      },
      x: {
        maxSize: .2 * 1000 * 1024,
        maxLifetime: 1 * 60 * 60,
        lifetime: 60 * 60,
        limiters: {
          burst: { // burst of up to 10 a second
            seconds: 10, // period over which to measure
            rate: 25 // how many to allow in that period
          },
          minute: { // its 120 in a minute
            seconds: 60, // period over which to measure
            rate: 100 // how many to allow in that period
          },
          day: { // its 10000 a day 
            seconds: 60 * 60 * 24, // period over which to measure
            rate: 10000 // how many to allow in that period
          },
          dailywrite: { //its 50mb a day
            seconds: 60 * 60 * 24,
            rate: 1000 * 1024 * 50,
            type: "quota" // means this is a quota rather than a limitation
          }
        }
      },
    },

    seeds: [{
      name: "wak",
      type: "writer",
      value: "d.r0L09-wkb#",
      plan: "a"
    }, {
      name: "wbk",
      type: "writer",
      value: "ee.2LX-wkb#",
      plan: "b"
    }, {
      name: "uak",
      type: "updater",
      value: "d.r00w6b#",
      plan: "a"
    }, {
      name: "ubk",
      type: "updater",
      value: "eezLX-w#",
      plan: "b"
    }, {
      name: "rak",
      type: "reader",
      value: "4%r009(wkB",
      plan: "a"
    }, {
      name: "rbk",
      type: "reader",
      value: "0.T%r0(9(AB",
      plan: "b"
    }, {
      name: "da",
      type: "item",
      value: "-i%09(AB",
      plan: "a"
    }, {
      name: "db",
      type: "item",
      value: "T%0(+!(B",
      plan: "b"
    }, {
      name: "ba",
      type: "boss",
      value: "z&9P=+&0^",
      plan: "a",
      boss: ["reader", "writer", "updater"],
    }, {
      name: "bb",
      type: "boss",
      value: "y12P=+&0^",
      plan: "b",
      boss: ["reader", "writer", "updater"]
    }, {
      name: "wxk",
      type: "writer",
      value: "d0L09-wkb#",
      plan: "x"
    }, {
      name: "uxk",
      type: "updater",
      value: "dr00w-b#",
      plan: "x"
    }, {
      name: "rxk",
      type: "reader",
      value: "4%r09(wkB",
      plan: "x"
    }, {
      name: "dx",
      type: "item",
      value: "-i-%09(AB",
      plan: "x"
    }, {
      name: "bx",
      type: "boss",
      value: "z&9=+&0^",
      plan: "x",
      boss: ["reader", "writer", "updater"],
    }],
    rateManagers: {},
    keyLength: 14,
    allowAccessorChanges: false,
    days: 28,
    itemSeed: "thestarsthatplaywithlaughingsamsdice",
    adminId: "ear",
    cryptoAlgo: 'aes-256-ctr',
    cryptoSeed: 'zootalors',
    accountSeed: "mauricechevalier",
    accountPrefix: "ac-",
    itemPrefix: "it-",
    statPrefix: "sw-",
    aliasPrefix: "ap-",
    db: {
      stats: 3,
      exchange: 2,
      admin: 1,
      rate: 0,
      bosses: 4,
      accounts: 5,
      alias:6,
      password: "mybo0xnbunieli1eso0qqve444rtheSeaAnd-24#the0ocEan-4#12#$--cafI9£4ax3"
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
    var pack = ns.errify(true, 200, 'boss keys', {
      ok: true,
      accountId: params.accountid,
      keys: []
    });

    // check we have the account id
    ns.errify(pack.accountId, 400, "accountid required", pack);
    pack = ns.checkAdmin (params.admin, pack);
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
    var pack = ns.errify(true, 200, 'boss keys', {
      ok: true,
      accountId: params.accountid,
      keys: keys
    });

    ns.errify(keys, 400, "no keys supplied to delete", pack);
    pack = ns.checkAdmin (params.admin, pack);
    if (!pack.ok) {
      return Promise.resolve(pack);
    }
    if (!Array.isArray(keys)) {
      pack.keys = [keys];
    }

    return redisBosses_.del(pack.keys)
      .then(function(result) {
        return ns.errify(result === pack.keys.length, 404, "only found " + result + " keys to delete", pack);
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
              return ns.errify(result === pack.keys.length, 500, "not all bosses deleted", pack);
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
    var pack = ns.errify(true, 200, 'deleted', {
      ok: true,
      accountId: params.accountid
    });
    ns.errify(pack.accountId, 400, "accountid required", pack);
    pack = ns.checkAdmin (params.admin, pack);
    
    if (pack.ok) {
      return redisAccounts_.del(pack.accountId)
        .then(function(result) {
          pack = ns.errify(result === 1, 404, "failed deleting account", pack);
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
            return ns.errify(result, 404, "bosskey " + bosskey + " doesn't exist", pack);
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
          ns.errify(result, 404, "account doesn't exist", pack);
          if (pack.ok) {
            var ob = JSON.parse(result);
            ns.errify(ob.active, 404, "accout is not active", pack);
            ns.errify(!apikey || apikey === ob.authid, 401, "apikey not valid for this account", pack);
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
  
  ns.checkAdmin = function (key , pack) {
    return ns.errify(key === env_.adminKey,401,"You need to provide an admin key for this operation",pack || {ok:true});
  };
  
  /**
   * register an account in redis
   */
  ns.registerAccount = function(req) {
    var params = paramSquash_(req);
    params.data = params.data || {};

    var pack = ns.errify(true, 201, 'created', {
      ok: true,
      accountId: params.accountid,
      authId: params.data.authid,
      modified: new Date().getTime(),
      active: params.data.active
    });

    ns.errify(pack.authId, 400, "authid required", pack);
    ns.errify(pack.accountId, 400, "accountid required", pack);
    ns.checkAdmin (params.admin, pack);
    
    if (!pack.ok) {
      return Promise.resolve(pack);
    }

    return redisAccounts_
      .set(pack.accountId, encryptText_(params.data, env_.effexMasterSeed + ns.settings.accountSeed + pack.accountId))
      .then(function(result) {
        return pack;
      })
      .catch(function(err) {
        return ns.errify(false, 500, err, pack);
      });
  };

  /**
   * get the stats for an account
   */
  ns.statsGet = function(req) {
    // the keys to write this against
    var params = ns.squashParams(req);
    var pack = ns.checkAdmin (params.admin);
    if (!pack.ok) {
      return Promise.resolve (pack);
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
            return ns.errify(true, 200, "", {
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
        return ns.errify(false, 500, err, {
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
    //var publicKey = lucky_.getUniqueString(ns.settings.keyLength);

    var coupon = ns.makeCoupon({
      accountid: pack.accountId,
      plan: pack.plan,
      type: 'item',
      seconds: pack.lifetime
    });

    ns.errify(coupon && coupon.ok, 500, "failed to generate item id", pack);
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

    // need env variables
    env_ = {
      redisPort: appConfigs.get("REDIS_PORT"),
      redisIp: appConfigs.get("REDIS_IP"),
      redisPass: appConfigs.get("REDIS_PASS"),
      effexMasterSeed: appConfigs.get("EFFEX_MASTER_SEED"),
      effexAlgo: appConfigs.get("EFFEX_ALGO"),
      expressPort: appConfigs.get("PORT"),
      expressHost: appConfigs.get("IP"),
      adminKey: appConfigs.get ("EFFEX_ADMIN")
    };

    // check we got them all
    if (Object.keys(env_).some(function(d) {
        return !env_[d];
      })) {
      console.log('missing required env variables', env_);
      process.exit(1);
    }
    
    console.log('looking for redis at:',env_.redisIp, ":",env_.redisPort);
    console.log('running on:',env_.expressHost, ":",env_.expressPort);
    
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
        showFriendlyErrorStack:true
      };

      try {
        var red = new redis(p);
        red.on ("error", function(error) {
          console.log ('failed to open redis', error);
          process.exit(2);
        });
        return red;
      }
      catch (err) {
        console.log('failed to get redis handle', err);
        process.exit(1);
      }
    }

    redisClient_ = redisConf_('exchange');
    redisAdmin_ = redisConf_('admin');
    redisRate_ = redisConf_('rate');
    redisStats_ = redisConf_('stats');
    redisBosses_ = redisConf_('bosses');
    redisAccounts_ = redisConf_('accounts');
    redisAlias_ = redisConf_ ('alias');

    // need a rate manager for each possible plan
    //var rm = require('./ratemanager.js');
    var rm = RateManager;
    ns.settings.rateManagers = Object.keys(ns.settings.plans)
      .reduce(function(p, c) {
        p[c] = new rm(ns.settings.plans[c].limiters, redisRate_);
        return p;
      }, {});

    return ns;
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
          code: 200
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
      code: 200
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
          return dealWithAlias_ (pack,params)
          .then (function (pack){
            return ns.read(ns.getPrivateKey(pack.accountId, pack.id));
          })
          .then(function(result) {
              //parse it
              var ob = obify_(result, pack);
              ns.statify(pack.accountId, couponKey, "remove", 0);
              ns.errify(ob, 404, "item cannot be removed as it does not exist", pack);
              // make sure we can touch it
              ns.errify(ob && ob.accountId && pack.accountId === ob.accountId, 500, "account id mismatches key", pack);
              if (pack.ok && canWrite_(pack, ob) && ob.owner === pack.writer) {
                // all is good
                return ns.checkAccount(pack)
                  .then(function(result) {
                    return result.ok ?
                      ns.del(ns.getPrivateKey(pack.accountId, pack.id))
                      .then(function(dr) {
                        ns.errify(dr, 500, "failed to delete item", pack);
                        if (pack.ok) {
                          pack.code = 204;
                        }
                        return pack;
                      }) : result;
                  });
              }
              else if (ob) {

                ns.errify(false, 403, "only the owner can remove an item", pack);

              }
              else {
                ns.errify(false, 404, "unable to delete - item was missing", pack);
              }
              return pack;
            });
        }
        else {
          return pack;
        }
      })
      .catch(function(err) {
        return Promise.resolve(ns.errify(false, 500, (err ? err.toString() : "") + ":caught an error:" + (pack.error || ""), pack));
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
                pack.code = 200;
                pack.modified = ob.modified;
              }

              else if (ob) {
                ns.errify(false, 403, "you are not allowed to read this data", pack);
                ns.errify(ob.accountId && pack.accountId === ob.accountId, 500, "item account id mismatches key", pack);
              }
              else {
                ns.errify(false, 404, "item is missing", pack);
              }
              return pack;
            })
        }
        else {
          return pack;
        }
      })
      .catch(function(err) {
        return Promise.resolve(ns.errify(false, 500, (err ? err.toString() : "") + ":caught an error:" + (pack.error || ""), pack));
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

    // get the plan info
    var plan = ns.settings.plans[pack.plan];
    ns.errify(plan, 500, "cant find plan info for plan:" + pack.plan, pack);
    // set the default lifetime
    if (pack.ok) {
      pack.lifetime = pack.lifetime || plan.lifetime;
      ns.errify(pack.lifetime <= plan.maxLifetime || plan.maxLifetime === 0, 400,
        "max lifetime for your plan is " + pack.plan.maxLifetime, pack);
    }

    // go away if we didnt make it past those 2 gates
    if (!pack.ok) {
      return Promise.resolve(null);
    }

    // two step .. get item if exists, create or update
    return getExistingThing_()
      .then(function(ob) {
        return writeOb_(ob);
      })
      .then(function(pack){
        // maybe there are aliase required, but only allowed if there's a writer key as well
        return pack.alias && pack.writer ? ns.multipleAlias (pack) : pack;
      })
      .catch(function(err) {
        return Promise.resolve(ns.errify(false, 500, err.toString(), pack));
      });


    function getExistingThing_() {

      // this would be new item since no id is specified
      if (!pack.id) {
        // set the default lifetime
        var kob = ns.getNewKey(pack);
        pack.id = kob.public;
        // whoever writes this new item will become its owner
        return Promise.resolve(null);
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
                ns.errify(ob, 404, 'item missing ' + pack.id, pack);

                ns.errify(ns.settings.allowAccessorChanges || !(pack.readers || pack.updaters),
                  401, "changing readers or updaters is not allowed", pack);

                ns.errify(!ob || (pack.writer === ob.owner || !(pack.readers || pack.updaters)),
                  401, "only the owner can change the readers or updaters", pack);

                ns.errify(!ob || (ob.accountId && pack.accountId === ob.accountId), 500, "item account id mismatches key", pack);
                if (pack.ok) {
                  if (canWrite_(pack, ob)) {
                    pack.modified = ob.modified;
                  }
                  else {
                    ns.errify(false, 401, "you are not allowed to write to this data", pack);
                  }
                }
                return ob;
              });
          }
          else {
            return null;
          }
        });
    }

    function writeOb_(oldOb) {


      // but dont bother if something has gone wrong
      if (!pack.ok) {
        return Promise.resolve(pack);
      }

      // the base item      
      var ob = oldOb || {};
      var writer = oldOb ? oldOb.writer : pack.writer;
      var updater = pack.updater;
      
      if (!writer) {
        console.log('should have been a writer', pack, oldOb);
      }

      // the value has some control stuff around it
      var data = {
        value: value,
        modified: new Date().getTime(),
        accountId: pack.accountId,
        writer: writer
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
      ns.errify(plan.maxSize > s.length + 1024, 400, "max size for your plan is " + plan.maxSize, pack);
      if (!pack.ok) {
        return Promise.resolve(pack);
      }

      // if after all that we're still good to go, we wont count this as an access, since we did it with the read
      // but we will check the write quota
      return ns.settings.rateManagers[pack.plan].getSlot(pack.accountId, "quota", s.length)
        .then(function(passed) {
          rlify_(passed, pack);
          pack.size = s.length;
          return pack.ok ? ns.write(ns.getPrivateKey(pack.accountId, pack.id), s, pack.lifetime) : pack;
        })
        .then(function(result) {
          if (pack.ok) {
            ns.statify(pack.accountId, couponKey, "set", s.length);
            pack.code = 201;
          }
          return pack;
        });

    }


  };

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

    var pack = ns.errify(seed, 400, "no matching plan and type for coupon", {
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
        ns.errify(false, 400, "key is invalid - maybe it needs an unlock parameter", pack);
      }
      else if (coupon.expired) {
        ns.errify(false, 401, "key has expired", pack);
      }
      else {
        pack.code = 200;
      }
    }
    catch (err) {
      pack = ns.errify(false, 400, "key is invalid", {});
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
      data = JSON.parse (s);
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
    if (pack.type !== 'writer' && (pack.type !== 'updater' || !params.id) ) {
      ns.errify(false, 401, (pack.id ? 
       "You need a writer or updater key to update items-" : "You need an updater key to update items-" ) 
       + pack.type, pack);
      return Promise.resolve(pack);
    }

    var couponKey = pack.key;

    pack = {
      writer: params.writer,
      updater: params.updater,
      ok: true,
      id: params.id,
      plan: pack.plan,
      accountId: pack.accountId
    };
    
    if (params.alias) {
      pack.alias = params.alias;
    }

    // check we have something to write, could be in post or params
    var value = data;

    // if theres no data
    if (typeof value === typeof undefined) {
      return Promise.resolve(ns.errify(false, 400, "You need to provide some data", pack));
    }

    // can specify readers
    if (params.readers) {
      // need to validate these are keys that can read
      pack.readers = params.readers.split(",");
      ns.errify(pack.readers.every(function(d) {
        var seed = findSeed_(d) || {};
        var coupon = ns.decodeCoupon(d, seed.value);
        return coupon.valid && !coupon.expired;
      }), 202, "warning:reader keys not validated-they may be locked", pack);
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
      }), 202, "warning:updater keys not validated-they may be locked", pack);
      // it was just a warning
      pack.ok = true;
    }
    pack.lifetime = params.lifetime ? parseInt(params.lifetime, 10) : 0;

    // now we can set it, but first we have to check that the account is good and if there's an alias, the item exists
    return Promise.all ([ns.checkAccount(pack),  pack.id ? dealWithAlias_ (pack,params): Promise.resolve (pack)])
      .then(function(results) {
        
        return results.every(function(d) {return d.ok}) ? 
          ns.set(pack, value, couponKey) : 
          results.filter(function(d) {return !d.ok;})[0];
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
      data = JSON.parse (s);
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

    // check we have an id
    if (!pack.id) {
      return Promise.resolve(ns.errify(false, 400, "You need to supply an id", pack));
    }

    // check we dont have a data packet
    if (data) {
      return Promise.resolve(ns.errify(false, 400, "Dont include data for reading. For writing, specify a writer key, not a reader key", pack));
    }

    // now we can set it
    return Promise.all ([ns.checkAccount(pack), dealWithAlias_ (pack,params) ])
      .then(function(results) {
        return results.every(function (d) { return d.ok;})  ? ns.get(pack, couponKey) : results[0].ok ? results[1] : results[0];
      });
  };

  function dealWithAlias_ (pack,params) {
    // check the id is valid
   
    return new Promise (function (resolve, reject) {
      // nothing to do
      if (!pack.id) {
        resolve (pack);
      }
      else {
        var idPack = ns.getCouponPack(pack.id, params);
        if (!idPack.ok) {
          // maybe its an alias
          var key = ns.settings.aliasPrefix + (pack.reader||pack.writer||pack.updater) + "-" + pack.id;
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
          resolve (pack);
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
      return Promise.resolve(ns.errify(false, 400, "You need to supply an id", pack));
    }

    // check we dont have a data packet
    if (data) {
      return Promise.resolve(ns.errify(false, 400, "Dont include data when removing", pack));
    }

    // now we can remove it
    return ns.remove(pack, couponKey,params);

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
   
  ns.multipleAlias = function (pack) {

  console.log('entering multiple',pack);
  
    // we have to create one for each key in the pack, including the writer
    var proms = ['updaters','readers']
    .reduce (function (p,c){
      (pack[c] || [])
      .forEach (function (d) {
        // clone the template
        var w = JSON.parse(JSON.stringify(p[0]));
        w.key = d;
        p.push (w);
      });
      return p;
    },[{
      id:pack.id,
      writer:pack.writer,
      alias:pack.alias,
      key:pack.writer
    }])
    .map (function (d) {
      return ns.createAlias(d);
    });
    
    // wait for all that happen and check
    return Promise.all(proms)
    .then (function (pa){
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
  ns.registerAlias = function (req) {
    return ns.createAlias (paramSquash_(req));
  };
  
  /**
   * asking the api to reigister an alias for a given key
   * @param {object} params
   * @return {Promise}
   */
  ns.createAlias = function(params) {

    // check all the keys make sense
    var pack = ns.getCouponPack(params.writer, params);
    if (!pack.ok) return Promise.resolve (pack);
    
    return ns.checkAccount(pack)
    .then (function (pack){
      
      if (!pack.ok) return pack;
      
      var keyPack = ns.getCouponPack (params.key , params);
      if (!keyPack.ok) return keyPack;
        
      var idPack = ns.getCouponPack (params.id , params);
      if (!idPack.ok) return idPack;
        
      // now figure out expiration
      var nDays = params.days ? parseInt(params.days, 10) : 0;
      var nSeconds = params.seconds ? parseInt(params.seconds, 10) : 0;
    
      // if nDays are specified then use that otherwise use the date of the key
      var maxTime = new Date(keyPack.validtill).getTime();
      var now = new Date();
      var target = Math.min (nDays ? coupon_.addDate(now, "Date", nDays).getTime() :
              (nSeconds ? coupon_.addDate(now, "Seconds", nSeconds).getTime() : maxTime), maxTime);
  
      // make a new pack
      var aliasPack = {
        type: "alias",
        plan: pack.plan,
        lockValue: (params.lock || ""),
        ok: true,
        validtill: new Date(target).toISOString(),
        key: keyPack.key,
        alias:params.alias,
        id:params.id,
        accountId: keyPack.accountId,
        writer:pack.key
      };
      
    
      // write to store
      var key = ns.settings.aliasPrefix + aliasPack.key + "-" + aliasPack.alias;
      var text = encryptText_(aliasPack.id, key);

      return redisAlias_.set(key, text, "EX", Math.round (target/1000))
      .then (function(result) {
        ns.statify(aliasPack.accountId, aliasPack.key, "set", text.length);
        aliasPack.code = 201;
        return aliasPack;
      });
    });
 
  };
    
  /**
   * asking the api to generate a key and validate the apikey
   * @param {request} req
   * @return {Promise}
   */
  ns.reqGetKey = function(req) {
    var params = paramSquash_(req);
    var pack = ns.getCouponPack(params.bosskey, params);

    if (pack.ok) {
      var seed = findSeed_(pack.key);
      ns.errify(seed, 500, "cant find seed for key", pack);

      ns.errify(seed.type === "boss" && seed.boss, 500, "wrong type of boss key", pack);

      // the api key was fine, check that that mode was good
      ns.errify(seed.boss && seed.boss.indexOf(params.mode) !== -1,
        400, "your boss key doesn't allow you to generate " + params.mode + " keys", pack);

      ns.errify(pack.accountId && pack.accountId !== "undefined", 500, "account id is missing", pack);

      // now we can generate access keys
      if (pack.ok) {

        var ak = findAk_(seed, params.mode);
        ns.errify(ak, 500, "cant find key to swap for boss key", pack);
        ns.errify(!(params.days && params.seconds), 400, "choose either seconds or days for key duration", pack);

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
              coupon_.generate(ak.value + pack.lockValue, aBitRandom, ak.name, pack.accountId)
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
      (ob && pack.updater && ((pack.updater === ob.owner) || ob.updaters.indexOf(pack.updater) != -1)) || // the updater is the owner, or allowed
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
      ns.errify(false, 500, "data in exchange was invalid" + str, pack);
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
  }

  return ns;

})({});




function start() {
  console.log('starting');
  // get ready
  Process.init();
  console.log('init done');
  // start listening
  App.init();


}

start();


// an implementation of a rate limiter
// @constructor RateManager
function RateManager(limiters, redisClient) {

  var cache_ = redisClient || {},
    self = this;

  self.settings = {

    cache: { //if using local cache.. could use redis for this

      expire: function(key, expiresec) {
        setTimeout(function() {
          if (cache_.hasOwnProperty(key)) {
            cache_.delete[key];
          }
          else {
            console.log("cache key should have existed", key);
          }
        }, expiresec * 1000);
      },

      incr: function(key, expiresec, incrValue) {

        return new Promise(function(resolve, reject) {
          if (!cache_.hasOwnProperty(key)) {
            cache_[key] = 0;
            this.expire(key, expiresec + (incrValue || 1));
          }
          cache_[key]++;
          resolve(cache_[key]);
        });
      }
    },

    keyGen: function(lim, id, win) {
      return lim + "-" + win + "-" + id;
    },

    limiters: {
      burst: { // burst of up to 5 a second
        seconds: 1, // period over which to measure
        rate: 20 // how many to allow in that period
      },
      minute: { // its 1 a second, measured over a minute
        seconds: 60, // period over which to measure
        rate: 200 // how many to allow in that period
      },
      hour: { // its 30 a minute, measured over an hour
        seconds: 60 * 60, // period over which to measure
        rate: 2000 // how many to allow in that period
      }
    }
  };

  // support for redis client
  if (redisClient) {
    self.settings.cache = {
      expire: function(key, expiresec) {
        //  not needed, built in to the incrementer
        return;
      },
      incr: function(key, expiresec, incrValue) {

        return cache_.multi()
          .incrby(key, incrValue || 1)
          .expire(key, expiresec)
          .exec()
          .then(function(results) {
            return results[0][1];
          });


      }
    };
  }

  // maybe different than the default
  if (limiters) {
    self.settings.limiters = limiters;
  }

  /**
   * gets a slot or returns a rate limit/quota error
   * @param {string} identity something to identify who this is
   * @param {string} [type=limit] can be limit or quota
   * @param {string} [incr=1] how many to increment by
   * @return {object} info about whether its ok to go, and if not, then why not
   */
  self.getSlot = function(identity, type, incr) {

    type = type || "limit";
    incr = incr || 1;

    // go through each limiter seeing if any limit is bust
    return Promise.all(
        Object.keys(self.settings.limiters)
        .filter(function(d) {
          return self.settings.limiters[d].type === type;
        })
        .map(function(limiter) {

          // these are the parameters for this limiter
          var lob = self.settings.limiters[limiter];
          var w = Math.floor(new Date().getTime() / (lob.seconds * 1000));

          // this key is written against for the number of attempts
          var key = (lob.keyGen || self.settings.keyGen)(limiter, identity, w);
          return {
            key: key,
            lob: lob,
            w: w,
            limiter: limiter
          };
        })
        .map(function(d) {
          return self.settings.cache.incr(d.key, d.lob.seconds, incr)
            .then(function(result) {
              d.count = result;
              return d;
            });
        })
      )
      .then(function(result) {
        return {
          ok: result.every(function(d) {
            return d.lob.rate >= d.count;
          }),
          failures: result.map(function(d) {
            return d.lob.rate > d.count ? null : d
          }).filter(function(d) {
            return d;
          })
        };
      });
  };

}
