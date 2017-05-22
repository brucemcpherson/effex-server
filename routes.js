/**
 * example urls
 * get baseurl for API info
 */

var express = require('express');
var app = express();
var cors = require('cors');
var bodyParser = require('body-parser');
//var morgan = require("morgan");

var Process = require('./process');

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

    // set up cors
    var copts = {
      origin: function (origin, callback) {
      // i could do whitelisting here .. test the origin against a list and replace true with result
      callback (null, true);
    }};
    
    
    // pick up ports from c9 env variables
    app.use(cors(copts));
    
    app.use(bodyParser.json()); // to support JSON-encoded bodies
    app.use(bodyParser.urlencoded({ // to support URL-encoded bodies
      extended: false
    }));
    //app.use(morgan('combined'));
    app.use(prommy);
    app.listen(Process.env.expressPort, Process.env.expressHost, function (){
      console.log("listening on ",Process.env.expressPort);
    }); //, process.env.IP);


/**    
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
*/ 
    
    app.get('/info', function(req, res) {
      res.prom(Promise.resolve({
        ok: true,
        code: 200,
        info: {
          api:'effex-api',
          version:'1.1'   
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
          eventLog:"/eventlog/:accesskey/:id/:event",
          watchLog:"/watchlog/:watchablekey/:accesskey",
          watchable:"/watchable/:watchablekey/:accesskey",
          release:"/release/:id/:updater/:intent",
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
            apikey: "needed for creating a bosskey - will be checked against account for validity",
            intention:"=update to state an intention to update while making a read",
            intent:"the intent key that was returned by the intention parameter to authorize an update"
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
        pack = Process.errify(params.apikey, Process.settings.errors.UNAUTHORIZED, "an apikey is needed to create a boss key", pack);
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
    
    //-- get the watch log
    app.get("/watchlog/:watchable/:reader", function(req, res) {
      var params = Process.squashParams(req);
      res.prom(Promise.resolve(Process.getWatchLog (params.watchable, params.reader , params.since)));
    });

    //-- get the watchable
    app.get("/watchable/:watchable/:reader", function(req, res) {
      var params = Process.squashParams(req);
      res.prom(Promise.resolve(Process.getWatchable (params)));
    });

    //-- get logged events
    app.get("/eventlog/:key/:id/:event", function(req, res) {
      var params = Process.squashParams(req);
      res.prom(Promise.resolve(Process.pullLogEvents (params)));
    });

    //-- start watching
    app.post("/onregister/:key/:id/:event", function(req, res) {
      var params = Process.squashParams(req);
      res.prom(Promise.resolve(Process.onRegister (params)));
    });

    //-- start watching
    app.delete("/offregister/:watchable", function(req, res) {
      var params = Process.squashParams(req);
      res.prom(Promise.resolve(Process.offRegister (params)));
    });

    
    //-- release an intention
    app.delete("/release/:id/:updater/:intentkey", function(req, res) {
      var params = Process.squashParams(req);
      res.prom(Promise.resolve(Process.releaseIntent (params)));
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

module.exports = App;



