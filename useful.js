/**
 * just some useful stuff
 */
module.exports =  (function (ns) {


  /**
   * objectify the retrieved item
   * @param {string} str the retrieved item
   * @return {object} the result
   */
  ns.obify = function (str) {
    var ob = null;
    try {
      ob = str ? JSON.parse(str) : null;
    }
    catch (err) {
      // didnt work, so just return the original string
      ob=str;
    }
    return {ok:typeof ob === "object", ob:ob};
  };

  /**
   * get items that match a partial key, and expecting objects
   * @param {object} handle the redis handle
   * @param {string} [pattern=*] the pattern to match
   * @return {Promise} the result
   */
  ns.getMatchingObs = function (handle,pattern) {

    return ns.getMatchingItems (handle, pattern)
    .then (function (result) {
      return result.map (function (d) {
       return {
         key:d.key, 
         data:ns.obify(d.data).ob
       }; 
      });
    });
    
 
  };
  
  /**
   * update an item. preserve ttl or set a new one
   * @param {object} handle the redis handle
   * @param {string} key the key
   * @param {*} content the content - if an object it'll get stringified
   * @param {number||null} expire if a number > 0 its an expire time, 0 means keep existing ttl, undefined means no expiry
   */
  ns.updateItem = function (handle , key , content,expire) {
    
    // first we have to get the expiry time if needed
    return (expire === 0 ? handle.ttl(key) : Promise.resolve (expire))
    .then (function (e) {
      
      // deal with errors retrieving the ttl (-1 no expiry, -2 no existing record)
      var ttl = e > 0 ? e :  undefined;
     
      // stingify the data if needed
      var data = typeof content === "object" ? JSON.stringify(content) : content;
      
      // set and apply ttl if needed
      return ttl ? handle.set (key, data , "EX", ttl) : handle.set (key,data);
    });
  };
  
  /**
   * get items that match a partial key
   * @param {object} handle the redis handle
   * @param {string} [pattern=*] the pattern to match
   * @return {Promise} the result
   */
  ns.getMatchingItems = function (handle,pattern) {

    // uses a stream to catch the scan results
    return new Promise (function (resolve, reject) {
      var stream = handle.scanStream( {
        match:pattern || "*"
      });
      var keys = [];
      stream.on ('data', function (r) {
        Array.prototype.push.apply (keys , r);
      });
      
      stream.on ('end', function (r) {
        resolve (keys);
      });
      
    })
    .then (function (keys) {
      return Promise.all (keys.map(function(k) {
        return handle.get(k)
        .then (function(d) {
          return Promise.resolve ({key:k, data:d});
        });
      }));

    });

  };
  
  /**
   * check a thing is a promise and make it so if not
   * @param {*} thing the thing
   * @param {Promise}
   */
  ns.promify = function (thing) {
    
    // is it already a promise
    var isPromise = !!thing && 
        (typeof thing === 'object' || typeof thing === 'function') 
    		&& typeof thing.then === 'function';
    
    // is is a function
    var isFunction = !isPromise && !!thing && typeof thing === 'function';
    
    // create a promise of it .. will also make a promise out of a value
    return Promise.resolve (isFunction ? thing() : thing);
  }
  /**
   * a handly timer
   * @param {*} [packet] something to pass through when time is up
   * @param {number} ms number of milliseconds to wait
   * @return {Promise} when over
   */
  ns.handyTimer = function ( ms , packet) {
    // wait some time then resolve
    return new Promise (function (resolve, reject) {
      setTimeout (function () { resolve (packet);},ms);
    });
  };
  
  /**
   * expbackoff
   * @param {function | Promise} action what to do 
   * @param {function} doRetry whether to retry
   * @param {number} [maxPasses=5] how many times to try
   * @param {number} [waitTime=500] how long to wait on failure
   * @param {number} [passes=0] how many times we've gone
   * @param {function} [setWaitTime=function(waitTime, passes,result) { ... return exptime.. }]
   * @return {Promise} when its all over
   */
  ns.expBackoff = function (action, doRetry, maxPasses , waitTime, passes, setWaitTime ) {
    
    // default calculation can be bypassed with a custom function
    setWaitTime = setWaitTime || function (waitTime , passes, result) {
       return  Math.pow(2,passes)*waitTime + Math.round (Math.random()*73);
    };
    
    // the defaults
    waitTime = waitTime || 500;
    passes = passes || 0;
    maxPasses = maxPasses || 5;
    
    // keep most recent result here
    var lastResult;
    
    // the result will actually be a promise
    // resolves, or rejects if there's an uncaught failure or we run out of attempts
    return new Promise (function (resolve, reject) {
      
      // start
      worker(waitTime);
      
      // recursive 
      function worker(expTime) {
console.log(expTime);
        // give up
        if (passes >= maxPasses) {
          
          // reject with last known result
          reject (lastResult);
        }
        // we still have some remaining attempts
        
        else {
          
          // call the action with the previous result as argument
          // turning it into a promise.resolve will handle both functions and promises
          ns.promify(action)
          	.then(function(result) {
            	// store what happened for later
            	lastResult = result;
            
            	// pass the result to the retry checker and wait a bit and go again if required
          		if (doRetry (lastResult , passes++)) { 
              	return ns.handyTimer(expTime)
                  .then (function () {
              			worker(setWaitTime(waitTime , passes , lastResult));
            			});
          		}
          		else {
                // finally
            		resolve (lastResult);
          		}
        		});
    
        }
      }
    });
  }; 
  
  
  return ns;
})({});
