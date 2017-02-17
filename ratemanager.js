// an implementation of a rate limiter
// @constructor RateManager
module.exports = function  (limiters,redisClient) {
  
  var cache_ = redisClient || {} ,self = this;
  
  self.settings = {

    cache:{                                              //if using local cache.. could use redis for this

      expire:function (key, expiresec) {
        setTimeout(function () { 
          if (cache_.hasOwnProperty(key)) {
           cache_.delete[key];  
          }
          else {
            console.log("cache key should have existed",key);
          }
        } , expiresec*1000);
      },
      
      incr:function (key , expiresec) {
        console.log('why am i here');
        return new Promise (function (resolve, reject) {
          if (!cache_.hasOwnProperty(key)) {
           cache_[key] = 0;
           this.expire (key , expiresec+1);
          }
          cache_[key]++; 
          resolve (cache_[key]);
        });
      }
    },
     
    keyGen: function (lim,id, win) {
        return lim + "-" + win + "-" + id;
    },
    
    limiters: {
      burst: {             // burst of up to 5 a second
        seconds:1,        // period over which to measure
        rate:20             // how many to allow in that period
      },
      minute: {            // its 1 a second, measured over a minute
        seconds:60,        // period over which to measure
        rate:200            // how many to allow in that period
      },
      hour: {              // its 30 a minute, measured over an hour
        seconds:60*60,     // period over which to measure
        rate:2000            // how many to allow in that period
      }
    }
  };
  
  // support for redis client
  if (redisClient) {
    self.settings.cache = {                                             
      expire:function (key, expiresec) {
        //  not needed, built in to the incrementer
        return ;
      },
      incr:function (key,  expiresec) {
        try {
          console.log (key,expiresec);
          console.log('incr', key, expiresec);
          return cache_.multi()
          .incr (key)
          .expire(key,expiresec)
          .exec() ;
        }
        catch (err) {
          throw 'damn';
        }
      }
    };
  }
  
  // maybe different than the default
  if (limiters) {
    self.settings.limiters = limiters;
  }
  self.getSlot = function (identity) {

    // go through each limiter seeing if any limit is bust
    return Promise.all (
      Object.keys(self.settings.limiters)
      .map (function (limiter) {
      
        // these are the parameters for this limiter
        var lob = self.settings.limiters[limiter];
        var w = Math.floor(new Date().getTime() / (lob.seconds *1000));
      
        // this key is written against for the number of attempts
        var key = (lob.keyGen || self.settings.keyGen) (limiter, identity, w);
        return {
          key:key,
          lob:lob,
          w:w,
          limiter:limiter
        };
      })
      .map (function (d) {
        console.log('about to incr', d);
        return cache_.incr (d.key, d.lob.seconds)
        .then (function (result) {
          d.count = result;
          return d;
        });
      })
      )
      .then (function (result) {
        return {
          ok:result.every (function (d) { return d.lob.rate >= d.count;} ),
          failures:result.map(function (d) { return d.lob.rate > d.count ? null : d} ).filter(function (d) { return d;} )
        }
      });
  };

};
