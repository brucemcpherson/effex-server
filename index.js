/**
 * example urls
 * get baseurl for API info
 */
var App = require ('./routes');
var Process = require ('./process');

function start() {
  console.log('starting');
  // get ready
  Process.init();
  console.log('init done');
  // start listening
  App.init();


}

start();


