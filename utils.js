const Rx = require('rxjs/Rx');
const urlParse = require('url-parse');
const http = require('http');
const fs = require('fs');
const requestPromise = require('request-promise');

module.exports.fsReadFileObservable = Rx.Observable.bindNodeCallback(fs.readFile);
module.exports.fsWriteFileObservable = Rx.Observable.bindNodeCallback(fs.writeFile);

const log = (type, msg) => console.error(`[${new Date().toString()}] [${type}] ${msg}`);
module.exports.logError = msg => log('ERROR', msg);
module.exports.logInfo = msg => log('INFO', msg);

module.exports.httpServerObservable = ({ host, port }) =>
  Rx.Observable.of(0)
    .map(() => http.createServer().listen(port, host))
    .mergeMap(
      server =>
        Rx.Observable.merge(
          Rx.Observable.fromEvent(server, 'error').mergeMap(Rx.Observable.throw),
          Rx.Observable.fromEventPattern(
            server.addListener.bind(server, 'listening'),
            () => {
              server.removeAllListeners('listening');
              server.removeAllListeners('error');
              server.removeAllListeners('close');
              server.close();
            },
          ),
        ),
      srcVal => srcVal,
    );

module.exports.httpRequestObservable = ({ server }) =>
  Rx.Observable.fromEventPattern(
    server.addListener.bind(server, 'request'),
    server.removeListener.bind(server, 'request'),
    (req, res) => ({ req, res }),
  )
    .map(({ req, res }) => ({ req: Object.assign(req, { query: urlParse(req.url, true).query }), res }))
    .takeUntil(Rx.Observable.fromEvent(server, 'close'));

module.exports.sendToLogstash = (url) => (data) => requestPromise.post(url, {
  json: true,
  body: data,
});