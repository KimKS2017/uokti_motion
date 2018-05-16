const Rx = require('rxjs/Rx');
const urlParse = require('url-parse');
const http = require('http');
const fs = require('fs');
const requestPromise = require('request-promise');
const {URL} = require('url');
const elasticsearch = require('elasticsearch');
const RxClient = require('rx-elasticsearch');
const _ = require('lodash');

module.exports.fsReadFileObservable = Rx.Observable.bindNodeCallback(fs.readFile);
module.exports.fsWriteFileObservable = Rx.Observable.bindNodeCallback(fs.writeFile);


const log = (type, msg) => console.error(`[${new Date().toString()}] [${type}] ${msg}`);
const logError = module.exports.logError = msg => log('ERROR', msg);
const logInfo = module.exports.logInfo =  msg => log('INFO', msg);

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

module.exports.getClient = (config) => {
  let esUrl = new URL(config.elasticsearch.url);

  const client = new elasticsearch.Client({
    host: [{
      protocol: esUrl.protocol,
      host: esUrl.hostname,
      port: esUrl.port || (esUrl.protocol === 'https:' ? 443 : (esUrl.protocol === 'http:' ? 80 : 9200)),
      path: esUrl.pathname,
      query: esUrl.search,
      auth: `${config.elasticsearch.username}:${config.elasticsearch.password}`,
    }],
    log: config.loglevel
  });

// Fix elasticsearch bug: https://github.com/elastic/elasticsearch-js/issues/537
  const oldClearScroll = client.clearScroll;
  client.clearScroll = params => oldClearScroll.call(client, Object.assign(params, {scrollId: [].concat(params.scrollId)}));

  return new RxClient.default(client);
};

module.exports.scrollQuery = ({rxClient}) => (params) =>
  rxClient
    .scroll(Object.assign({scroll: '1m', size: 100}, params))
    .concatMap(res => Rx.Observable.from(res.hits.hits));

const bulk = ({rxClient, bulkSize = 100, test = true, concurrency = 1, retries = 3, delay = 0}) => (observable, fn) =>
  observable
    .concatMap(fn)
    .bufferCount(bulkSize)
    .mergeMap(res =>
        Rx.Observable.of(_.flatten(res))
          .mergeMap(res =>
            test ? res : new Promise(
              (resolve, reject) =>
                rxClient.client.bulk(
                  {body: res},
                  (err, res) =>
                    err ? reject({message: 'Error while executing bulk request', payload: err}) : (
                      res.errors ? reject({
                        message: 'Error while executing bulk request',
                        payload: res
                      }) : resolve({message: `${res.items.length} records processed`, payload: res})
                    )
                )
            )
          )
          .retry(retries)
          .delay(delay),
      (srcVal, resVal) => resVal,
      concurrency
    );

module.exports.bulkUpdate = ({rxClient, bulkSize, test, upsert = false, fresh = false, fresh_old_id = '_id', concurrency, retries, delay}) => (observable) =>
  bulk({rxClient, bulkSize, test, concurrency, retries, delay})(
    observable,
    res =>
      res._script
        ? Rx.Observable.from([
          {update: {_index: res._index, _type: res._type, _id: res._id}},
          {script: {inline: res._script, lang: "painless"}}
        ])
        : (
          res._target && _.keys(res._target).length > 0
            ? (
              fresh
                ? Rx.Observable.from([
                  [
                    {delete: {_index: res._index, _type: res._type, _id: res[fresh_old_id]}}
                  ],
                  [
                    {create: {_index: res._index, _type: res._type, _id: res._id}},
                    res._target,
                  ],
                ])
                : Rx.Observable.from([
                  [
                    {update: {_index: res._index, _type: res._type, _id: res._id}},
                    {doc: res._target, doc_as_upsert: upsert},
                  ]
                ])
            )
            : Rx.Observable.empty()
        )
  );

module.exports.bulkCreate = ({rxClient, bulkSize = 100, test = true, concurrency, retries, delay}) => (observable) =>
  bulk({rxClient, bulkSize, test, concurrency, retries, delay})(
    observable,
    res =>
      res._target
        ? Rx.Observable.from([
          {create: {_index: res._index, _type: res._type, _id: res._id}},
          res._target
        ])
        : Rx.Observable.empty()
  );