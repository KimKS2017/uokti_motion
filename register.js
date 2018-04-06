const Rx = require('rxjs/Rx');

const requestPromise = require('request-promise');
const config = require('./config');
const {
  httpServerObservable, httpRequestObservable,
  fsReadFileObservable, fsWriteFileObservable,
  logError, logInfo,
} = require('./utils');

const errorHtml = (msg) =>
  `<html>
<head><meta charset="UTF-8"></head>
<body><h3>${msg}</h3></body>
</html>`;

httpServerObservable(config.httpServer)
  .mergeMap(server => httpRequestObservable({ server }))
  .takeUntil(Rx.Observable.fromEvent(process, 'SIGINT'))
  .filter(({ req }) => req.url !== '/favicon.ico')
  .mergeMap(
    ({ req, res }) => {
      if (req.query === undefined || req.query.code === undefined || req.query.state === undefined) {
        res.writeHead(400);
        res.end(errorHtml('Некорректный запрос'));
        logError(`Некорректный запрос: ${req.url}`);
        return Rx.Observable.empty();
      }
      return Rx.Observable.of({ req, res })
        .mergeMap(
          ({ req: { query, url }, res }) =>
            Rx.Observable.from(
              requestPromise.post(config.instagram.access_token_url, {
                json: true,
                form: {
                  client_id: config.instagram.client_id,
                  client_secret: config.instagram.client_secret,
                  grant_type: 'authorization_code',
                  redirect_uri: config.instagram.redirect_uri,
                  code: query.code,
                },
              }),
            )
              .catch(
                err => {
                  logError(`Ошибка получения access_token (${url}): ${err.message}`);
                  res.writeHead(500);
                  res.end(errorHtml('Ошибка предоставления доступа к аккаунту Instagram, выполните повторный запрос или обратитесь к администратору'), 'utf8');
                  return Rx.Observable.empty();
                },
              ),
          ({ req: { query } }, resVal) => Object.assign({}, resVal, {
            employee_code: query.state,
            timestamp: new Date().getTime(),
          }),
        )
        .do(data => logInfo(`Получен access_token для пользователя (${data.user.username}/${data.employee_code}): ${data.access_token}`))
        .mergeMap(
          data =>
            fsReadFileObservable(`users.json`, 'utf8')
              .catch(err => err.code === 'ENOENT' ? Rx.Observable.of('[]') : Rx.Observable.throw(err))
              .map(contents => JSON.parse(contents))
              .map(dataFromFile => [...dataFromFile, data])
              .mergeMap(sumData => fsWriteFileObservable(`data/users.json`, JSON.stringify(sumData, null, '  '), 'utf8'))
              .catch(
                err => {
                  logError(`Ошибка сохранения кода в файл users.json: ${err.message}`);
                  return fsWriteFileObservable(`data/${data.employee_code}.json`, JSON.stringify(data, null, '  '), 'utf8')
                    .catch(err => {
                      logError(`Ошибка сохранения кода в отдельный файл: ${err.message}`);
                      return Rx.Observable.empty();
                    })
                }
              ),
          srcVal => srcVal,
          1,
        )
        .do(() => {
          res.writeHead(302, { Location: config.uokti.url });
          res.end();
        })
    }
  )
  .subscribe();