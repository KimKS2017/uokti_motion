const fs = require('fs');
const Rx = require('rxjs/Rx');
const csv = require('csv-parse');
const { streamToRx } = require('rxjs-stream');
const moment = require('moment');
const {
  getClient, scrollQuery, bulkUpdate,
} = require('./utils');

const config = require('./config');
const rxClient = getClient(config);

const round = (num, signs) => Math.round(Math.pow(10, signs) * num) / Math.pow(10, signs);
const carmaForUsersNum = (n) => Math.min(n > 3 ? round(0.0179 * Math.exp(0.3413 * n), 2) : 0, 0.5);
const carmaForUsersCarmaDiff = (d) => carmaForUsersNum(d);
const arrayMaxDiff = (arr) => Math.max(...arr) - Math.min(...arr);

const usersCurrentCarma = {};

const users$ = streamToRx(fs.createReadStream('./users.csv').pipe(csv({ columns: ['login', 'displayName', 'department'] })))
    .reduce((acc, curr) => Object.assign(acc, { [curr['login']]: [curr['displayName'], curr['department']] }), {});

const carma$ = scrollQuery({ rxClient })({
  index: 'metrics-instagram',
  scroll: '1m',
  size: 100,
  sort: "@timestamp:asc",
})
  .pluck('_source')
  .withLatestFrom(users$)
  .map(([{'@timestamp': timestamp, tagged_users}, registered_users]) => ({
    timestamp,
    tagged_users,
    registered_users,
    users: tagged_users.filter(userid => !!registered_users[userid]),
  }))
  .concatMap(({timestamp, users, registered_users}) => {
    const carmas = users.map(userid => usersCurrentCarma[userid] || 0);
    const photoCarma = round(1 + carmaForUsersNum(users.length) + carmaForUsersCarmaDiff(arrayMaxDiff(carmas)), 2);
    users.forEach(userid => usersCurrentCarma[userid] = round((usersCurrentCarma[userid] || 0) + photoCarma, 2));
    return Rx.Observable.from(users.map(userid => ({
      _index: 'metrics-instagram-users',
      _type: 'user',
      _id: userid + timestamp,
      _target: {
        '@timestamp': timestamp,
        '@timestamp_import': moment().utcOffset('+03:00').format(moment.defaultFormat),
        login: userid,
        carma: usersCurrentCarma[userid],
        displayName: registered_users[userid][0],
        department: registered_users[userid][1]
      }
    })));
  })
  .let(bulkUpdate({rxClient, test: false, upsert: true}))
  .subscribe(console.log);
