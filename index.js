const Rx = require('rxjs/Rx');
const _ = require('underscore');
const requestPromise = require('request-promise');
const jar = requestPromise.jar();
const request = requestPromise.defaults({
  jar: jar,
  // proxy: 'http://localhost:8888',
  strictSSL: false,
});
const {
  sendToLogstash, logError, logInfo,
} = require('./utils');
const moment = require('moment');

const config = require('./config');

const loginToInstagram = ({ username, password }) =>
  Rx.Observable.from(request.get('https://www.instagram.com', { resolveWithFullResponse: true }))
    .mergeMap(() => request.post('https://www.instagram.com/accounts/login/ajax/', {
      form: {
        username,
        password,
      },
      headers: {
        referer: 'https://www.instagram.com/',
        'x-csrftoken': jar.getCookies('https://www.instagram.com').find(cookie => cookie.key === 'csrftoken').value,
      },
    }));

const getInstagramImages = ({ megaTag }) => (observable) =>
  observable
    .mergeMap(
      () => Rx.Observable.from(request.get(`https://www.instagram.com/explore/tags/${megaTag}/?__a=1`, { json: true }))
        .map(res => res.graphql.hashtag.edge_hashtag_to_media)
        .expand(prev =>
          prev.page_info.has_next_page ?
            Rx.Observable.from(request.get(
              `https://www.instagram.com/graphql/query/`,
              {
                json: true,
                qs: {
                  query_hash: '298b92c8d7cad703f7565aa892ede943',
                  variables: JSON.stringify({
                    tag_name: megaTag,
                    first: 100,
                    after: prev.page_info.end_cursor,
                  }),
                },
              },
            ))
              .map(res => res.data.hashtag.edge_hashtag_to_media) :
            Rx.Observable.empty(),
        )
        .concatMap(result => Rx.Observable.from(result.edges))
        .pluck('node'),
    );

const addInstagramTaggedUsers = (observable) =>
  observable
    .mergeMap(
      node =>
        Rx.Observable.of(node.shortcode)
          .mergeMap(shortcode => request.get(`https://www.instagram.com/p/${shortcode}/?__a=1`, { json: true }))
          .retry(3)
          .map(result => result.graphql.shortcode_media)
          .catch(err => {
            logError(`Ошибка получения информации по записи ${node.shortcode}: ${err.message}`);
            return Rx.Observable.empty();
          }),
      (sourceValue, resultValue) =>
        _.extend(
          {},
          _.pick(sourceValue, 'id', 'shortcode', 'taken_at_timestamp', 'display_url'),
          { owner: _.pick(resultValue.owner, 'id', 'username', 'profile_pic_url') },
          {
            likes: sourceValue.edge_liked_by.count,
            description: sourceValue.edge_media_to_caption.edges.map(edge => edge.node.text).join('\n'),
            tagged_users: _.uniq([...resultValue.edge_media_to_tagged_user.edges.map(edge => edge.node.user.username), resultValue.owner.username]),
          },
        ),
      1,
    );

loginToInstagram({ username: config.instagram.username, password: config.instagram.password })
  .let(getInstagramImages({ megaTag: config.instagram.megaTag }))
  // .take(1000)
  .let(addInstagramTaggedUsers)
  .filter(edge => edge.tagged_users.length > 1)
  .map(edge => _.extend({}, edge, {
    '@timestamp': moment(edge.taken_at_timestamp * 1000).utcOffset('+03:00').format(moment.defaultFormat),
    '@timestamp_import': moment().utcOffset('+03:00').format(moment.defaultFormat),
  }))
  // .do(console.log)
  .mergeMap(sendToLogstash(config.logstash.url))
  .subscribe({
    error: err => logError(`Общая ошибка обработки: ${err.message}`),
    complete: _ => logInfo(`Данные успешно загружены`)
  });
