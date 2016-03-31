'use strict';

const amqp = require('amqplib');

var connections = {};
var channels = {};

function *getConnection(source) {
  if (!connections[source.uri]) {
    connections[source.uri] = yield amqp.connect(source);
  }
  return connections[source.uri];
}

function *getChannel(source) {
  var connection = yield getConnection(source);
  if (!channels[source.uri]) {
    channels[source.uri] = yield connection.createChannel();
  }

  return channels[source.uri];
}

module.exports = function(component) {
  component.defaultOptions = {
    concurrency: -1,
  };

  component.process = function *(message, options) {
    var channel = yield getChannel(message.uri);
    channel.assertQueue(options.queue);
    channel.sendToQueue(options.queue, message.serialize());
  };

  component.start = function *(source) {
    var options = source.options;
    var noAck = options.concurrency <= -1;

    if (source.uri.indexOf('//') === -1) {
      var userinfo = (options.username || '') + (options.password ? (':' + options.password) : '');
      var authority = (userinfo ? (userinfo + '@') : '') +
        (options.host || '') +
        (options.port ? (':' + options.port) : '');
      var query = '';

      options.uri = 'amqp://' + authority +
        (options.vhost ? ('/' + options.vhost) : '') +
        (query ? ('?' + query) : '');
    } else {
      options.uri = source.uri;
    }

    // var channel = yield getChannel(source);
    var connection = yield getConnection(source);
    var channel = yield connection.createChannel();
    channel.assertQueue(source.options.queue);
    if (!noAck) {
      channel.prefetch(options.concurrency);
    }
    channel.consume(source.options.queue, function(msg) {
      var message = component.createMessage(source.uri, 'inOut');
      message.unserialize(msg.content);

      component.request(message)
        .then(function() {
          if (!noAck) channel.ack(msg);
        }, function() {
          if (!noAck) channel.ack(msg);
        });
    }, { noAck: noAck });
  };
};