/*
 *  Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
 *
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the LICENSE file in the root of the source
 *  tree.
 */

/* More information about these options at jshint.com/docs/options */

/* globals  describe, expect, it, beforeEach, afterEach, fail, WebSocket:true,
   XMLHttpRequest:true, SignalingChannel, webSockets:true, xhrs:true,
   FAKE_WSS_URL, FAKE_WSS_POST_URL, FAKE_ROOM_ID, FAKE_CLIENT_ID,
   MockXMLHttpRequest, MockWebSocket */

'use strict';

describe('Signaling Channel Test', function() {
  beforeEach(function() {
    webSockets = [];
    xhrs = [];

    this.realWebSocket = WebSocket;
    WebSocket = MockWebSocket;

    this.channel =
        new SignalingChannel(FAKE_WSS_URL, FAKE_WSS_POST_URL);

    this.realXMLHttpRequest = XMLHttpRequest;
    XMLHttpRequest = MockXMLHttpRequest;
  });

  afterEach(function() {
    WebSocket = this.realWebSocket;
    XMLHttpRequest = this.realXMLHttpRequest;
  });

  it('open success', function(done) {
    var promise = this.channel.open();
    expect(webSockets.length).toEqual(1);

    promise.then(function() {
      done();
    }).catch(function() {
      fail('Websocket could not be opened.');
    });

    var socket = webSockets[0];
    socket.simulateOpenResult(true);
  });

  it('receive message', function(done) {
    this.channel.open();
    var socket = webSockets[0];
    socket.simulateOpenResult(true);

    expect(socket.onmessage).not.toBeNull();

    this.channel.onmessage = function(msg) {
      expect(msg).toEqual(expectedMsg);
      done();
    };

    var expectedMsg = 'hi';
    var event = {
      'data': JSON.stringify({'msg': expectedMsg})
    };
    socket.onmessage(event);
  });

  it('open failure', function(done) {
    var promise = this.channel.open();
    expect(webSockets.length).toEqual(1);

    promise.then(function() {
      fail('WebSocket could be opened');
    }).catch(function() {
      done();
    });

    var socket = webSockets[0];
    socket.simulateOpenResult(false);
  });

  it('register before open', function() {
    this.channel.open();
    this.channel.register(FAKE_ROOM_ID, FAKE_CLIENT_ID);

    var socket = webSockets[0];
    socket.simulateOpenResult(true);

    expect(socket.messages.length).toEqual(1);

    var registerMessage = {
      cmd: 'register',
      roomid: FAKE_ROOM_ID,
      clientid: FAKE_CLIENT_ID
    };
    expect(socket.messages[0]).toEqual(JSON.stringify(registerMessage));
  });

  it('register after open', function() {
    this.channel.open();
    var socket = webSockets[0];
    socket.simulateOpenResult(true);
    this.channel.register(FAKE_ROOM_ID, FAKE_CLIENT_ID);

    expect(socket.messages.length).toEqual(1);

    var registerMessage = {
      cmd: 'register',
      roomid: FAKE_ROOM_ID,
      clientid: FAKE_CLIENT_ID
    };
    expect(socket.messages[0]).toEqual(JSON.stringify(registerMessage));
  });

  it('send before open', function() {
    this.channel.open();
    this.channel.register(FAKE_ROOM_ID, FAKE_CLIENT_ID);
    var message = 'hello';
    this.channel.send(message);

    expect(xhrs.length).toEqual(1);
    expect(xhrs[0].readyState).toEqual(2);
    expect(xhrs[0].url)
        .toEqual(FAKE_WSS_POST_URL + '/' + FAKE_ROOM_ID + '/' + FAKE_CLIENT_ID);
    expect(xhrs[0].method).toEqual('POST');
    expect(xhrs[0].body).toEqual(message);
  });

  it('send after open', function() {
    this.channel.open();
    var socket = webSockets[0];
    socket.simulateOpenResult(true);
    this.channel.register(FAKE_ROOM_ID, FAKE_CLIENT_ID);

    var message = 'hello';
    var wsMessage = {
      cmd: 'send',
      msg: message
    };
    this.channel.send(message);

    expect(socket.messages.length).toEqual(2);
    expect(socket.messages[1]).toEqual(JSON.stringify(wsMessage));
  });

  it('close after register', function() {
    this.channel.open();
    var socket = webSockets[0];
    socket.simulateOpenResult(true);
    this.channel.register(FAKE_ROOM_ID, FAKE_CLIENT_ID);

    expect(socket.readyState).toEqual(WebSocket.OPEN);
    this.channel.close();
    expect(socket.readyState).toEqual(WebSocket.CLOSED);

    expect(xhrs.length).toEqual(1);
    expect(xhrs[0].readyState).toEqual(4);
    expect(xhrs[0].url)
        .toEqual(FAKE_WSS_POST_URL + '/' + FAKE_ROOM_ID + '/' + FAKE_CLIENT_ID);
    expect(xhrs[0].method).toEqual('DELETE');
  });

  it('close before register', function() {
    this.channel.open();
    this.channel.close();
    expect(xhrs.length).toEqual(0);
  });

  it('V2 register waits for acknowledgement and sends epoch', function(done) {
    this.channel = new SignalingChannel(FAKE_WSS_URL, FAKE_WSS_POST_URL, 2);
    this.channel.configureV2('admission-token', '0');
    this.channel.open();
    var socket = webSockets[0];
    socket.simulateOpenResult(true);
    var registered = this.channel.register(FAKE_ROOM_ID, FAKE_CLIENT_ID);
    expect(JSON.parse(socket.messages[0])).toEqual({
      cmd: 'register',
      roomid: FAKE_ROOM_ID,
      clientid: FAKE_CLIENT_ID,
      ver: 2,
      token: 'admission-token'
    });

    socket.onmessage({data: JSON.stringify({
      control: 'registered',
      roomid: FAKE_ROOM_ID,
      epoch: '0',
      mode: 'p2p',
      is_initiator: true
    })});
    registered.then(function() {
      this.channel.send('candidate');
      expect(JSON.parse(socket.messages[1])).toEqual({
        cmd: 'send',
        epoch: '0',
        msg: 'candidate'
      });
      done();
    }.bind(this));
  });

  it('V2 close does not use the V1 HTTP fallback', function() {
    this.channel = new SignalingChannel(FAKE_WSS_URL, FAKE_WSS_POST_URL, 2);
    this.channel.configureV2('admission-token', '0');
    this.channel.open();
    var socket = webSockets[0];
    socket.simulateOpenResult(true);
    this.channel.register(FAKE_ROOM_ID, FAKE_CLIENT_ID);
    this.channel.close();
    expect(xhrs.length).toEqual(0);
  });
});
