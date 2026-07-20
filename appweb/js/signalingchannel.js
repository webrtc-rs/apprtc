/*
 *  Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
 *
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the LICENSE file in the root of the source
 *  tree.
 */

/* More information about these options at jshint.com/docs/options */

/* globals parseJSON, trace, sendUrlRequest, RemoteWebSocket */
/* exported SignalingChannel */

'use strict';

// This class implements a signaling channel based on WebSocket.
var SignalingChannel = function(wssUrl, wssPostUrl, signalingVersion) {
  this.wssUrl_ = wssUrl;
  this.wssPostUrl_ = wssPostUrl;
  this.roomId_ = null;
  this.clientId_ = null;
  this.websocket_ = null;
  this.registered_ = false;
  this.signalingVersion_ = signalingVersion || 1;
  this.admissionToken_ = null;
  this.signalEpoch_ = null;
  this.registrationPromise_ = null;
  this.resolveRegistration_ = null;
  this.rejectRegistration_ = null;

  // Public callbacks. Keep it sorted.
  this.onerror = null;
  this.oncontrol = null;
  this.onmessage = null;
};

SignalingChannel.prototype.open = function() {
  if (this.websocket_) {
    trace('ERROR: SignalingChannel has already opened.');
    return;
  }

  trace('Opening signaling channel.');
  return new Promise(function(resolve, reject) {
    this.websocket_ = new WebSocket(this.wssUrl_);

    this.websocket_.onopen = function() {
      trace('Signaling channel opened.');

      this.websocket_.onerror = function() {
        trace('Signaling channel error.');
        if (this.rejectRegistration_) {
          this.rejectRegistration_(Error('WebSocket error.'));
          this.clearRegistrationPromise_();
        }
        if (this.onerror) {
          this.onerror('WebSocket error.');
        }
      }.bind(this);
      this.websocket_.onclose = function(event) {
        // TODO(tkchin): reconnect to WSS.
        trace('Channel closed with code:' + event.code +
            ' reason:' + event.reason);
        this.websocket_ = null;
        this.registered_ = false;
        if (this.rejectRegistration_) {
          this.rejectRegistration_(Error('WebSocket closed before registration.'));
          this.clearRegistrationPromise_();
        }
      };

      if (this.clientId_ && this.roomId_) {
        this.register(this.roomId_, this.clientId_);
      }

      resolve();
    }.bind(this);

    this.websocket_.onmessage = function(event) {
      trace('WSS->C: ' + event.data);

      var message = parseJSON(event.data);
      if (!message) {
        trace('Failed to parse WSS message: ' + event.data);
        return;
      }
      if (message.error) {
        trace('Signaling server error message: ' + message.error);
        if (this.rejectRegistration_) {
          this.rejectRegistration_(Error(message.error));
          this.clearRegistrationPromise_();
        }
        if (this.onerror) {
          this.onerror(message.error);
        }
        return;
      }
      if (message.control) {
        if (message.epoch !== undefined) {
          this.signalEpoch_ = message.epoch;
        }
        if (message.control === 'registered') {
          this.registered_ = true;
          if (this.resolveRegistration_) {
            this.resolveRegistration_(message);
            this.clearRegistrationPromise_();
          }
        }
        if (this.oncontrol) {
          this.oncontrol(message);
        }
        return;
      }
      if (message.msg !== undefined && this.onmessage) {
        this.onmessage(message.msg);
      }
    }.bind(this);

    this.websocket_.onerror = function() {
      reject(Error('WebSocket error.'));
    };
  }.bind(this));
};

SignalingChannel.prototype.configureV2 = function(admissionToken, signalEpoch) {
  this.admissionToken_ = admissionToken;
  this.signalEpoch_ = signalEpoch;
};

SignalingChannel.prototype.register = function(roomId, clientId) {
  if (this.registered_) {
    trace('ERROR: SignalingChannel has already registered.');
    return Promise.resolve();
  }

  this.roomId_ = roomId;
  this.clientId_ = clientId;

  if (!this.roomId_) {
    trace('ERROR: missing roomId.');
  }
  if (!this.clientId_) {
    trace('ERROR: missing clientId.');
  }
  if (!this.websocket_ || this.websocket_.readyState !== WebSocket.OPEN) {
    trace('WebSocket not open yet; saving the IDs to register later.');
    return Promise.resolve();
  }
  trace('Registering signaling channel.');
  var registerMessage = {
    cmd: 'register',
    roomid: this.roomId_,
    clientid: this.clientId_
  };
  if (this.signalingVersion_ === 2) {
    if (!this.admissionToken_) {
      return Promise.reject(Error('Missing V2 admission token.'));
    }
    registerMessage.ver = 2;
    registerMessage.token = this.admissionToken_;
  }
  this.websocket_.send(JSON.stringify(registerMessage));
  if (this.signalingVersion_ === 1) {
    this.registered_ = true;
    trace('Signaling channel registered.');
    return Promise.resolve();
  }
  if (!this.registrationPromise_) {
    this.registrationPromise_ = new Promise(function(resolve, reject) {
      this.resolveRegistration_ = resolve;
      this.rejectRegistration_ = reject;
    }.bind(this));
  }
  return this.registrationPromise_;
};

SignalingChannel.prototype.clearRegistrationPromise_ = function() {
  this.registrationPromise_ = null;
  this.resolveRegistration_ = null;
  this.rejectRegistration_ = null;
};

SignalingChannel.prototype.close = function(async) {
  if (this.websocket_) {
    this.websocket_.close();
    this.websocket_ = null;
  }

  if (!this.clientId_ || !this.roomId_) {
    return Promise.resolve();
  }
  if (this.signalingVersion_ === 2) {
    this.clientId_ = null;
    this.roomId_ = null;
    this.registered_ = false;
    this.clearRegistrationPromise_();
    return Promise.resolve();
  }
  // Tell the V1 WebSocket POST fallback that we're done.
  var path = this.getWssPostUrl();

  return sendUrlRequest('DELETE', path, async).catch(function(error) {
    trace('Error deleting web socket connection: ' + error.message);
  }.bind(this)).then(function() {
    this.clientId_ = null;
    this.roomId_ = null;
    this.registered_ = false;
  }.bind(this));
};

SignalingChannel.prototype.send = function(message) {
  if (!this.roomId_ || !this.clientId_) {
    trace('ERROR: SignalingChannel has not registered.');
    return;
  }
  trace('C->WSS: ' + message);

  var wssMessage = {
    cmd: 'send',
    msg: message
  };
  if (this.signalingVersion_ === 2) {
    if (this.signalEpoch_ === null || this.signalEpoch_ === undefined) {
      trace('ERROR: SignalingChannel has no V2 signal epoch.');
      return;
    }
    wssMessage.epoch = this.signalEpoch_.toString();
  }
  var msgString = JSON.stringify(wssMessage);

  if (this.websocket_ && this.websocket_.readyState === WebSocket.OPEN) {
    this.websocket_.send(msgString);
  } else if (this.signalingVersion_ === 1) {
    var path = this.getWssPostUrl();
    var xhr = new XMLHttpRequest();
    xhr.open('POST', path, true);
    xhr.send(wssMessage.msg);
  } else if (this.onerror) {
    this.onerror('V2 signaling WebSocket is not open.');
  }
};

SignalingChannel.prototype.getWssPostUrl = function() {
  return this.wssPostUrl_ + '/' + this.roomId_ + '/' + this.clientId_;
};
