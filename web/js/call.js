/*
 *  Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
 *
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the LICENSE file in the root of the source
 *  tree.
 */

/* More information about these options at jshint.com/docs/options */

/* globals trace, requestIceServers, sendUrlRequest, sendAsyncUrlRequest,
   SignalingChannel, PeerConnectionClient, setupLoopback,
   parseJSON, apprtc, Constants */

/* exported Call */

'use strict';

// How long the SFU -> P2P handoff may hold the grid waiting for direct media before it
// gives up and switches to the P2P layout anyway.
var DOWNGRADE_MEDIA_TIMEOUT_MS = 10000;

var Call = function(params) {
  this.params_ = params;
  this.roomServer_ = params.roomServer || '';

  this.channel_ = new SignalingChannel(
      params.wssUrl, params.wssPostUrl, params.signalingVersion || 1);
  this.channel_.onmessage = this.onRecvSignalingChannelMessage_.bind(this);
  this.channel_.oncontrol = this.onRecvSignalingControl_.bind(this);
  this.channel_.onerror = this.onError_.bind(this);

  this.pcClient_ = null;
  // Retained during a mode transition so the outgoing transport keeps its media on
  // screen until the incoming one is ready: the P2P client while upgrading, the SFU
  // client while downgrading.
  this.p2pPcClient_ = null;
  this.sfuPcClient_ = null;
  this.downgradeTimer_ = null;
  this.createPcClientPromise_ = null;
  this.localStream_ = null;
  this.errorMessageQueue_ = [];
  this.startTime = null;

  // Public callbacks. Keep it sorted.
  this.oncallerstarted = null;
  this.onerror = null;
  this.oniceconnectionstatechange = null;
  this.onlocalstreamadded = null;
  this.onmodechange = null;
  this.onnewicecandidate = null;
  this.onremotehangup = null;
  this.onremotesdpset = null;
  this.onremotestreamadded = null;
  this.onremotetrack = null;
  this.onsfunegotiated = null;
  this.onsignalingstatechange = null;
  this.onturnstatusmessage = null;

  this.getMediaPromise_ = null;
  this.getIceServersPromise_ = null;
  this.requestMediaAndIceServers_();
};

Call.prototype.requestMediaAndIceServers_ = function() {
  this.getMediaPromise_ = this.maybeGetMedia_();
  this.getIceServersPromise_ = this.maybeGetIceServers_();
};

Call.prototype.isInitiator = function() {
  return this.params_.isInitiator;
};

Call.prototype.getMode = function() {
  return this.params_.mode || 'p2p';
};

Call.prototype.start = function(roomId) {
  this.connectToRoom_(roomId);
  if (this.params_.isLoopback) {
    setupLoopback(this.params_.wssUrl, roomId);
  }
};

Call.prototype.restart = function() {
  // Reinitialize the promises so the media gets hooked up as a result
  // of calling maybeGetMedia_.
  this.requestMediaAndIceServers_();
  this.start(this.params_.previousRoomId);
};

Call.prototype.hangup = function(async) {
  this.startTime = null;

  if (this.localStream_) {
    if (typeof this.localStream_.getTracks === 'undefined') {
      // Support legacy browsers, like phantomJs we use to run tests.
      this.localStream_.stop();
    } else {
      this.localStream_.getTracks().forEach(function(track) {
        track.stop();
      });
    }
    this.localStream_ = null;
  }

  if (!this.params_.roomId) {
    return;
  }

  if (this.pcClient_) {
    this.pcClient_.close();
    this.pcClient_ = null;
    this.createPcClientPromise_ = null;
  }
  this.closeRetainedPcClients_();

  // Remove membership before closing signaling. V1 then sends its legacy BYE;
  // V2 relies on the authority's p2p-promote control to notify the survivor.

  // This section of code is executed in both sync and async depending on
  // where it is called from. When the browser is closed, the requests must
  // be executed as sync to finish before the browser closes. When called
  // from pressing the hang up button, the requests are executed async.

  var steps = [];
  steps.push({
    step: function() {
      // Send POST request to /leave.
      var path = this.getLeaveUrl_();
      var headers = this.params_.signalingVersion === 2 ? {
        Authorization: 'Bearer ' + this.params_.admissionToken
      } : null;
      return sendUrlRequest('POST', path, async, undefined, headers);
    }.bind(this),
    errorString: 'Error sending /leave:'
  });
  if (this.params_.signalingVersion !== 2) {
    steps.push({
      step: function() {
        // Send bye to the other V1 client.
        this.channel_.send(JSON.stringify({type: 'bye'}));
      }.bind(this),
      errorString: 'Error sending bye:'
    });
  }
  steps.push({
    step: function() {
      // Close signaling channel.
      return this.channel_.close(async);
    }.bind(this),
    errorString: 'Error closing signaling channel:'
  });
  steps.push({
    step: function() {
      this.params_.previousRoomId = this.params_.roomId;
      this.params_.roomId = null;
      this.params_.clientId = null;
    }.bind(this),
    errorString: 'Error setting params:'
  });

  if (async) {
    var errorHandler = function(errorString, error) {
      trace(errorString + ' ' + error.message);
    };
    var promise = Promise.resolve();
    for (var i = 0; i < steps.length; ++i) {
      promise = promise.then(steps[i].step).catch(
          errorHandler.bind(this, steps[i].errorString));
    }

    return promise;
  }
  // Execute the cleanup steps.
  var executeStep = function(executor, errorString) {
    try {
      executor();
    } catch (ex) {
      trace(errorString + ' ' + ex);
    }
  };

  for (var j = 0; j < steps.length; ++j) {
    executeStep(steps[j].step, steps[j].errorString);
  }

  if (this.params_.roomId !== null || this.params_.clientId !== null) {
    trace('ERROR: sync cleanup tasks did not complete successfully.');
  } else {
    trace('Cleanup completed.');
  }
  return Promise.resolve();
};

Call.prototype.getLeaveUrl_ = function() {
  return this.roomServer_ +
      (this.params_.signalingVersion === 2 ? '/v2/leave/' : '/leave/') +
      this.params_.roomId +
      '/' + this.params_.clientId;
};

Call.prototype.onRemoteHangup = function() {
  this.startTime = null;

  // On remote hangup this client becomes the new initiator.
  this.params_.isInitiator = true;

  if (this.pcClient_) {
    this.pcClient_.close();
    this.pcClient_ = null;
    this.createPcClientPromise_ = null;
  }

  this.startSignaling_();
};

Call.prototype.getPeerConnectionStates = function() {
  if (!this.pcClient_) {
    return null;
  }
  return this.pcClient_.getPeerConnectionStates();
};

Call.prototype.getPeerConnectionStats = function(callback) {
  if (!this.pcClient_) {
    return;
  }
  this.pcClient_.getPeerConnectionStats(callback);
};

Call.prototype.toggleVideoMute = function() {
  var videoTracks = this.localStream_.getVideoTracks();
  if (videoTracks.length === 0) {
    trace('No local video available.');
    return;
  }

  trace('Toggling video mute state.');
  for (var i = 0; i < videoTracks.length; ++i) {
    videoTracks[i].enabled = !videoTracks[i].enabled;
  }
  trace('Video ' + (videoTracks[0].enabled ? 'unmuted.' : 'muted.'));
};

Call.prototype.toggleAudioMute = function() {
  var audioTracks = this.localStream_.getAudioTracks();
  if (audioTracks.length === 0) {
    trace('No local audio available.');
    return;
  }

  trace('Toggling audio mute state.');
  for (var i = 0; i < audioTracks.length; ++i) {
    audioTracks[i].enabled = !audioTracks[i].enabled;
  }
  trace('Audio ' + (audioTracks[0].enabled ? 'unmuted.' : 'muted.'));
};

// Connects client to the room. This happens by simultaneously requesting
// media, requesting turn, and join the room. Once all three of those
// tasks is complete, the signaling process begins. At the same time, a
// WebSocket connection is opened using |wss_url| followed by a subsequent
// registration once HTTP admission completes.
Call.prototype.connectToRoom_ = function(roomId) {
  this.params_.roomId = roomId;
  // Asynchronously open a WebSocket connection to WSS.
  // TODO(jiayl): We don't need to wait for the signaling channel to open before
  // start signaling.
  var channelPromise = this.channel_.open().catch(function(error) {
    this.onError_('WebSocket open error: ' + error.message);
    return Promise.reject(error);
  }.bind(this));

  // Asynchronously join the room.
  var joinPromise =
      this.joinRoom_().then(function(roomParams) {
        // The only difference in parameters should be clientId and isInitiator,
        // and the turn servers that we requested.
        // TODO(tkchin): clean up response format. JSHint doesn't like it.

        this.params_.clientId = roomParams.client_id;
        this.params_.roomId = roomParams.room_id;
        this.params_.roomLink = roomParams.room_link;
        this.params_.isInitiator = roomParams.is_initiator === true ||
            roomParams.is_initiator === 'true';

        this.params_.messages = roomParams.messages || [];
        if (this.params_.signalingVersion === 2) {
          this.params_.mode = roomParams.mode;
          this.params_.signalEpoch = roomParams.epoch;
          this.params_.admissionToken = roomParams.admission_token;
          this.channel_.configureV2(
              this.params_.admissionToken, this.params_.signalEpoch);
          if (this.params_.mode === 'sfu') {
            this.params_.sfuMode = true;
            this.params_.isInitiator = true;
          }
        }
      }.bind(this)).catch(function(error) {
        this.onError_('Room server join error: ' + error.message);
        return Promise.reject(error);
      }.bind(this));

  // Register only after both the WebSocket and HTTP admission are ready.
  Promise.all([channelPromise, joinPromise]).then(function() {
    return this.channel_.register(
        this.params_.roomId, this.params_.clientId).then(function(control) {
      if (control && control.control === 'registered') {
        this.params_.signalEpoch = control.epoch;
        this.params_.mode = control.mode || this.params_.mode;
        if (this.params_.mode === 'sfu') {
          this.params_.sfuMode = true;
          this.params_.isInitiator = true;
        } else {
          this.params_.isInitiator = control.is_initiator === true;
        }
      }
      // V2 waits for the authoritative registered snapshot before allowing
      // offer/answer/candidate production.
      return Promise.all([this.getIceServersPromise_, this.getMediaPromise_])
          .then(function() {
            if (this.params_.mode === 'sfu') {
              this.params_.mode = 'upgrading';
              if (this.onmodechange) {
                this.onmodechange('upgrading');
              }
            }
            this.startSignaling_();
          }.bind(this));
    }.bind(this));
  }.bind(this)).catch(function(error) {
    this.onError_('WebSocket register error: ' + error.message);
  }.bind(this));
};

// Asynchronously request user media if needed.
Call.prototype.maybeGetMedia_ = function() {
  // mediaConstraints.audio and mediaConstraints.video could be objects, so
  // check '!=== false' instead of '=== true'.
  var needStream = (this.params_.mediaConstraints.audio !== false ||
                    this.params_.mediaConstraints.video !== false);
  var mediaPromise = null;
  if (needStream) {
    var mediaConstraints = this.params_.mediaConstraints;

    mediaPromise = navigator.mediaDevices.getUserMedia(mediaConstraints)
        .catch(function(error) {
          if (error.name !== 'NotFoundError') {
            throw error;
          }
          return navigator.mediaDevices.enumerateDevices()
              .then(function(devices) {
                var cam = devices.find(function(device) {
                  return device.kind === 'videoinput';
                });
                var mic = devices.find(function(device) {
                  return device.kind === 'audioinput';
                });
                var constraints = {
                  video: cam && mediaConstraints.video,
                  audio: mic && mediaConstraints.audio
                };
                return navigator.mediaDevices.getUserMedia(constraints);
              });
        })
        .then(function(stream) {
          trace('Got access to local media with mediaConstraints:\n' +
          '  \'' + JSON.stringify(mediaConstraints) + '\'');

          this.onUserMediaSuccess_(stream);
        }.bind(this)).catch(function(error) {
          this.onError_('Error getting user media: ' + error.message);
          this.onUserMediaError_(error);
        }.bind(this));
  } else {
    mediaPromise = Promise.resolve();
  }
  return mediaPromise;
};

// Asynchronously request an ICE server if needed.
Call.prototype.maybeGetIceServers_ = function() {
  var shouldRequestIceServers =
      (this.params_.iceServerRequestUrl &&
      this.params_.iceServerRequestUrl.length > 0 &&
      this.params_.peerConnectionConfig.iceServers &&
      this.params_.peerConnectionConfig.iceServers.length === 0);

  var iceServerPromise = null;
  if (shouldRequestIceServers) {
    var requestUrl = this.params_.iceServerRequestUrl;
    iceServerPromise =
        requestIceServers(requestUrl, this.params_.iceServerTransports).then(
            function(iceServers) {
              var servers = this.params_.peerConnectionConfig.iceServers;
              this.params_.peerConnectionConfig.iceServers =
              servers.concat(iceServers);
            }.bind(this)).catch(function(error) {
          if (this.onturnstatusmessage) {
            // Error retrieving ICE servers.
            var subject =
                encodeURIComponent('AppRTC demo ICE servers not working');
            this.onturnstatusmessage(
                'No TURN server; unlikely that media will traverse networks. ' +
                'If this persists please ' +
                '<a href="mailto:discuss-webrtc@googlegroups.com?' +
                'subject=' + subject + '">' +
                'report it to discuss-webrtc@googlegroups.com</a>.');
          }
          trace(error.message);
        }.bind(this));
  } else {
    iceServerPromise = Promise.resolve();
  }
  return iceServerPromise;
};

Call.prototype.onUserMediaSuccess_ = function(stream) {
  this.localStream_ = stream;
  if (this.onlocalstreamadded) {
    this.onlocalstreamadded(stream);
  }
};

Call.prototype.onUserMediaError_ = function(error) {
  var errorMessage = 'Failed to get access to local media. Error name was ' +
      error.name + '. Continuing without sending a stream.';
  this.onError_('getUserMedia error: ' + errorMessage);
  this.errorMessageQueue_.push(error);
  alert(errorMessage);
};

Call.prototype.maybeCreatePcClientAsync_ = function() {
  if (this.pcClient_) {
    return Promise.resolve();
  }
  if (this.createPcClientPromise_) {
    return this.createPcClientPromise_;
  }
  this.createPcClientPromise_ = new Promise(function(resolve, reject) {
    if (typeof RTCPeerConnection.generateCertificate === 'function') {
      var certParams = {name: 'ECDSA', namedCurve: 'P-256'};
      RTCPeerConnection.generateCertificate(certParams)
          .then(function(cert) {
            trace('ECDSA certificate generated successfully.');
            this.params_.peerConnectionConfig.certificates = [cert];
            this.createPcClient_();
            resolve();
          }.bind(this))
          .catch(function(error) {
            trace('ECDSA certificate generation failed.');
            reject(error);
          });
    } else {
      this.createPcClient_();
      resolve();
    }
  }.bind(this));
  return this.createPcClientPromise_;
};

Call.prototype.createPcClient_ = function() {
  this.pcClient_ = new PeerConnectionClient(this.params_, this.startTime);
  this.pcClient_.onsignalingmessage = this.sendSignalingMessage_.bind(this);
  this.pcClient_.onremotehangup = this.onremotehangup;
  this.pcClient_.onremotesdpset = this.onremotesdpset;
  this.pcClient_.onremotestreamadded = this.onremotestreamadded;
  this.pcClient_.onremotetrack = this.onremotetrack;
  this.pcClient_.onsfunegotiated = this.onsfunegotiated;
  this.pcClient_.onsignalingstatechange = this.onsignalingstatechange;
  this.pcClient_.oniceconnectionstatechange = this.oniceconnectionstatechange;
  if (this.params_.sfuMode) {
    this.pcClient_.oniceconnectionstatechange =
        this.onSfuIceConnectionStateChange_.bind(this);
  } else if (this.params_.mode === 'downgrading') {
    this.pcClient_.oniceconnectionstatechange =
        this.onDowngradeIceConnectionStateChange_.bind(this);
  }
  this.pcClient_.onnewicecandidate = this.onnewicecandidate;
  this.pcClient_.onerror = this.onerror;
  trace('Created PeerConnectionClient');
};

Call.prototype.startSignaling_ = function() {
  trace('Starting signaling.');
  if (this.isInitiator() && this.oncallerstarted) {
    this.oncallerstarted(this.params_.roomId, this.params_.roomLink);
  }

  this.startTime = window.performance.now();

  this.maybeCreatePcClientAsync_()
      .then(function() {
        if (this.localStream_) {
          trace('Adding local stream.');
          this.pcClient_.addStream(this.localStream_);
        }
        if (this.params_.isInitiator) {
          this.pcClient_.startAsCaller(this.params_.offerOptions);
        } else {
          this.pcClient_.startAsCallee(this.params_.messages);
        }
      }.bind(this))
      .catch(function(e) {
        this.onError_('Create PeerConnection exception: ' + e);
        alert('Cannot create RTCPeerConnection: ' + e.message);
      }.bind(this));
};

// Join the room and returns room parameters.
Call.prototype.joinRoom_ = function() {
  return new Promise(function(resolve, reject) {
    if (!this.params_.roomId) {
      reject(Error('Missing room id.'));
    }
    var path = this.roomServer_ +
        (this.params_.signalingVersion === 2 ? '/v2/join/' : '/join/') +
        this.params_.roomId + window.location.search;

    sendAsyncUrlRequest('POST', path).then(function(response) {
      var responseObj = parseJSON(response);
      if (!responseObj) {
        reject(Error('Error parsing response JSON.'));
        return;
      }
      if (responseObj.result !== 'SUCCESS') {
        // TODO (chuckhays) : handle room full state by returning to room
        // selection state.
        // When room is full, responseObj.result === 'FULL'
        reject(Error('Registration error: ' + responseObj.result));
        if (responseObj.result === 'FULL') {
          var getPath = this.roomServer_ +
              (this.params_.signalingVersion === 2 ? '/v2/r/' : '/r/') +
              this.params_.roomId + window.location.search;
          window.location.assign(getPath);
        }
        return;
      }
      trace('Joined the room.');
      resolve(responseObj.params);
    }.bind(this)).catch(function(error) {
      reject(Error('Failed to join the room: ' + error.message));
      return;
    }.bind(this));
  }.bind(this));
};

Call.prototype.onRecvSignalingChannelMessage_ = function(msg) {
  this.maybeCreatePcClientAsync_()
      .then(function() {
        this.pcClient_.receiveSignalingMessage(msg);
      }.bind(this));
};

Call.prototype.onRecvSignalingControl_ = function(control) {
  if (control.epoch !== undefined) {
    this.params_.signalEpoch = control.epoch;
  }
  if (control.control === 'p2p-promote') {
    this.params_.isInitiator = control.is_initiator === true;
    if (this.onremotehangup) {
      this.onremotehangup();
    } else {
      this.onRemoteHangup();
    }
  } else if (control.control === 'sfu-upgrade') {
    this.startSfuUpgrade_();
  } else if (control.control === 'sfu-downgrade') {
    this.startSfuDowngrade_(control);
  } else if (control.control === 'room-failed') {
    this.onError_('SFU room failed: ' + (control.reason || 'worker unavailable'));
  }
};

Call.prototype.startSfuUpgrade_ = function() {
  if (this.params_.sfuMode || this.params_.mode === 'upgrading') {
    return;
  }
  trace('Starting P2P to SFU upgrade at epoch ' + this.params_.signalEpoch + '.');
  this.params_.mode = 'upgrading';
  this.params_.sfuMode = true;
  this.params_.isInitiator = true;
  this.p2pPcClient_ = this.pcClient_;
  this.pcClient_ = null;
  this.createPcClientPromise_ = null;
  if (this.onmodechange) {
    this.onmodechange('upgrading');
  }
  this.startTime = window.performance.now();
  this.maybeCreatePcClientAsync_().then(function() {
    if (this.localStream_) {
      this.pcClient_.addStream(this.localStream_);
    }
    this.pcClient_.startAsCaller(this.params_.offerOptions);
  }.bind(this)).catch(function(error) {
    this.onError_('SFU upgrade failed: ' + error.message);
  }.bind(this));
};

// SFU -> P2P downgrade: the room shrank to two members, so signaling committed direct
// P2P and told us to leave the SFU. This mirrors the upgrade handoff. Signaling is
// break-before-make (it has already retired both SFU legs on the worker), but the browser
// is not: the SFU peer connection is retained so its last received frames keep the grid
// populated while the direct P2P connection negotiates with the same local tracks. The
// elected initiator offers; the other answers. The SFU client is closed and the layout
// switches only once direct media is ready.
Call.prototype.startSfuDowngrade_ = function(control) {
  if (this.params_.mode === 'p2p' || this.params_.mode === 'downgrading') {
    return;
  }
  trace('Starting SFU to P2P downgrade at epoch ' + this.params_.signalEpoch + '.');
  this.params_.mode = 'downgrading';
  // The new connection is a direct peer connection, so it must not use the SFU's
  // always-polite negotiation role.
  this.params_.sfuMode = false;
  this.params_.isInitiator = control.is_initiator === true;
  // Retain the SFU client for media continuity, and drop any P2P client still held from
  // an upgrade handoff that never completed — it belongs to a retired epoch.
  this.sfuPcClient_ = this.pcClient_;
  this.retireSfuClientCallbacks_();
  if (this.p2pPcClient_) {
    this.p2pPcClient_.close();
    this.p2pPcClient_ = null;
  }
  this.pcClient_ = null;
  this.createPcClientPromise_ = null;
  if (this.onmodechange) {
    this.onmodechange('downgrading');
  }
  // The peer may never answer (it can leave during the handoff), and a retained SFU
  // client shows nothing but frozen frames. Bound the transition so the UI cannot sit
  // in "switching" forever.
  this.startDowngradeTimer_();
  this.startTime = window.performance.now();
  this.maybeCreatePcClientAsync_().then(function() {
    if (this.localStream_) {
      this.pcClient_.addStream(this.localStream_);
    }
    if (this.params_.isInitiator) {
      this.pcClient_.startAsCaller(this.params_.offerOptions);
    } else {
      this.pcClient_.startAsCallee(this.params_.messages);
    }
  }.bind(this)).catch(function(error) {
    this.finishSfuDowngrade_();
    this.onError_('SFU downgrade failed: ' + error.message);
  }.bind(this));
};

Call.prototype.onSfuIceConnectionStateChange_ = function() {
  var states = this.pcClient_ && this.pcClient_.getPeerConnectionStates();
  if (states && (states.iceConnectionState === 'connected' ||
      states.iceConnectionState === 'completed')) {
    this.params_.mode = 'sfu';
    if (this.p2pPcClient_) {
      this.p2pPcClient_.close();
      this.p2pPcClient_ = null;
    }
    if (this.onmodechange) {
      this.onmodechange('sfu');
    }
  }
  if (this.oniceconnectionstatechange) {
    this.oniceconnectionstatechange();
  }
};

// The downgrade counterpart of onSfuIceConnectionStateChange_: the direct P2P connection
// is up, so the retained SFU client can go and the grid can give way to the P2P stage.
Call.prototype.onDowngradeIceConnectionStateChange_ = function() {
  var states = this.pcClient_ && this.pcClient_.getPeerConnectionStates();
  if (states && (states.iceConnectionState === 'connected' ||
      states.iceConnectionState === 'completed')) {
    this.finishSfuDowngrade_();
  }
  if (this.oniceconnectionstatechange) {
    this.oniceconnectionstatechange();
  }
};

Call.prototype.startDowngradeTimer_ = function() {
  this.clearDowngradeTimer_();
  this.downgradeTimer_ = window.setTimeout(function() {
    this.downgradeTimer_ = null;
    if (this.params_.mode === 'downgrading') {
      trace('Direct P2P media did not arrive; completing the downgrade anyway.');
      this.finishSfuDowngrade_();
    }
  }.bind(this), DOWNGRADE_MEDIA_TIMEOUT_MS);
};

Call.prototype.clearDowngradeTimer_ = function() {
  if (this.downgradeTimer_) {
    window.clearTimeout(this.downgradeTimer_);
    this.downgradeTimer_ = null;
  }
};

// Complete the SFU -> P2P transition exactly once: hand the layout back to the full-screen
// P2P stage. The retained SFU client is *not* closed here — closing it ends its remote
// tracks, which empties the very grid tiles the UI is still showing. The UI releases it
// through releaseRetiredSfuTransport() once the P2P layout has taken over.
Call.prototype.finishSfuDowngrade_ = function() {
  if (this.params_.mode !== 'downgrading') {
    return;
  }
  this.clearDowngradeTimer_();
  this.params_.mode = 'p2p';
  if (this.onmodechange) {
    this.onmodechange('p2p');
  } else {
    this.releaseRetiredSfuTransport();
  }
};

// Called by the UI when the retired SFU connection's frames are no longer on screen.
Call.prototype.releaseRetiredSfuTransport = function() {
  if (this.sfuPcClient_) {
    this.sfuPcClient_.close();
    this.sfuPcClient_ = null;
  }
};

// The retained SFU client is kept only so its already-received frames stay on screen. Its
// callbacks still point at live handlers that read this.pcClient_ — which is now the direct
// P2P client — so a late ICE or signaling event from the retired transport could otherwise
// flip the session back to SFU mode or emit a retired-epoch frame. Silence it.
Call.prototype.retireSfuClientCallbacks_ = function() {
  if (!this.sfuPcClient_) {
    return;
  }
  this.sfuPcClient_.oniceconnectionstatechange = null;
  this.sfuPcClient_.onsignalingmessage = null;
  this.sfuPcClient_.onsignalingstatechange = null;
  this.sfuPcClient_.onremotehangup = null;
  this.sfuPcClient_.onremotesdpset = null;
  this.sfuPcClient_.onremotestreamadded = null;
  this.sfuPcClient_.onremotetrack = null;
  this.sfuPcClient_.onsfunegotiated = null;
  this.sfuPcClient_.onnewicecandidate = null;
};

Call.prototype.closeRetainedPcClients_ = function() {
  this.clearDowngradeTimer_();
  if (this.p2pPcClient_) {
    this.p2pPcClient_.close();
    this.p2pPcClient_ = null;
  }
  this.releaseRetiredSfuTransport();
};

Call.prototype.sendSignalingMessage_ = function(message) {
  var msgString = JSON.stringify(message);
  if (this.params_.signalingVersion === 2) {
    this.channel_.send(msgString);
    return;
  }
  if (this.params_.isInitiator) {
    // Initiator posts all messages to GAE. GAE will either store the messages
    // until the other client connects, or forward the message to Collider if
    // the other client is already connected.
    // Must append query parameters in case we've specified alternate WSS url.
    var path = this.roomServer_ + '/message/' + this.params_.roomId +
        '/' + this.params_.clientId + window.location.search;
    var xhr = new XMLHttpRequest();
    xhr.open('POST', path, true);
    xhr.send(msgString);
    trace('C->GAE: ' + msgString);
  } else {
    this.channel_.send(msgString);
  }
};

Call.prototype.onError_ = function(message) {
  if (this.onerror) {
    this.onerror(message);
  }
};
