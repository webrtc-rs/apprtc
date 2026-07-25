/*
 *  Copyright (c) 2016 The WebRTC project authors. All Rights Reserved.
 *
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the LICENSE file in the root of the source
 *  tree.
 */

/* More information about these options at jshint.com/docs/options */

/* globals trace, mergeConstraints, parseJSON, iceCandidateType,
   maybePreferAudioReceiveCodec, maybePreferVideoReceiveCodec,
   maybePreferAudioSendCodec, maybePreferVideoSendCodec,
   maybeSetAudioSendBitRate, maybeSetVideoSendBitRate,
   maybeSetAudioReceiveBitRate, maybeSetVideoSendInitialBitRate,
   maybeSetVideoReceiveBitRate, maybeSetVideoSendInitialBitRate,
   maybeRemoveVideoFec, maybeSetOpusOptions, DOMException */

/* exported PeerConnectionClient */

// TODO(jansson) disabling for now since we are going replace JSHINT.
// (It does not say where the strict violation is hence it's not worth fixing.).
// jshint strict:false

'use strict';

var PeerConnectionClient = function(params, startTime) {
  this.params_ = params;
  this.startTime_ = startTime;

  trace('Creating RTCPeerConnnection with:\n' +
    '  config: \'' + JSON.stringify(params.peerConnectionConfig) + '\';\n' +
    '  constraints: \'' + JSON.stringify(params.peerConnectionConstraints) +
    '\'.');

  // Create an RTCPeerConnection via the polyfill (adapter.js).
  this.pc_ = new RTCPeerConnection(
      params.peerConnectionConfig, params.peerConnectionConstraints);
  this.pc_.onicecandidate = this.onIceCandidate_.bind(this);
  this.pc_.ontrack = this.onRemoteStreamAdded_.bind(this);
  this.pc_.onremovestream = trace.bind(null, 'Remote stream removed.');
  this.pc_.onsignalingstatechange = this.onSignalingStateChanged_.bind(this);
  this.pc_.oniceconnectionstatechange =
      this.onIceConnectionStateChanged_.bind(this);
  this.pc_.onnegotiationneeded = this.onNegotiationNeeded_.bind(this);
  window.dispatchEvent(new CustomEvent('pccreated', {
    detail: {
      pc: this,
      time: new Date(),
      userId: this.params_.roomId + (this.isInitiator_ ? '-0' : '-1'),
      sessionId: this.params_.roomId
    }
  }));

  this.hasRemoteSdp_ = false;
  this.isDrainingMessages_ = false;
  this.messageQueue_ = [];
  this.isInitiator_ = false;
  this.started_ = false;
  this.sfuMode_ = params.sfuMode === true;
  this.pendingRemoteRequestId_ = null;
  this.perfectNegotiation_ = params.signalingVersion === 2;
  this.polite_ = false;
  this.makingOffer_ = false;
  this.ignoreOffer_ = false;
  this.isSettingRemoteAnswerPending_ = false;
  this.offerRevision_ = 0;
  this.renegotiationPending_ = false;

  // TODO(jiayl): Replace callbacks with events.
  // Public callbacks. Keep it sorted.
  this.onerror = null;
  this.oniceconnectionstatechange = null;
  this.onnewicecandidate = null;
  this.onremotehangup = null;
  this.onremotesdpset = null;
  this.onremotestreamadded = null;
  this.onremotetrack = null;
  this.onsfunegotiated = null;
  this.onsignalingmessage = null;
  this.onsignalingstatechange = null;
};

// Set up audio and video regardless of what devices are present.
// Disable comfort noise for maximum audio quality.
PeerConnectionClient.DEFAULT_SDP_OFFER_OPTIONS_ = {
  offerToReceiveAudio: 1,
  offerToReceiveVideo: 1,
  voiceActivityDetection: false
};

PeerConnectionClient.prototype.addStream = function(stream) {
  if (!this.pc_) {
    return;
  }
  // Use the standard addTrack rather than the legacy addStream: Chrome keeps
  // addStream for backwards compatibility, but Safari only implements addTrack.
  stream.getTracks().forEach(function(track) {
    this.pc_.addTrack(track, stream);
  }.bind(this));
};

PeerConnectionClient.prototype.startAsCaller = function(offerOptions) {
  if (!this.pc_) {
    return false;
  }

  if (this.started_) {
    return false;
  }

  this.isInitiator_ = true;
  this.started_ = true;
  // The P2P initiator is impolite. A browser connected to the SFU is polite
  // because it must yield to authoritative subscribe offers from the SFU.
  this.polite_ = this.perfectNegotiation_ && this.sfuMode_;
  var constraints = mergeConstraints(
      PeerConnectionClient.DEFAULT_SDP_OFFER_OPTIONS_, offerOptions);
  trace('Sending offer to peer, with constraints: \n\'' +
      JSON.stringify(constraints) + '\'.');
  this.makeOffer_(constraints)
      .catch(this.onError_.bind(this, 'createOffer'));

  return true;
};

PeerConnectionClient.prototype.startAsCallee = function(initialMessages) {
  if (!this.pc_) {
    return false;
  }

  if (this.started_) {
    return false;
  }

  this.isInitiator_ = false;
  this.started_ = true;
  this.polite_ = this.perfectNegotiation_;

  if (initialMessages && initialMessages.length > 0) {
    // Convert received messages to JSON objects and add them to the message
    // queue.
    for (var i = 0, len = initialMessages.length; i < len; i++) {
      this.receiveSignalingMessage(initialMessages[i]);
    }
    return true;
  }

  // We may have queued messages received from the signaling channel before
  // started.
  if (this.messageQueue_.length > 0) {
    this.drainMessageQueue_();
  }
  return true;
};

PeerConnectionClient.prototype.receiveSignalingMessage = function(message) {
  var messageObj = parseJSON(message);
  if (!messageObj) {
    return;
  }
  if (this.perfectNegotiation_ &&
      (messageObj.type === 'offer' || messageObj.type === 'answer')) {
    this.hasRemoteSdp_ = true;
    // V2 WebSocket delivery is ordered. Preserve description order so a
    // publish answer is applied before a following SFU subscribe offer.
    this.messageQueue_.push(messageObj);
  } else if (this.sfuMode_ && messageObj.type === 'offer') {
    this.hasRemoteSdp_ = true;
    // SFU subscribe offers follow the publish answer on the same ordered
    // signaling connection. Preserve that ordering so the subscribe offer
    // cannot roll back a publish offer whose answer is still being applied.
    this.messageQueue_.push(messageObj);
  } else if ((this.isInitiator_ && messageObj.type === 'answer') ||
      (!this.isInitiator_ && messageObj.type === 'offer')) {
    this.hasRemoteSdp_ = true;
    // Always process the initial remote SDP before candidates.
    this.messageQueue_.unshift(messageObj);
  } else if (messageObj.type === 'candidate' ||
      messageObj.type === 'end-of-candidates') {
    this.messageQueue_.push(messageObj);
  } else if (messageObj.type === 'bye') {
    if (this.onremotehangup) {
      this.onremotehangup();
    }
  }
  this.drainMessageQueue_();
};

PeerConnectionClient.prototype.close = function() {
  if (!this.pc_) {
    return;
  }

  this.pc_.close();
  window.dispatchEvent(new CustomEvent('pcclosed', {
    detail: {
      pc: this,
      time: new Date(),
    }
  }));
  this.pc_ = null;
};

PeerConnectionClient.prototype.getPeerConnectionStates = function() {
  if (!this.pc_) {
    return null;
  }
  return {
    'signalingState': this.pc_.signalingState,
    'iceGatheringState': this.pc_.iceGatheringState,
    'iceConnectionState': this.pc_.iceConnectionState
  };
};

PeerConnectionClient.prototype.getPeerConnectionStats = function(callback) {
  if (!this.pc_) {
    return;
  }
  this.pc_.getStats(null)
      .then(callback);
};

// SFU mode only: constrain every transceiver to VP8 (video) / Opus (audio) via
// setCodecPreferences, so this client publishes and receives only the codec the SFU forwards —
// no client can negotiate a codec (VP9/H264/G722/...) the SFU does not forward. This is the
// setCodecPreferences equivalent of the SFU chat sample's setupCodecs(); it must run before
// createOffer/createAnswer so the preference shapes the generated m= lines. A no-op where the
// browser lacks setCodecPreferences / getCapabilities.
PeerConnectionClient.prototype.setupCodecs_ = function() {
  if (!this.sfuMode_ || !this.pc_ ||
      typeof RTCRtpSender === 'undefined' ||
      !RTCRtpSender.getCapabilities) {
    return;
  }
  var videoCodecs = RTCRtpSender.getCapabilities('video').codecs
      .filter(function(codec) {
        return codec.mimeType.toLowerCase() === 'video/vp8';
      });
  var audioCodecs = RTCRtpSender.getCapabilities('audio').codecs
      .filter(function(codec) {
        return codec.mimeType.toLowerCase() === 'audio/opus';
      });
  this.pc_.getTransceivers().forEach(function(transceiver) {
    if (!transceiver.setCodecPreferences) {
      return;
    }
    // A transceiver always has both a sender and a receiver; use whichever carries the track so
    // the kind is known for send-only (publish) and recv-only (forward) transceivers alike.
    var kind = (transceiver.sender && transceiver.sender.track &&
                transceiver.sender.track.kind) ||
               (transceiver.receiver && transceiver.receiver.track &&
                transceiver.receiver.track.kind);
    try {
      if (kind === 'video' && videoCodecs.length > 0) {
        transceiver.setCodecPreferences(videoCodecs);
      } else if (kind === 'audio' && audioCodecs.length > 0) {
        transceiver.setCodecPreferences(audioCodecs);
      }
    } catch (e) {
      trace('setCodecPreferences failed: ' + e);
    }
  });
};

PeerConnectionClient.prototype.doAnswer_ = function() {
  trace('Sending answer to peer.');
  this.setupCodecs_();
  return this.pc_.createAnswer()
      .then(this.setLocalSdpAndNotify_.bind(this))
      .then(this.notifySfuNegotiated_.bind(this));
};

// After answering an SFU (re-)offer, the negotiated transceiver directions reflect the room's
// current publish state. Report the ids of the forwarded tracks the SFU is still sending us so the
// UI can drop the tiles of any peer whose media is gone (e.g. it left the room). A departed peer's
// transceiver flips to 'inactive' rather than firing the remote track's 'ended', so this
// reconciliation — not track events — is what removes the stale tile. No-op outside SFU mode.
PeerConnectionClient.prototype.notifySfuNegotiated_ = function() {
  if (!this.sfuMode_ || !this.pc_ || !this.onsfunegotiated) {
    return;
  }
  var live = {};
  this.pc_.getTransceivers().forEach(function(transceiver) {
    var track = transceiver.receiver && transceiver.receiver.track;
    if (track && (transceiver.currentDirection === 'recvonly' ||
        transceiver.currentDirection === 'sendrecv')) {
      live[track.id] = true;
    }
  });
  this.onsfunegotiated(live);
};

PeerConnectionClient.prototype.makeOffer_ = function(offerOptions) {
  if (!this.pc_ || this.makingOffer_) {
    return Promise.resolve();
  }
  this.makingOffer_ = true;
  this.setupCodecs_();
  var revision = ++this.offerRevision_;
  return this.pc_.createOffer(offerOptions).then(function(offer) {
    // A polite V2 peer may receive and accept a remote offer while createOffer
    // is pending. In that case this local offer has been superseded.
    if (revision !== this.offerRevision_ ||
        (this.perfectNegotiation_ && this.pc_.signalingState !== 'stable')) {
      trace('Discarding superseded local offer.');
      return;
    }
    return this.setLocalSdpAndNotify_(offer);
  }.bind(this)).finally(function() {
    this.makingOffer_ = false;
    this.maybeRenegotiate_();
  }.bind(this));
};

PeerConnectionClient.prototype.setLocalSdpAndNotify_ =
    function(sessionDescription) {
      sessionDescription.sdp = maybeSetOpusOptions(
          sessionDescription.sdp,
          this.params_);
      sessionDescription.sdp = maybePreferAudioReceiveCodec(
          sessionDescription.sdp,
          this.params_);
      sessionDescription.sdp = maybePreferVideoReceiveCodec(
          sessionDescription.sdp,
          this.params_);
      sessionDescription.sdp = maybeSetAudioReceiveBitRate(
          sessionDescription.sdp,
          this.params_);
      sessionDescription.sdp = maybeSetVideoReceiveBitRate(
          sessionDescription.sdp,
          this.params_);
      sessionDescription.sdp = maybeRemoveVideoFec(
          sessionDescription.sdp,
          this.params_);
      return this.pc_.setLocalDescription(sessionDescription)
          .then(trace.bind(null, 'Set session description success.'))
          .then(function() {
            if (this.onsignalingmessage) {
              // Chrome version of RTCSessionDescription can't be serialized directly
              // because it JSON.stringify won't include attributes which are on the
              // object's prototype chain. By creating the message to serialize
              // explicitly we can avoid the issue.
              var message = {
                sdp: sessionDescription.sdp,
                type: sessionDescription.type
              };
              if (sessionDescription.type === 'answer' &&
                  this.pendingRemoteRequestId_ !== null) {
                message.requestid = this.pendingRemoteRequestId_.toString();
                this.pendingRemoteRequestId_ = null;
              }
              this.onsignalingmessage(message);
            }
          }.bind(this));
    };

PeerConnectionClient.prototype.setRemoteSdp_ = function(message) {
  message.sdp = maybeSetOpusOptions(message.sdp, this.params_);
  message.sdp = maybePreferAudioSendCodec(message.sdp, this.params_);
  message.sdp = maybePreferVideoSendCodec(message.sdp, this.params_);
  message.sdp = maybeSetAudioSendBitRate(message.sdp, this.params_);
  message.sdp = maybeSetVideoSendBitRate(message.sdp, this.params_);
  message.sdp = maybeSetVideoSendInitialBitRate(message.sdp, this.params_);
  message.sdp = maybeRemoveVideoFec(message.sdp, this.params_);
  return this.pc_.setRemoteDescription(new RTCSessionDescription(message))
      .then(this.onSetRemoteDescriptionSuccess_.bind(this));
};

PeerConnectionClient.prototype.onSetRemoteDescriptionSuccess_ = function() {
  trace('Set remote session description success.');
  // By now all ontrack events for the setRemoteDescription have fired, so we
  // can know if the peer has any remote video tracks that we need to wait for.
  // Otherwise, transition immediately to the active state. Use the standard
  // getReceivers rather than the legacy getRemoteStreams, which Safari dropped.
  var hasRemoteVideo = this.pc_.getReceivers().some(function(receiver) {
    return receiver.track && receiver.track.kind === 'video';
  });
  if (this.onremotesdpset) {
    this.onremotesdpset(hasRemoteVideo);
  }
};

PeerConnectionClient.prototype.processSignalingMessage_ = function(message) {
  if (this.perfectNegotiation_ &&
      (message.type === 'offer' || message.type === 'answer')) {
    return this.processV2Description_(message);
  } else if (message.type === 'offer' && this.sfuMode_) {
    return this.answerSfuOffer_(message);
  } else if (message.type === 'offer' && !this.isInitiator_) {
    if (this.pc_.signalingState !== 'stable') {
      trace('ERROR: remote offer received in unexpected state: ' +
            this.pc_.signalingState);
      return Promise.resolve();
    }
    return this.setRemoteSdp_(message).then(this.doAnswer_.bind(this));
  } else if (message.type === 'answer' && this.isInitiator_) {
    if (this.pc_.signalingState !== 'have-local-offer') {
      trace('ERROR: remote answer received in unexpected state: ' +
            this.pc_.signalingState);
      return Promise.resolve();
    }
    return this.setRemoteSdp_(message);
  } else if (message.type === 'candidate') {
    if (this.perfectNegotiation_ && this.ignoreOffer_) {
      return Promise.resolve();
    }
    var candidate = new RTCIceCandidate({
      sdpMLineIndex: message.label,
      candidate: message.candidate
    });
    this.recordIceCandidate_('Remote', candidate);
    return this.pc_.addIceCandidate(candidate)
        .then(trace.bind(null, 'Remote candidate added successfully.'));
  } else if (message.type === 'end-of-candidates') {
    if (this.perfectNegotiation_ && this.ignoreOffer_) {
      return Promise.resolve();
    }
    return this.pc_.addIceCandidate(null)
        .then(trace.bind(null, 'Remote end-of-candidates added successfully.'));
  } else {
    trace('WARNING: unexpected message: ' + JSON.stringify(message));
    return Promise.resolve();
  }
};

PeerConnectionClient.prototype.processV2Description_ = function(message) {
  var isOffer = message.type === 'offer';
  var readyForOffer = !this.makingOffer_ &&
      (this.pc_.signalingState === 'stable' ||
       this.isSettingRemoteAnswerPending_);
  var offerCollision = isOffer && !readyForOffer;
  this.ignoreOffer_ = !this.polite_ && offerCollision;
  if (this.ignoreOffer_) {
    trace('Ignoring colliding V2 offer as the impolite peer.');
    return Promise.resolve();
  }

  if (offerCollision) {
    // Invalidate createOffer work which has not reached setLocalDescription.
    ++this.offerRevision_;
    if (this.sfuMode_) {
      // The answer to an SFU subscribe offer cannot add the browser's pending
      // publish m-lines. Publish them in a new offer once this collision has
      // been resolved and the signaling state is stable again.
      this.renegotiationPending_ = true;
    }
  }
  if (this.sfuMode_ && isOffer) {
    this.pendingRemoteRequestId_ = message.requestid === undefined ?
        null : message.requestid;
  }

  var ready = Promise.resolve();
  if (offerCollision && this.pc_.signalingState !== 'stable') {
    trace('Rolling back a local V2 offer as the polite peer.');
    ready = this.pc_.setLocalDescription({type: 'rollback'});
  }
  this.isSettingRemoteAnswerPending_ = message.type === 'answer';
  return ready.then(function() {
    return this.setRemoteSdp_(message);
  }.bind(this)).then(function() {
    this.ignoreOffer_ = false;
    if (isOffer) {
      return this.doAnswer_();
    }
  }.bind(this)).finally(function() {
    this.isSettingRemoteAnswerPending_ = false;
    this.maybeRenegotiate_();
  }.bind(this));
};

PeerConnectionClient.prototype.answerSfuOffer_ = function(message) {
  var ready = Promise.resolve();
  if (this.pc_.signalingState === 'have-local-offer') {
    trace('Rolling back a local SFU offer to answer a subscribe offer.');
    ready = this.pc_.setLocalDescription({type: 'rollback'});
  } else if (this.pc_.signalingState !== 'stable') {
    trace('WARNING: SFU offer received in unexpected state: ' +
        this.pc_.signalingState);
    return Promise.resolve();
  }
  this.pendingRemoteRequestId_ = message.requestid === undefined ?
      null : message.requestid;
  return ready.then(function() {
    return this.setRemoteSdp_(message);
  }.bind(this)).then(function() {
    return this.doAnswer_();
  }.bind(this));
};

PeerConnectionClient.prototype.onNegotiationNeeded_ = function() {
  if (!this.sfuMode_ || !this.started_ || !this.pc_ ||
      this.pc_.signalingState !== 'stable' || this.makingOffer_) {
    return;
  }
  trace('SFU negotiation needed; publishing a fresh offer.');
  this.makeOffer_(this.params_.offerOptions)
      .catch(this.onError_.bind(this, 'createOffer'));
};

PeerConnectionClient.prototype.maybeRenegotiate_ = function() {
  if (!this.renegotiationPending_ || !this.pc_ || this.makingOffer_ ||
      this.pc_.signalingState !== 'stable') {
    return;
  }
  this.renegotiationPending_ = false;
  trace('Publishing after a polite SFU offer collision.');
  this.makeOffer_(this.params_.offerOptions)
      .catch(this.onError_.bind(this, 'createOffer'));
};

// When we receive messages from GAE registration and from the WSS connection,
// we add them to a queue and drain it if conditions are right.
PeerConnectionClient.prototype.drainMessageQueue_ = function() {
  // It's possible that we finish registering and receiving messages from WSS
  // before our peer connection is created or started. We need to wait for the
  // peer connection to be created and started before processing messages.
  //
  // Also, the order of messages is in general not the same as the POST order
  // from the other client because the POSTs are async and the server may handle
  // some requests faster than others. We need to process offer before
  // candidates so we wait for the offer to arrive first if we're answering.
  // Offers are added to the front of the queue.
  if (!this.pc_ || !this.started_ || !this.hasRemoteSdp_ ||
      this.isDrainingMessages_) {
    return;
  }
  this.isDrainingMessages_ = true;
  var messages = this.messageQueue_;
  this.messageQueue_ = [];
  var sequence = Promise.resolve();
  messages.forEach(function(message) {
    sequence = sequence.then(function() {
      return this.processSignalingMessage_(message);
    }.bind(this));
  }.bind(this));
  sequence.catch(this.onError_.bind(this, 'processSignalingMessage'))
      .then(function() {
        this.isDrainingMessages_ = false;
        if (this.messageQueue_.length > 0) {
          this.drainMessageQueue_();
        }
      }.bind(this));
};

PeerConnectionClient.prototype.onIceCandidate_ = function(event) {
  if (event.candidate) {
    // Eat undesired candidates.
    if (this.filterIceCandidate_(event.candidate)) {
      var message = {
        type: 'candidate',
        label: event.candidate.sdpMLineIndex,
        id: event.candidate.sdpMid,
        candidate: event.candidate.candidate
      };
      if (this.onsignalingmessage) {
        this.onsignalingmessage(message);
      }
      this.recordIceCandidate_('Local', event.candidate);
    }
  } else {
    trace('End of candidates.');
    if (this.onsignalingmessage) {
      this.onsignalingmessage({type: 'end-of-candidates'});
    }
  }
};

PeerConnectionClient.prototype.onSignalingStateChanged_ = function() {
  if (!this.pc_) {
    return;
  }
  trace('Signaling state changed to: ' + this.pc_.signalingState);

  if (this.onsignalingstatechange) {
    this.onsignalingstatechange();
  }
};

PeerConnectionClient.prototype.onIceConnectionStateChanged_ = function() {
  if (!this.pc_) {
    return;
  }
  trace('ICE connection state changed to: ' + this.pc_.iceConnectionState);
  if (this.pc_.iceConnectionState === 'completed') {
    trace('ICE complete time: ' +
        (window.performance.now() - this.startTime_).toFixed(0) + 'ms.');
  }

  if (this.oniceconnectionstatechange) {
    this.oniceconnectionstatechange();
  }
};

// Return false if the candidate should be dropped, true if not.
PeerConnectionClient.prototype.filterIceCandidate_ = function(candidateObj) {
  var candidateStr = candidateObj.candidate;

  // Always eat TCP candidates. Not needed in this context.
  if (candidateStr.indexOf('tcp') !== -1) {
    return false;
  }

  // If we're trying to eat non-relay candidates, do that.
  if (this.params_.peerConnectionConfig.iceTransports === 'relay' &&
      iceCandidateType(candidateStr) !== 'relay') {
    return false;
  }

  return true;
};

PeerConnectionClient.prototype.recordIceCandidate_ =
    function(location, candidateObj) {
      if (this.onnewicecandidate) {
        this.onnewicecandidate(location, candidateObj.candidate);
      }
    };

PeerConnectionClient.prototype.onRemoteStreamAdded_ = function(event) {
  // In SFU mode each forwarded track is surfaced individually (its msid carries the publishing
  // client id), so the UI can group a peer's video and audio into one tile. In P2P mode there is
  // a single remote peer, handled as one stream.
  if (this.sfuMode_) {
    if (this.onremotetrack) {
      this.onremotetrack(event);
    }
    return;
  }
  if (!this.onremotestreamadded) {
    return;
  }
  if (event.streams && event.streams.length > 0) {
    this.onremotestreamadded(event.streams[0]);
    return;
  }
  if (event.track) {
    this.onremotestreamadded(new MediaStream([event.track]));
  }
};

PeerConnectionClient.prototype.onError_ = function(tag, error) {
  if (this.onerror) {
    this.onerror(tag + ': ' + error.toString());
  }
};
