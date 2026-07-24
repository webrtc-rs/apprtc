/*
 *  Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
 *
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the LICENSE file in the root of the source
 *  tree.
 */

/* More information about these options at jshint.com/docs/options */

/* globals trace, InfoBox, setUpFullScreen, onFullScreenChange, isFullScreen,
   RoomSelection, $ */
/* exported AppController, remoteVideo */

'use strict';

// TODO(jiayl): remove |remoteVideo| once the chrome browser tests are updated.
// Do not use in the production code.
var remoteVideo = $('#remote-video');

// Keep this in sync with the HTML element id attributes. Keep it sorted.
var UI_CONSTANTS = {
  confirmJoinButton: '#confirm-join-button',
  confirmJoinDiv: '#confirm-join-div',
  confirmJoinRoomSpan: '#confirm-join-room-span',
  fullscreenSvg: '#fullscreen',
  hangupSvg: '#hangup',
  icons: '#icons',
  infoDiv: '#info-div',
  localVideo: '#local-video',
  miniVideo: '#mini-video',
  muteAudioSvg: '#mute-audio',
  muteVideoSvg: '#mute-video',
  newRoomButton: '#new-room-button',
  newRoomLink: '#new-room-link',
  privacyLinks: '#privacy',
  remoteVideo: '#remote-video',
  rejoinButton: '#rejoin-button',
  rejoinDiv: '#rejoin-div',
  rejoinLink: '#rejoin-link',
  roomLinkHref: '#room-link-href',
  roomSelectionDiv: '#room-selection',
  roomSelectionInput: '#room-id-input',
  roomSelectionInputLabel: '#room-id-input-label',
  roomSelectionJoinButton: '#join-button',
  roomSelectionRandomButton: '#random-button',
  roomSelectionRecentList: '#recent-rooms-list',
  roomSelectionV2Checkbox: '#signaling-v2-checkbox',
  sharingDiv: '#sharing-div',
  sfuGrid: '#sfu-grid',
  statusDiv: '#status-div',
  turnInfoDiv: '#turn-info-div',
  videosDiv: '#videos',
};

// How long the SFU grid may be held after the direct P2P transport connects, waiting for
// the direct remote video to become playable, before the P2P layout takes over anyway.
var P2P_LAYOUT_MEDIA_TIMEOUT_MS = 3000;

// The controller that connects the Call with the UI.
var AppController = function(loadingParams) {
  trace('Initializing; server= ' + loadingParams.roomServer + '.');
  trace('Initializing; room=' + loadingParams.roomId + '.');

  this.hangupSvg_ = $(UI_CONSTANTS.hangupSvg);
  this.icons_ = $(UI_CONSTANTS.icons);
  this.localVideo_ = $(UI_CONSTANTS.localVideo);
  this.miniVideo_ = $(UI_CONSTANTS.miniVideo);
  this.sharingDiv_ = $(UI_CONSTANTS.sharingDiv);
  this.sfuGrid_ = $(UI_CONSTANTS.sfuGrid);
  this.sfuTiles_ = {};
  this.statusDiv_ = $(UI_CONSTANTS.statusDiv);
  this.turnInfoDiv_ = $(UI_CONSTANTS.turnInfoDiv);
  this.remoteVideo_ = $(UI_CONSTANTS.remoteVideo);
  this.videosDiv_ = $(UI_CONSTANTS.videosDiv);
  this.roomLinkHref_ = $(UI_CONSTANTS.roomLinkHref);
  this.rejoinDiv_ = $(UI_CONSTANTS.rejoinDiv);
  this.rejoinLink_ = $(UI_CONSTANTS.rejoinLink);
  this.newRoomLink_ = $(UI_CONSTANTS.newRoomLink);
  this.rejoinButton_ = $(UI_CONSTANTS.rejoinButton);
  this.newRoomButton_ = $(UI_CONSTANTS.newRoomButton);

  this.muteAudioIconSet_ =
      new AppController.IconSet_(UI_CONSTANTS.muteAudioSvg);
  this.muteVideoIconSet_ =
      new AppController.IconSet_(UI_CONSTANTS.muteVideoSvg);
  this.fullscreenIconSet_ =
      new AppController.IconSet_(UI_CONSTANTS.fullscreenSvg);

  this.loadingParams_ = loadingParams;
  this.loadUrlParams_();

  var paramsPromise = Promise.resolve({});

  Promise.resolve(paramsPromise).then(function(newParams) {
    // Merge newly retrieved params with loadingParams.
    if (newParams) {
      Object.keys(newParams).forEach(function(key) {
        this.loadingParams_[key] = newParams[key];
      }.bind(this));
    }

    this.newRoomButton_.addEventListener('click',
        this.onNewRoomClick_.bind(this), false);
    this.rejoinButton_.addEventListener('click',
        this.onRejoinClick_.bind(this), false);

    this.roomLink_ = '';
    this.roomSelection_ = null;
    this.localStream_ = null;
    this.remoteVideoResetTimer_ = null;
    this.p2pLayoutTimer_ = null;
    this.p2pLayoutCanPlay_ = null;
    this.p2pLayoutPending_ = false;

    // If the params has a roomId specified, we should connect to that room
    // immediately. If not, show the room selection UI.
    if (this.loadingParams_.roomId) {
      this.createCall_();

      // Ask the user to confirm.
      if (!RoomSelection.matchRandomRoomPattern(this.loadingParams_.roomId)) {
        // Show the room name only if it does not match the random room pattern.
        $(UI_CONSTANTS.confirmJoinRoomSpan).textContent = ' "' +
            this.loadingParams_.roomId + '"';
      }
      var confirmJoinDiv = $(UI_CONSTANTS.confirmJoinDiv);
      this.show_(confirmJoinDiv);

      $(UI_CONSTANTS.confirmJoinButton).onclick = function() {
        this.hide_(confirmJoinDiv);

        // Record this room in the recently used list.
        var recentlyUsedList = new RoomSelection.RecentlyUsedList();
        recentlyUsedList.pushRecentRoom(this.loadingParams_.roomId);
        this.finishCallSetup_(this.loadingParams_.roomId);
      }.bind(this);

      if (this.loadingParams_.bypassJoinConfirmation) {
        $(UI_CONSTANTS.confirmJoinButton).onclick();
      }
    } else {
      // Display the room selection UI.
      this.showRoomSelection_();
    }
  }.bind(this)).catch(function(error) {
    trace('Error initializing: ' + error.message);
  }.bind(this));
};

AppController.prototype.createCall_ = function() {
  var privacyLinks = $(UI_CONSTANTS.privacyLinks);
  this.hide_(privacyLinks);
  this.call_ = new Call(this.loadingParams_);
  this.infoBox_ = new InfoBox($(UI_CONSTANTS.infoDiv), this.call_,
      this.loadingParams_.versionInfo);

  var roomErrors = this.loadingParams_.errorMessages;
  var roomWarnings = this.loadingParams_.warningMessages;
  if (roomErrors && roomErrors.length > 0) {
    for (var i = 0; i < roomErrors.length; ++i) {
      this.infoBox_.pushErrorMessage(roomErrors[i]);
    }
    return;
  } else if (roomWarnings && roomWarnings.length > 0) {
    for (var j = 0; j < roomWarnings.length; ++j) {
      this.infoBox_.pushWarningMessage(roomWarnings[j]);
    }
  }

  // TODO(jiayl): replace callbacks with events.
  this.call_.onremotehangup = this.onRemoteHangup_.bind(this);
  this.call_.onremotesdpset = this.onRemoteSdpSet_.bind(this);
  this.call_.onremotestreamadded = this.onRemoteStreamAdded_.bind(this);
  this.call_.onremotetrack = this.onSfuTrackAdded_.bind(this);
  this.call_.onsfunegotiated = this.pruneStaleSfuTiles_.bind(this);
  this.call_.onlocalstreamadded = this.onLocalStreamAdded_.bind(this);
  this.call_.onmodechange = this.onModeChange_.bind(this);

  this.call_.onsignalingstatechange =
      this.infoBox_.updateInfoDiv.bind(this.infoBox_);
  this.call_.oniceconnectionstatechange =
      this.infoBox_.updateInfoDiv.bind(this.infoBox_);
  this.call_.onnewicecandidate =
      this.infoBox_.recordIceCandidateTypes.bind(this.infoBox_);

  this.call_.onerror = this.displayError_.bind(this);
  this.call_.onturnstatusmessage = this.displayTurnStatus_.bind(this);
  this.call_.oncallerstarted = this.displaySharingInfo_.bind(this);
};

AppController.prototype.showRoomSelection_ = function() {
  var roomSelectionDiv = $(UI_CONSTANTS.roomSelectionDiv);
  this.roomSelection_ = new RoomSelection(roomSelectionDiv, UI_CONSTANTS);

  this.show_(roomSelectionDiv);
  this.roomSelection_.onRoomSelected = function(roomName, signalingVersion) {
    this.hide_(roomSelectionDiv);
    this.loadingParams_.signalingVersion = signalingVersion;
    this.createCall_();
    this.finishCallSetup_(roomName);

    this.roomSelection_.removeEventListeners();
    this.roomSelection_ = null;
    if (this.localStream_) {
      this.attachLocalStream_();
    }
  }.bind(this);
};

AppController.prototype.setupUi_ = function() {
  this.iconEventSetup_();
  document.onkeypress = this.onKeyPress_.bind(this);
  window.onmousemove = this.showIcons_.bind(this);

  $(UI_CONSTANTS.muteAudioSvg).onclick = this.toggleAudioMute_.bind(this);
  $(UI_CONSTANTS.muteVideoSvg).onclick = this.toggleVideoMute_.bind(this);
  $(UI_CONSTANTS.fullscreenSvg).onclick = this.toggleFullScreen_.bind(this);
  $(UI_CONSTANTS.hangupSvg).onclick = this.hangup_.bind(this);

  setUpFullScreen();
  onFullScreenChange(this.onFullScreenChange_.bind(this));
  // Adopt whatever state the browser is already in (for example an F11 window).
  this.onFullScreenChange_();
};

AppController.prototype.finishCallSetup_ = function(roomId) {
  this.call_.start(roomId);
  this.setupUi_();

  // Call hangup with async = false. Required to complete multiple
  // clean up steps before page is closed.
  window.onbeforeunload = function() {
    this.call_.hangup(false);
  }.bind(this);

  window.onpopstate = function(event) {
    if (!event.state) {
      // TODO (chuckhays) : Resetting back to room selection page not
      // yet supported, reload the initial page instead.
      trace('Reloading main page.');
      location.href = location.origin;
    } else {
      // This could be a forward request to open a room again.
      if (event.state.roomLink) {
        location.href = event.state.roomLink;
      }
    }
  };
};

AppController.prototype.hangup_ = function() {
  trace('Hanging up.');
  this.hide_(this.icons_);
  this.displayStatus_('Hanging up');
  this.transitionToDone_();

  // Call hangup with async = true.
  this.call_.hangup(true);
  // Reset key and mouse event handlers.
  document.onkeypress = null;
  window.onmousemove = null;
};

AppController.prototype.onRemoteHangup_ = function() {
  // The peer left while a downgrade was still waiting for its media: its video is never
  // going to arrive, so retire the grid now rather than letting the fallback timer fire
  // into the waiting layout.
  if (this.p2pLayoutPending_) {
    this.completeP2pLayout_();
  }
  this.displayStatus_('The remote side hung up.');
  this.transitionToWaiting_();

  this.call_.onRemoteHangup();
};

AppController.prototype.onRemoteSdpSet_ = function(hasRemoteVideo) {
  var mode = this.call_.getMode();
  // While a transition owns the screen, the stage must not activate under the grid;
  // onModeChange_ finishes the transition once the new transport's media is ready.
  // |p2pLayoutPending_| covers the tail of a downgrade: Call has already committed mode
  // p2p, but the grid is still up and #local-video is still empty, so letting
  // transitionToActive_ run here would blank the self-view (it copies #local-video's
  // stream into #mini-video).
  if (mode === 'upgrading' || mode === 'sfu' || mode === 'downgrading' ||
      this.p2pLayoutPending_) {
    return;
  }
  if (hasRemoteVideo) {
    trace('Waiting for remote video.');
    this.waitForRemoteVideo_();
  } else {
    trace('No remote video stream; not waiting for media to arrive.');
    // TODO(juberti): Make this wait for ICE connection before transitioning.
    this.transitionToActive_();
  }
};

AppController.prototype.waitForRemoteVideo_ = function() {
  // Wait for the actual video to start arriving before moving to the active
  // call state.
  if (this.remoteVideo_.readyState >= 2) { // i.e. can play
    trace('Remote video started; currentTime: ' +
          this.remoteVideo_.currentTime);
    this.transitionToActive_();
  } else {
    this.remoteVideo_.oncanplay = this.waitForRemoteVideo_.bind(this);
  }
};

AppController.prototype.onRemoteStreamAdded_ = function(stream) {
  // P2P only: the SFU path is per-track (see onSfuTrackAdded_).
  this.deactivate_(this.sharingDiv_);
  this.displayTurnStatus_('');
  trace('Remote stream added.');
  this.remoteVideo_.srcObject = stream;
  this.infoBox_.getRemoteTrackIds(stream);

  if (this.remoteVideoResetTimer_) {
    clearTimeout(this.remoteVideoResetTimer_);
    this.remoteVideoResetTimer_ = null;
  }
};

// Recover the publishing client id from a forwarded track. The SFU stamps each forwarded track's
// msid with `peer-<clientId>`, so it surfaces here as the received stream id and/or the track id;
// try both (a peer's audio and video may arrive on different stream ids).
AppController.prototype.sfuPublisherId_ = function(event) {
  var ids = [];
  if (event.streams && event.streams[0]) {
    ids.push(event.streams[0].id);
  }
  if (event.track) {
    ids.push(event.track.id);
  }
  for (var i = 0; i < ids.length; ++i) {
    var match = /peer-(\d+)/.exec(ids[i] || '');
    if (match) {
      return match[1];
    }
  }
  return null;
};

// Find (or lazily create) the grid tile that groups one peer's video and audio, keyed by the
// publishing client id and captioned with it.
AppController.prototype.getOrCreateSfuTile_ = function(key, publisher) {
  var tile = this.sfuTiles_[key];
  if (tile) {
    return tile;
  }
  tile = document.createElement('div');
  tile.className = 'sfu-tile';
  tile.dataset.participantId = key;
  var caption = document.createElement('span');
  caption.className = 'sfu-caption';
  caption.textContent = publisher ? ('Peer ' + publisher) : 'Peer';
  tile.appendChild(caption);
  this.sfuGrid_.appendChild(tile);
  this.sfuTiles_[key] = tile;
  return tile;
};

// SFU mode: place each forwarded track into its publisher's tile — video fills the tile, audio is
// a hidden element that only plays. Mirrors the SFU chat sample's per-track grid.
AppController.prototype.onSfuTrackAdded_ = function(event) {
  var track = event.track;
  if (!track) {
    return;
  }
  this.deactivate_(this.sharingDiv_);
  this.displayTurnStatus_('');

  var domId = 'sfu-media-' + track.id;
  if (document.getElementById(domId)) {
    return;
  }
  var publisher = this.sfuPublisherId_(event);
  var key = publisher ||
      (event.streams && event.streams[0] && event.streams[0].id) || track.id;
  var tile = this.getOrCreateSfuTile_(key, publisher);
  var kind = track.kind === 'audio' ? 'audio' : 'video';

  // Replace any existing element of the same kind in this tile (e.g. a re-forwarded track).
  var previous = tile.querySelector(kind);
  if (previous) {
    previous.remove();
  }
  var el = document.createElement(kind);
  el.id = domId;
  el.autoplay = true;
  el.playsInline = true;
  if (kind === 'audio') {
    el.style.display = 'none';
  }
  var media = new MediaStream();
  media.addTrack(track);
  el.srcObject = media;
  tile.appendChild(el);

  var play = function() {
    el.play().catch(function(error) {
      trace('SFU media play failed: ' + error);
    });
  };
  el.addEventListener('loadedmetadata', play);
  play();

  // Drop the tile if the track ends (peer left / stopped publishing).
  track.addEventListener('ended', function() {
    el.remove();
    if (!tile.querySelector('video') && !tile.querySelector('audio')) {
      tile.remove();
      delete this.sfuTiles_[key];
    }
  }.bind(this));
};

// After the SFU re-offer is answered, |liveTrackIds| is the set (id -> true) of forwarded tracks
// the SFU is still sending us. Remove any media element whose track is gone, then drop any tile
// left with no media — this is what makes a departed peer's grid tile disappear. Remote-track
// 'ended' is unreliable here (SFU renegotiation flips the transceiver to inactive rather than
// stopping the track), so we reconcile against the negotiated transceivers instead.
AppController.prototype.pruneStaleSfuTiles_ = function(liveTrackIds) {
  var mediaElements = this.sfuGrid_.querySelectorAll('video, audio');
  for (var i = 0; i < mediaElements.length; ++i) {
    var el = mediaElements[i];
    var trackId = el.id.replace(/^sfu-media-/, '');
    if (!liveTrackIds[trackId]) {
      el.remove();
    }
  }
  for (var key in this.sfuTiles_) {
    if (!this.sfuTiles_.hasOwnProperty(key)) {
      continue;
    }
    var tile = this.sfuTiles_[key];
    if (!tile.querySelector('video') && !tile.querySelector('audio')) {
      tile.remove();
      delete this.sfuTiles_[key];
    }
  }
};

AppController.prototype.onModeChange_ = function(mode) {
  if (mode === 'upgrading') {
    this.displayStatus_('Switching to group call…');
    return;
  }
  if (mode === 'downgrading') {
    // SFU -> P2P downgrade in progress. Keep the grid exactly as it is: the retained SFU
    // peer connection holds each tile's last frames while the direct P2P connection
    // negotiates, so the participant never blinks out of existence mid-transition.
    this.displayStatus_('Switching to direct call…');
    return;
  }
  if (mode === 'p2p') {
    // The direct transport is up. Hold the grid — still showing the SFU's last frames —
    // for the final stretch until the direct remote video can actually play, so the stage
    // never appears blank. Bounded, because an audio-only or stalled peer must not leave a
    // retired grid on screen forever.
    if (this.sfuGrid_ && !this.sfuGrid_.classList.contains('hidden') &&
        this.remoteVideo_.srcObject && this.remoteVideo_.readyState < 2) {
      this.p2pLayoutPending_ = true;
      // Use a listener rather than the .oncanplay property: transitionToActive_,
      // transitionToWaiting_ and waitForRemoteVideo_ all overwrite that one property,
      // and any of them would silently cancel this completion.
      this.p2pLayoutCanPlay_ = this.completeP2pLayout_.bind(this);
      this.remoteVideo_.addEventListener('canplay', this.p2pLayoutCanPlay_);
      this.p2pLayoutTimer_ = setTimeout(
          this.completeP2pLayout_.bind(this), P2P_LAYOUT_MEDIA_TIMEOUT_MS);
      return;
    }
    this.completeP2pLayout_();
    return;
  }
  if (mode === 'sfu') {
    this.videosDiv_.classList.remove('active');
    this.videosDiv_.classList.add('sfu-mode');
    this.sfuGrid_.classList.remove('hidden');
    this.deactivate_(this.remoteVideo_);
    // Show the local stream in the self-view. A client that joined directly in SFU mode never
    // ran transitionToActive_ (which normally moves the local stream to the mini video), so set
    // it here too — otherwise its self-view is an empty white box.
    if (this.localStream_) {
      this.miniVideo_.srcObject = this.localStream_;
    }
    this.activate_(this.miniVideo_);
    this.show_(this.icons_);
    this.show_(this.hangupSvg_);
    this.displayStatus_('');
  }
};

// Drop whatever is keeping the grid on screen for the tail of a downgrade.
AppController.prototype.clearP2pLayoutHold_ = function() {
  this.p2pLayoutPending_ = false;
  if (this.p2pLayoutTimer_) {
    clearTimeout(this.p2pLayoutTimer_);
    this.p2pLayoutTimer_ = null;
  }
  if (this.p2pLayoutCanPlay_) {
    this.remoteVideo_.removeEventListener('canplay', this.p2pLayoutCanPlay_);
    this.p2pLayoutCanPlay_ = null;
  }
};

// Retire the per-publisher grid and return to the full-screen P2P layout. Only the remote
// view changes; the self-view and controls keep their P2P position. Idempotent, because it
// is reached from whichever of the canplay callback and the fallback timer fires first.
AppController.prototype.completeP2pLayout_ = function() {
  this.clearP2pLayoutHold_();
  for (var tileKey in this.sfuTiles_) {
    if (this.sfuTiles_.hasOwnProperty(tileKey)) {
      this.sfuTiles_[tileKey].remove();
    }
  }
  this.sfuTiles_ = {};
  this.sfuGrid_.classList.add('hidden');
  this.videosDiv_.classList.remove('sfu-mode');
  // Restore the P2P container, which mirrors the self-view back to the right.
  this.activate_(this.videosDiv_);
  if (this.localStream_) {
    this.localVideo_.srcObject = this.localStream_;
    this.miniVideo_.srcObject = this.localStream_;
  }
  this.activate_(this.miniVideo_);
  this.show_(this.icons_);
  this.show_(this.hangupSvg_);
  this.displayStatus_('');
  // onRemoteSdpSet_ suppresses the stage transition while the grid owns the screen, so
  // finish it here: the direct remote stream is normally already attached by now.
  if (this.remoteVideo_.srcObject) {
    this.transitionToActive_();
  } else {
    this.deactivate_(this.remoteVideo_);
  }
  // The retired SFU connection's frames are off screen now, so its peer connection can go.
  // Closing it ends its remote tracks, which is what the grid tiles were holding.
  this.call_.releaseRetiredSfuTransport();
};

AppController.prototype.onLocalStreamAdded_ = function(stream) {
  trace('User has granted access to local media.');
  this.localStream_ = stream;
  this.infoBox_.getLocalTrackIds(this.localStream_);

  if (!this.roomSelection_) {
    this.attachLocalStream_();
  }
};

AppController.prototype.attachLocalStream_ = function() {
  trace('Attaching local stream.');
  this.localVideo_.srcObject = this.localStream_;

  this.displayStatus_('');
  this.activate_(this.localVideo_);
  this.show_(this.icons_);
  if (this.localStream_.getVideoTracks().length === 0) {
    this.hide_($(UI_CONSTANTS.muteVideoSvg));
  }
  if (this.localStream_.getAudioTracks().length === 0) {
    this.hide_($(UI_CONSTANTS.muteAudioSvg));
  }
};

AppController.prototype.transitionToActive_ = function() {
  // Stop waiting for remote video.
  this.remoteVideo_.oncanplay = undefined;
  var connectTime = window.performance.now();
  this.infoBox_.setSetupTimes(this.call_.startTime, connectTime);
  this.infoBox_.updateInfoDiv();
  trace('Call setup time: ' + (connectTime - this.call_.startTime).toFixed(0) +
      'ms.');

  // Prepare the remote video and PIP elements. Prefer the captured stream over
  // #local-video's: after any mode transition the full-screen local video has already been
  // emptied, and copying its null srcObject here is what leaves the self-view black.
  trace('reattachMediaStream: ' + this.localVideo_.srcObject);
  this.miniVideo_.srcObject = this.localStream_ || this.localVideo_.srcObject;

  // Transition opacity from 0 to 1 for the remote and mini videos.
  this.activate_(this.remoteVideo_);
  this.activate_(this.miniVideo_);
  // Transition opacity from 1 to 0 for the local video.
  this.deactivate_(this.localVideo_);
  this.localVideo_.srcObject = null;
  // Rotate the div containing the videos 180 deg with a CSS transform.
  this.activate_(this.videosDiv_);
  this.show_(this.hangupSvg_);
  this.displayStatus_('');
};

AppController.prototype.transitionToWaiting_ = function() {
  // Stop waiting for remote video.
  this.remoteVideo_.oncanplay = undefined;

  this.hide_(this.hangupSvg_);
  // Rotate the div containing the videos -180 deg with a CSS transform.
  this.deactivate_(this.videosDiv_);

  if (!this.remoteVideoResetTimer_) {
    this.remoteVideoResetTimer_ = setTimeout(function() {
      this.remoteVideoResetTimer_ = null;
      trace('Resetting remoteVideo src after transitioning to waiting.');
      this.remoteVideo_.srcObject = null;
    }.bind(this), 800);
  }

  // Set localVideo.srcObject now so that the local stream won't be lost if the
  // call is restarted before the timeout. Prefer the captured stream: #mini-video may be
  // empty if a mode transition was still in flight when the peer left.
  this.localVideo_.srcObject = this.localStream_ || this.miniVideo_.srcObject;

  // Transition opacity from 0 to 1 for the local video.
  this.activate_(this.localVideo_);
  // Transition opacity from 1 to 0 for the remote and mini videos.
  this.deactivate_(this.remoteVideo_);
  this.deactivate_(this.miniVideo_);
};

AppController.prototype.transitionToDone_ = function() {
  // Stop waiting for remote video, including a downgrade still holding the grid.
  this.remoteVideo_.oncanplay = undefined;
  this.clearP2pLayoutHold_();
  this.deactivate_(this.localVideo_);
  this.deactivate_(this.remoteVideo_);
  this.deactivate_(this.miniVideo_);
  this.hide_(this.hangupSvg_);
  this.activate_(this.rejoinDiv_);
  this.show_(this.rejoinDiv_);
  this.displayStatus_('');
  this.displayTurnStatus_('');
};

AppController.prototype.onRejoinClick_ = function() {
  this.deactivate_(this.rejoinDiv_);
  this.hide_(this.rejoinDiv_);
  this.call_.restart();
  this.setupUi_();
};

AppController.prototype.onNewRoomClick_ = function() {
  this.deactivate_(this.rejoinDiv_);
  this.hide_(this.rejoinDiv_);
  this.showRoomSelection_();
};

// Spacebar, or m: toggle audio mute.
// c: toggle camera(video) mute.
// f: toggle fullscreen.
// i: toggle info panel.
// q: quit (hangup)
// Return false to screen out original Chrome shortcuts.
AppController.prototype.onKeyPress_ = function(event) {
  switch (String.fromCharCode(event.charCode)) {
    case ' ':
    case 'm':
      if (this.call_) {
        this.call_.toggleAudioMute();
        this.muteAudioIconSet_.toggle();
      }
      return false;
    case 'c':
      if (this.call_) {
        this.call_.toggleVideoMute();
        this.muteVideoIconSet_.toggle();
      }
      return false;
    case 'f':
      this.toggleFullScreen_();
      return false;
    case 'i':
      this.infoBox_.toggleInfoDiv();
      return false;
    case 'q':
      this.hangup_();
      return false;
    case 'l':
      this.toggleMiniVideo_();
      return false;
    default:
      return;
  }
};

AppController.prototype.pushCallNavigation_ = function(roomId, roomLink) {
  window.history.pushState({'roomId': roomId, 'roomLink': roomLink}, roomId,
      roomLink);
};

AppController.prototype.displaySharingInfo_ = function(roomId, roomLink) {
  this.roomLinkHref_.href = roomLink;
  this.roomLinkHref_.text = roomLink;
  this.roomLink_ = roomLink;
  this.pushCallNavigation_(roomId, roomLink);
  this.activate_(this.sharingDiv_);
};

AppController.prototype.displayStatus_ = function(status) {
  if (status === '') {
    this.deactivate_(this.statusDiv_);
  } else {
    this.activate_(this.statusDiv_);
  }
  this.statusDiv_.innerHTML = status;
};

AppController.prototype.displayTurnStatus_ = function(status) {
  if (status === '') {
    this.deactivate_(this.turnInfoDiv_);
  } else {
    this.activate_(this.turnInfoDiv_);
  }
  this.turnInfoDiv_.innerHTML = status;
};

AppController.prototype.displayError_ = function(error) {
  trace(error);
  this.infoBox_.pushErrorMessage(error);
};

AppController.prototype.toggleAudioMute_ = function() {
  this.call_.toggleAudioMute();
  this.muteAudioIconSet_.toggle();
};

AppController.prototype.toggleVideoMute_ = function() {
  this.call_.toggleVideoMute();
  this.muteVideoIconSet_.toggle();
};

AppController.prototype.toggleFullScreen_ = function() {
  if (isFullScreen()) {
    trace('Exiting fullscreen.');
    document.cancelFullScreen();
  } else {
    trace('Entering fullscreen.');
    document.body.requestFullScreen();
  }
  // The button is deliberately not updated here. Entering fullscreen is asynchronous and
  // can be refused, and the user can leave it with Esc, F11 or the browser's own controls
  // without this handler running at all. onFullScreenChange_ is the single place that
  // syncs the button, so it can never disagree with the actual state.
};

// Sync the fullscreen button to the browser's real fullscreen state. `on` is what paints
// it blue (`svg.on circle` goes transparent, revealing `#fullscreen.on`'s background) and
// swaps the enter/exit glyph.
AppController.prototype.onFullScreenChange_ = function() {
  var on = isFullScreen();
  this.fullscreenIconSet_.set(on);
  document.querySelector('svg#fullscreen title').textContent =
      on ? 'Exit fullscreen' : 'Enter fullscreen';
};

AppController.prototype.toggleMiniVideo_ = function() {
  if (this.miniVideo_.classList.contains('active')) {
    this.deactivate_(this.miniVideo_);
  } else {
    this.activate_(this.miniVideo_);
  }
};

AppController.prototype.hide_ = function(element) {
  element.classList.add('hidden');
};

AppController.prototype.show_ = function(element) {
  element.classList.remove('hidden');
};

AppController.prototype.activate_ = function(element) {
  element.classList.add('active');
};

AppController.prototype.deactivate_ = function(element) {
  element.classList.remove('active');
};

AppController.prototype.showIcons_ = function() {
  if (!this.icons_.classList.contains('active')) {
    this.activate_(this.icons_);
    this.setIconTimeout_();
  }
};

AppController.prototype.hideIcons_ = function() {
  if (this.icons_.classList.contains('active')) {
    this.deactivate_(this.icons_);
  }
};

AppController.prototype.setIconTimeout_ = function() {
  if (this.hideIconsAfterTimeout) {
    window.clearTimeout.bind(this, this.hideIconsAfterTimeout);
  }
  this.hideIconsAfterTimeout = window.setTimeout(function() {
    this.hideIcons_();
  }.bind(this), 5000);
};

AppController.prototype.iconEventSetup_ = function() {
  this.icons_.onmouseenter = function() {
    window.clearTimeout(this.hideIconsAfterTimeout);
  }.bind(this);

  this.icons_.onmouseleave = function() {
    this.setIconTimeout_();
  }.bind(this);
};

AppController.prototype.loadUrlParams_ = function() {
  /* eslint-disable dot-notation */
  // Suppressing eslint warns about using urlParams['KEY'] instead of
  // urlParams.KEY, since we'd like to use string literals to avoid the Closure
  // compiler renaming the properties.
  var DEFAULT_VIDEO_CODEC = 'VP9';
  var urlParams = queryStringToDictionary(window.location.search);
  this.loadingParams_.audioSendBitrate = urlParams['asbr'];
  this.loadingParams_.audioSendCodec = urlParams['asc'];
  this.loadingParams_.audioRecvBitrate = urlParams['arbr'];
  this.loadingParams_.audioRecvCodec = urlParams['arc'];
  this.loadingParams_.opusMaxPbr = urlParams['opusmaxpbr'];
  this.loadingParams_.opusFec = urlParams['opusfec'];
  this.loadingParams_.opusDtx = urlParams['opusdtx'];
  this.loadingParams_.opusStereo = urlParams['stereo'];
  this.loadingParams_.videoSendBitrate = urlParams['vsbr'];
  this.loadingParams_.videoSendInitialBitrate = urlParams['vsibr'];
  this.loadingParams_.videoSendCodec = urlParams['vsc'];
  this.loadingParams_.videoRecvBitrate = urlParams['vrbr'];
  this.loadingParams_.videoRecvCodec = urlParams['vrc'] || DEFAULT_VIDEO_CODEC;
  this.loadingParams_.videoFec = urlParams['videofec'];
  /* eslint-enable dot-notation */
};

AppController.IconSet_ = function(iconSelector) {
  this.iconElement = document.querySelector(iconSelector);
};

AppController.IconSet_.prototype.toggle = function() {
  this.set(!this.iconElement.classList.contains('on'));
};

// `on` is the whole visual state of an icon button, not just its colour: CSS uses it to
// make `svg.on circle` transparent (revealing the button's blue background), to display
// `svg.on path.on`, and to hide `svg.on path.off` — so the fill, the button style and the
// glyph all follow from this one class.
AppController.IconSet_.prototype.set = function(on) {
  this.iconElement.classList.toggle('on', on);
};
