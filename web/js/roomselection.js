/*
 *  Copyright (c) 2015 The WebRTC project authors. All Rights Reserved.
 *
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the LICENSE file in the root of the source
 *  tree.
 */

/* More information about these options at jshint.com/docs/options */

/* globals randomString, Storage, parseJSON */
/* exported RoomSelection */

'use strict';


// ── V2 room tokens ───────────────────────────────────────────────────────────────────
//
// A V2 room id is a UUIDv8 (RFC 9562 §5.8) carried in links as 22 base64url characters.
// This is the browser half of the codec in `signaling/src/v2.rs`; the two must agree, so
// the rules are the same: exactly 22 characters, canonical trailing bits, version 8 and
// the RFC 9562 variant.

var ROOM_TOKEN_LENGTH = 22;

function base64UrlEncode(bytes) {
  var binary = '';
  for (var i = 0; i < bytes.length; ++i) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function base64UrlDecode(token) {
  var padded = token.replace(/-/g, '+').replace(/_/g, '/');
  while (padded.length % 4 !== 0) {
    padded += '=';
  }
  var binary = atob(padded);
  var bytes = new Uint8Array(binary.length);
  for (var i = 0; i < binary.length; ++i) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

var RoomSelection = function(roomSelectionDiv,
  uiConstants, recentRoomsKey, setupCompletedCallback) {
  this.roomSelectionDiv_ = roomSelectionDiv;

  this.setupCompletedCallback_ = setupCompletedCallback;

  this.roomIdInput_ = this.roomSelectionDiv_.querySelector(
      uiConstants.roomSelectionInput);
  this.roomIdInputLabel_ = this.roomSelectionDiv_.querySelector(
      uiConstants.roomSelectionInputLabel);
  this.roomSelectionLink_ = this.roomSelectionDiv_.querySelector(
      uiConstants.roomSelectionLink);
  this.roomJoinButton_ = this.roomSelectionDiv_.querySelector(
      uiConstants.roomSelectionJoinButton);
  this.roomRandomButton_ = this.roomSelectionDiv_.querySelector(
      uiConstants.roomSelectionRandomButton);
  this.roomRecentList_ = this.roomSelectionDiv_.querySelector(
      uiConstants.roomSelectionRecentList);
  this.signalingV2Checkbox_ = this.roomSelectionDiv_.querySelector(
      uiConstants.roomSelectionV2Checkbox);

  this.instructions_ = document.querySelector('#instructions');
  // Seed the input for whichever protocol the checkbox starts on, then validate it.
  this.applySignalingVersionUi_();

  this.roomIdInputListener_ = this.onRoomIdInput_.bind(this);
  this.roomIdInput_.addEventListener('input', this.roomIdInputListener_, false);

  this.signalingVersionListener_ = this.onSignalingVersionChange_.bind(this);
  this.signalingV2Checkbox_.addEventListener(
      'change', this.signalingVersionListener_, false);

  this.roomIdKeyupListener_ = this.onRoomIdKeyPress_.bind(this);
  this.roomIdInput_.addEventListener('keyup', this.roomIdKeyupListener_, false);

  this.roomRandomButtonListener_ = this.onRandomButton_.bind(this);
  this.roomRandomButton_.addEventListener(
      'click', this.roomRandomButtonListener_, false);

  this.roomJoinButtonListener_ = this.onJoinButton_.bind(this);
  this.roomJoinButton_.addEventListener(
      'click', this.roomJoinButtonListener_, false);

  this.roomLinkClickListener_ = this.onRoomLinkClick_.bind(this);
  if (this.roomSelectionLink_) {
    this.roomSelectionLink_.addEventListener(
        'click', this.roomLinkClickListener_, false);
  }

  // Public callbacks. Keep it sorted.
  this.onRoomSelected = null;

  this.recentlyUsedList_ = new RoomSelection.RecentlyUsedList(recentRoomsKey);
  this.startBuildingRecentRoomList_();
};

// Mint a room id: 122 random bits with the version and variant nibbles stamped in.
RoomSelection.generateRoomToken = function() {
  var bytes = new Uint8Array(16);
  window.crypto.getRandomValues(bytes);
  bytes[6] = (bytes[6] & 0x0f) | 0x80;   // version 8
  bytes[8] = (bytes[8] & 0x3f) | 0x80;   // RFC 9562 variant
  return base64UrlEncode(bytes);
};

// True only for a canonical encoding of a UUIDv8. 128 bits is not a multiple of 6, so the
// final character carries 4 must-be-zero bits — without that check one room would answer
// to sixteen different links.
RoomSelection.isRoomToken = function(value) {
  if (typeof value !== 'string' || value.length !== ROOM_TOKEN_LENGTH) {
    return false;
  }
  if (!/^[A-Za-z0-9_-]{21}[AQgw]$/.test(value)) {
    return false;
  }
  try {
    var bytes = base64UrlDecode(value);
    return bytes.length === 16 &&
        (bytes[6] & 0xf0) === 0x80 &&
        (bytes[8] & 0xc0) === 0x80;
  } catch (e) {
    return false;
  }
};

// Accept either a bare token or a full room link, so a pasted link works.
RoomSelection.roomTokenFrom = function(value) {
  var text = (value || '').trim();
  var match = text.match(/\/v2\/r\/([A-Za-z0-9_-]+)/);
  if (match) {
    text = match[1];
  }
  return RoomSelection.isRoomToken(text) ? text : null;
};

// A machine-generated room name, which the confirm-join prompt hides rather than reading
// back at the user: a V1 nine-digit random room, or any V2 minted token.
RoomSelection.matchRandomRoomPattern = function(input) {
  return input.match(/^\d{9}$/) !== null || RoomSelection.isRoomToken(input);
};

RoomSelection.prototype.removeEventListeners = function() {
  this.roomIdInput_.removeEventListener('input', this.roomIdInputListener_);
  this.roomIdInput_.removeEventListener('keyup', this.roomIdKeyupListener_);
  this.signalingV2Checkbox_.removeEventListener(
      'change', this.signalingVersionListener_);
  this.roomRandomButton_.removeEventListener(
      'click', this.roomRandomButtonListener_);
  if (this.roomSelectionLink_) {
    this.roomSelectionLink_.removeEventListener(
        'click', this.roomLinkClickListener_);
  }
  this.roomJoinButton_.removeEventListener(
      'click', this.roomJoinButtonListener_);
};

RoomSelection.prototype.startBuildingRecentRoomList_ = function() {
  this.recentlyUsedList_.getRecentRooms().then(function(recentRooms) {
    this.buildRecentRoomList_(recentRooms);
    if (this.setupCompletedCallback_) {
      this.setupCompletedCallback_();
    }
  }.bind(this)).catch(function(error) {
    trace('Error building recent rooms list: ' + error.message);
  }.bind(this));
};

RoomSelection.prototype.buildRecentRoomList_ = function(recentRooms) {
  var lastChild = this.roomRecentList_.lastChild;
  while (lastChild) {
    this.roomRecentList_.removeChild(lastChild);
    lastChild = this.roomRecentList_.lastChild;
  }

  for (var i = 0; i < recentRooms.length; ++i) {
    // Create link in recent list
    var li = document.createElement('li');
    var href = document.createElement('a');
    var linkText = document.createTextNode(recentRooms[i]);
    href.appendChild(linkText);
    href.href = location.origin + this.roomPath_(recentRooms[i]);
    li.appendChild(href);
    this.roomRecentList_.appendChild(li);

    // Set up click handler to avoid browser navigation.
    href.addEventListener('click',
        this.makeRecentlyUsedClickHandler_(recentRooms[i]).bind(this), false);
  }
};

RoomSelection.prototype.onRoomIdInput_ = function() {
  var room = this.roomIdInput_.value;
  var valid;
  if (this.signalingV2Checkbox_.checked) {
    // V2 rooms are minted, not typed: the field holds a room link (or a bare token
    // someone pasted), and anything else is rejected rather than turned into a room.
    valid = RoomSelection.roomTokenFrom(room) !== null;
  } else {
    // V1 is unchanged: any uint64 room id, typed freely.
    valid = /^\d+$/.test(room);
    if (valid) {
      try {
        var val = BigInt(room);
        valid = val >= 0n && val <= 18446744073709551615n;
      } catch (e) {
        valid = false;
      }
    }
  }

  if (valid) {
    this.roomJoinButton_.disabled = false;
    this.roomIdInput_.classList.remove('invalid');
    this.roomIdInputLabel_.classList.add('hidden');
  } else {
    this.roomJoinButton_.disabled = true;
    this.roomIdInput_.classList.add('invalid');
    this.roomIdInputLabel_.classList.remove('hidden');
  }
};

RoomSelection.prototype.onSignalingVersionChange_ = function() {
  this.applySignalingVersionUi_();
  this.recentlyUsedList_.getRecentRooms().then(function(recentRooms) {
    this.buildRecentRoomList_(recentRooms);
  }.bind(this));
};

RoomSelection.prototype.onRoomIdKeyPress_ = function(event) {
  if (event.which !== 13 || this.roomJoinButton_.disabled) {
    return;
  }
  this.onJoinButton_();
};

/// Shape the room-selection controls for the selected protocol. V2 mints a link and shows
/// it read-only; V1 keeps the free-form numeric room id it has always had.
RoomSelection.prototype.applySignalingVersionUi_ = function() {
  var v2 = this.signalingV2Checkbox_.checked;
  if (v2) {
    if (this.instructions_) {
      this.instructions_.textContent =
          'Join the room link or generate a new room link';
    }
    this.roomIdInput_.readOnly = true;
    this.roomRandomButton_.textContent = 'GENERATE';
    this.roomIdInputLabel_.textContent = 'Enter a valid room link, or generate one.';
    if (RoomSelection.roomTokenFrom(this.roomIdInput_.value) === null) {
      this.roomIdInput_.value = this.roomLink_(RoomSelection.generateRoomToken());
    }
    // A V2 room is a link, so show a real anchor rather than a read-only text box: it can
    // be clicked, hovered to reveal the target, opened in a new tab, and copied with
    // "Copy link address". The input stays in the DOM as the value the buttons read.
    this.showRoomLink_(true);
  } else {
    if (this.instructions_) {
      this.instructions_.textContent = 'Please enter a room id:';
    }
    this.roomIdInput_.readOnly = false;
    this.roomRandomButton_.textContent = 'RANDOM';
    this.roomIdInputLabel_.textContent =
        'Room id must be a valid number (up to 18446744073709551615).';
    if (!/^\d+$/.test(this.roomIdInput_.value)) {
      this.roomIdInput_.value = Math.floor(Math.random() * 1000000000).toString();
    }
    this.showRoomLink_(false);
  }
  this.onRoomIdInput_();
};

/// Swap between the V1 text field and the V2 clickable room link.
RoomSelection.prototype.showRoomLink_ = function(show) {
  if (!this.roomSelectionLink_) {
    return;
  }
  this.roomSelectionLink_.classList.toggle('hidden', !show);
  this.roomIdInput_.classList.toggle('hidden', show);
  if (show) {
    this.updateRoomLink_();
  }
};

/// Mirror the current room into the anchor, so its href always matches what JOIN would do.
RoomSelection.prototype.updateRoomLink_ = function() {
  if (!this.roomSelectionLink_) {
    return;
  }
  var link = this.roomIdInput_.value;
  this.roomSelectionLink_.href = link;
  this.roomSelectionLink_.textContent = link;
  this.roomSelectionLink_.title = 'Click to join this room';
};

/// Join through the app rather than reloading the page, matching the recent-room links.
/// Modified clicks (new tab, new window, middle click) are left to the browser.
RoomSelection.prototype.onRoomLinkClick_ = function(event) {
  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey ||
      (typeof event.button === 'number' && event.button !== 0)) {
    return;
  }
  var token = RoomSelection.roomTokenFrom(this.roomSelectionLink_.href);
  if (token === null) {
    return;
  }
  event.preventDefault();
  this.loadRoom_(token);
};

/// The absolute link a room token is shared as.
RoomSelection.prototype.roomLink_ = function(token) {
  return location.origin + '/v2/r/' + encodeURIComponent(token);
};

RoomSelection.prototype.onRandomButton_ = function() {
  if (this.signalingV2Checkbox_.checked) {
    this.roomIdInput_.value = this.roomLink_(RoomSelection.generateRoomToken());
    this.updateRoomLink_();
  } else {
    this.roomIdInput_.value = Math.floor(Math.random() * 1000000000).toString();
  }
  this.onRoomIdInput_();
};

RoomSelection.prototype.onJoinButton_ = function() {
  if (this.signalingV2Checkbox_.checked) {
    var token = RoomSelection.roomTokenFrom(this.roomIdInput_.value);
    if (token === null) {
      return;
    }
    this.loadRoom_(token);
    return;
  }
  this.loadRoom_(this.roomIdInput_.value);
};

RoomSelection.prototype.makeRecentlyUsedClickHandler_ = function(roomName) {
  return function(e) {
    e.preventDefault();
    this.loadRoom_(roomName);
  };
};

RoomSelection.prototype.loadRoom_ = function(roomName) {
  this.recentlyUsedList_.pushRecentRoom(roomName);
  if (this.onRoomSelected) {
    this.onRoomSelected(roomName, this.signalingV2Checkbox_.checked ? 2 : 1);
  }
};

RoomSelection.prototype.roomPath_ = function(roomName) {
  return (this.signalingV2Checkbox_.checked ? '/v2/r/' : '/r/') +
      encodeURIComponent(roomName);
};

RoomSelection.RecentlyUsedList = function(key) {
  // This is the length of the most recently used list.
  this.LISTLENGTH_ = 10;

  this.RECENTROOMSKEY_ = key || 'recentRooms';
  this.storage_ = new Storage();
};

// Add a room to the recently used list and store to local storage.
RoomSelection.RecentlyUsedList.prototype.pushRecentRoom = function(roomId) {
  // Push recent room to top of recent list, keep max of this.LISTLENGTH_
  // entries.
  return new Promise(function(resolve, reject) {
    if (!roomId) {
      resolve();
      return;
    }

    this.getRecentRooms().then(function(recentRooms) {
      recentRooms = [roomId].concat(recentRooms);
      // Remove any duplicates from the list, leaving the first occurance.
      recentRooms = recentRooms.filter(function(value, index, self) {
        return self.indexOf(value) === index;
      });
      recentRooms = recentRooms.slice(0, this.LISTLENGTH_);
      this.storage_.setStorage(this.RECENTROOMSKEY_,
          JSON.stringify(recentRooms), function() {
            resolve();
          });
    }.bind(this)).catch(function(err) {
      reject(err);
    }.bind(this));
  }.bind(this));
};

// Get the list of recently used rooms from local storage.
RoomSelection.RecentlyUsedList.prototype.getRecentRooms = function() {
  return new Promise(function(resolve) {
    this.storage_.getStorage(this.RECENTROOMSKEY_, function(value) {
      var recentRooms = parseJSON(value);
      if (!recentRooms) {
        recentRooms = [];
      }
      resolve(recentRooms);
    });
  }.bind(this));
};
