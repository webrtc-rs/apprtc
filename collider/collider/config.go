// Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

package collider

// loopbackClientID is the synthetic peer added in loopback debug mode (was
// constants.LOOPBACK_CLIENT_ID).
const loopbackClientID = "LOOPBACK_CLIENT_ID"

// IceServer mirrors a single entry of the RTCConfiguration.iceServers array.
type IceServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

// Config holds the server configuration that used to live in the App Engine
// room server (constants.py + app.yaml env_variables). It is populated from
// flags/env by collidermain and consumed by the room server handlers.
type Config struct {
	// WebRoot is the path to the web_app/ directory served as static assets.
	WebRoot string

	// Host, if set, overrides the public host:port used to build self URLs
	// (wss_url/wss_post_url/room_link). When empty the request Host is used.
	Host string

	// ForceTLS, if true, builds https/wss self URLs even when the incoming
	// request looks like plain HTTP (e.g. behind a TLS-terminating proxy).
	ForceTLS bool

	// IceServerOverride, if non-nil, is returned verbatim from /v1alpha/iceconfig
	// and used as the iceServers of the peer connection config (was
	// constants.ICE_SERVER_OVERRIDE).
	IceServerOverride []IceServer

	// IceServerUrls is the list of ICE urls returned from /v1alpha/iceconfig
	// when no override is set (was constants.ICE_SERVER_URLS).
	IceServerUrls []string

	// IceServerBaseUrl is the origin of the ICE server provider used to build
	// ice_server_url. When empty, the server's own origin is used so the page
	// fetches /v1alpha/iceconfig from this binary.
	IceServerBaseUrl string

	// IceServerApiKey is the api key appended to ice_server_url.
	IceServerApiKey string

	// HeaderMessage is an optional banner shown on every page (was HEADER_MESSAGE).
	HeaderMessage string

	// BypassJoinConfirmation skips the "Ready to join?" prompt (was
	// BYPASS_JOIN_CONFIRMATION).
	BypassJoinConfirmation bool

	// RoomMaxAgeSec is how long an idle room is kept before the sweeper reaps
	// it (was ROOM_MEMCACHE_EXPIRATION_SEC). 0 disables the sweeper.
	RoomMaxAgeSec int
}
