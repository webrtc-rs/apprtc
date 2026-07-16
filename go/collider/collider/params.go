// Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

package collider

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"strings"
)

// roomParameters is the port of apprtc.py::get_room_parameters. The JSON tags
// match the keys the web app expects (see web_app/js/*.js and the templates).
// The *_json fields are pre-marshaled JSON strings that the templates inject
// verbatim (the Jinja "| safe" filter), so the JS literals parse correctly.
type roomParameters struct {
	// Plain string params (default-escaped in templates).
	ClientID            string `json:"client_id,omitempty"`
	RoomID              string `json:"room_id,omitempty"`
	RoomLink            string `json:"room_link,omitempty"`
	WssURL              string `json:"wss_url"`
	WssPostURL          string `json:"wss_post_url"`
	IceServerURL        string `json:"ice_server_url"`
	IceServerTransports string `json:"ice_server_transports"`
	HeaderMessage       string `json:"header_message"`
	IsInitiator         string `json:"is_initiator,omitempty"`

	// JSON-valued params injected raw into the page (Jinja "| safe").
	IsLoopback             string `json:"is_loopback"`
	PcConfig               string `json:"pc_config"`
	PcConstraints          string `json:"pc_constraints"`
	OfferOptions           string `json:"offer_options"`
	MediaConstraints       string `json:"media_constraints"`
	BypassJoinConfirmation string `json:"bypass_join_confirmation"`
	VersionInfo            string `json:"version_info"`
	IncludeLoopbackJS      string `json:"include_loopback_js"`

	// Messages queued for the joining client + advisory messages.
	Messages        []string `json:"messages,omitempty"`
	ErrorMessages   []string `json:"error_messages"`
	WarningMessages []string `json:"warning_messages"`
}

func mustJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(b)
}

// generateRandom returns a numeric string of the given length, mirroring
// apprtc.py::generate_random.
func generateRandom(length int) string {
	const digits = "0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = digits[rand.Intn(len(digits))]
	}
	return string(b)
}

// selfOrigin returns the (scheme, host) this server is reachable at, honoring
// the Host/ForceTLS config overrides. Mirrors maybe_use_https_host_url.
func (cfg *Config) selfOrigin(r *http.Request) (httpScheme, wsScheme, host string) {
	host = cfg.Host
	if host == "" {
		host = r.Host
	}
	tls := cfg.ForceTLS || r.TLS != nil
	if tls {
		return "https", "wss", host
	}
	return "http", "ws", host
}

// buildRoomParameters ports get_room_parameters. roomID/clientID may be empty
// (e.g. for the landing page or /params). isInitiator is nil unless known.
func (cfg *Config) buildRoomParameters(r *http.Request, roomID, clientID string, isInitiator *bool) roomParameters {
	httpScheme, wsScheme, host := cfg.selfOrigin(r)

	// Single, self-hosted Collider: the WSS server is this binary.
	wssURL := wsScheme + "://" + host + "/ws"
	wssPostURL := httpScheme + "://" + host

	// pc_config: iceServers filled in by the client via the TURN request, plus
	// the override when configured.
	pcConfig := map[string]interface{}{
		"iceServers":    []interface{}{},
		"bundlePolicy":  "max-bundle",
		"rtcpMuxPolicy": "require",
	}
	if len(cfg.IceServerOverride) > 0 {
		pcConfig["iceServers"] = cfg.IceServerOverride
	}
	if it := r.URL.Query().Get("it"); it != "" {
		pcConfig["iceTransports"] = it
	}

	// ice_server_url: where the client fetches TURN credentials. Defaults to
	// this server's own /v1alpha/iceconfig endpoint.
	base := r.URL.Query().Get("ts")
	if base == "" {
		base = cfg.IceServerBaseUrl
	}
	if base == "" {
		base = httpScheme + "://" + host
	}
	iceServerURL := ""
	if base != "" {
		iceServerURL = base + "/v1alpha/iceconfig?key=" + cfg.IceServerApiKey
	}

	isLoopback := r.URL.Query().Get("debug") == "loopback"
	includeLoopbackJS := ""
	if isLoopback {
		includeLoopbackJS = `<script src="/js/loopback.js"></script>`
	}

	params := roomParameters{
		WssURL:              wssURL,
		WssPostURL:          wssPostURL,
		IceServerURL:        iceServerURL,
		IceServerTransports: r.URL.Query().Get("tt"),
		HeaderMessage:       cfg.HeaderMessage,

		IsLoopback:             mustJSON(isLoopback),
		PcConfig:               mustJSON(pcConfig),
		PcConstraints:          mustJSON(map[string]interface{}{"optional": []interface{}{}}),
		OfferOptions:           "{}",
		MediaConstraints:       mustJSON(map[string]interface{}{"audio": true, "video": true}),
		BypassJoinConfirmation: mustJSON(cfg.BypassJoinConfirmation),
		VersionInfo:            "null",
		IncludeLoopbackJS:      includeLoopbackJS,

		ErrorMessages:   []string{},
		WarningMessages: []string{},
	}

	if roomID != "" {
		params.RoomID = roomID
		roomLink := httpScheme + "://" + host + "/r/" + roomID
		if q := r.URL.RawQuery; q != "" {
			roomLink += "?" + q
		}
		params.RoomLink = roomLink
	}
	if clientID != "" {
		params.ClientID = clientID
	}
	if isInitiator != nil {
		params.IsInitiator = boolStr(*isInitiator)
	}
	return params
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// iceConfig returns the body of the /v1alpha/iceconfig response, mirroring
// IceConfigurationPage.
func (cfg *Config) iceConfig() map[string]interface{} {
	if len(cfg.IceServerOverride) > 0 {
		return map[string]interface{}{"iceServers": cfg.IceServerOverride}
	}
	// With no configured urls, return an empty list rather than a single entry
	// whose "urls" is null: the browser rejects {urls: null} with "ICE server
	// protocol not supported" when building the RTCPeerConnection.
	iceServers := []interface{}{}
	if len(cfg.IceServerUrls) > 0 {
		iceServers = append(iceServers,
			map[string]interface{}{"urls": cfg.IceServerUrls})
	}
	return map[string]interface{}{"iceServers": iceServers}
}

// trimColliderPath splits a "/a/b" style path into its non-empty segments.
func trimColliderPath(p string) []string {
	parts := strings.Split(p, "/")
	out := parts[:0]
	for _, s := range parts {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
