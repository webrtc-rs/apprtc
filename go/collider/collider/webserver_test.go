// Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

package collider

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// Tests initiator election and queued-message handoff in room.addClient,
// the in-process port of apprtc.py::add_client_to_room.
func TestAddClientInitiatorElection(t *testing.T) {
	r := createNewRoom("room")

	isInit, msgs, err := r.addClient("a", false)
	if err != nil {
		t.Fatalf("addClient(a) error: %v", err)
	}
	if !isInit {
		t.Error("first client should be the initiator")
	}
	if len(msgs) != 0 {
		t.Errorf("first client got messages %v, want none", msgs)
	}

	// The initiator queues an offer before the callee joins.
	r.send("a", "offer")

	isInit, msgs, err = r.addClient("b", false)
	if err != nil {
		t.Fatalf("addClient(b) error: %v", err)
	}
	if isInit {
		t.Error("second client should not be the initiator")
	}
	if len(msgs) != 1 || msgs[0] != "offer" {
		t.Errorf("second client got messages %v, want [offer]", msgs)
	}

	// Third client must be rejected as the room is full.
	if _, _, err = r.addClient("c", false); err == nil {
		t.Error("third client should be rejected with FULL")
	}
}

// Tests that removing a client promotes the survivor to initiator
// (port of apprtc.py::remove_client_from_room).
func TestRemovePromotesInitiator(t *testing.T) {
	r := createNewRoom("room")
	r.addClient("a", false)
	r.addClient("b", false)

	if r.clients["a"].isInitiator != true || r.clients["b"].isInitiator != false {
		t.Fatalf("unexpected initiator flags: a=%v b=%v", r.clients["a"].isInitiator, r.clients["b"].isInitiator)
	}

	r.remove("a")
	if !r.clients["b"].isInitiator {
		t.Error("after removing the initiator, the survivor should be promoted")
	}
}

func testConfig() *Config {
	return &Config{
		WebRoot:       "../../web_app",
		Host:          "example.com",
		ForceTLS:      true,
		IceServerUrls: []string{"stun:stun.l.google.com:19302"},
	}
}

// Tests that buildRoomParameters produces self-referential wss URLs and the
// expected JSON-valued fields.
func TestBuildRoomParameters(t *testing.T) {
	cfg := testConfig()
	r := httptest.NewRequest("GET", "https://example.com/r/abc", nil)
	p := cfg.buildRoomParameters(r, "abc", "client1", nil)

	if p.WssURL != "wss://example.com/ws" {
		t.Errorf("WssURL = %q, want wss://example.com/ws", p.WssURL)
	}
	if p.WssPostURL != "https://example.com" {
		t.Errorf("WssPostURL = %q, want https://example.com", p.WssPostURL)
	}
	if p.RoomID != "abc" || p.ClientID != "client1" {
		t.Errorf("RoomID/ClientID = %q/%q, want abc/client1", p.RoomID, p.ClientID)
	}
	if !strings.Contains(p.PcConfig, "max-bundle") {
		t.Errorf("PcConfig = %q, want it to contain max-bundle", p.PcConfig)
	}
	if !strings.HasPrefix(p.RoomLink, "https://example.com/r/abc") {
		t.Errorf("RoomLink = %q, want it to start with https://example.com/r/abc", p.RoomLink)
	}
}

// Tests the iceConfig body for both override and url-list cases.
func TestIceConfig(t *testing.T) {
	cfg := testConfig()
	ic := cfg.iceConfig()
	if _, ok := ic["iceServers"]; !ok {
		t.Errorf("iceConfig missing iceServers: %v", ic)
	}

	cfg.IceServerOverride = []IceServer{{URLs: []string{"turn:example.com"}, Username: "u", Credential: "c"}}
	ic = cfg.iceConfig()
	servers, ok := ic["iceServers"].([]IceServer)
	if !ok || len(servers) != 1 || servers[0].Username != "u" {
		t.Errorf("iceConfig override = %v, want the configured server", ic)
	}
}

// TestIceConfigNoUrls guards against emitting an iceServers entry whose "urls"
// is null when nothing is configured: the browser rejects {urls: null} with
// "ICE server protocol not supported". The list must be empty instead.
func TestIceConfigNoUrls(t *testing.T) {
	cfg := &Config{}
	ic := cfg.iceConfig()
	servers, ok := ic["iceServers"].([]interface{})
	if !ok {
		t.Fatalf("iceServers = %T, want []interface{}: %v", ic["iceServers"], ic)
	}
	if len(servers) != 0 {
		t.Errorf("iceServers = %v, want an empty list when no urls are configured", servers)
	}
}

// Tests that the Jinja-to-Go template conversion parses and renders the real
// web_app templates with the raw-JS config injected verbatim.
func TestTemplatesRender(t *testing.T) {
	tmpl, err := loadTemplates("../../web_app")
	if err != nil {
		t.Fatalf("loadTemplates error: %v", err)
	}

	cfg := testConfig()
	r := httptest.NewRequest("GET", "https://example.com/r/abc", nil)
	ctx := cfg.buildRoomParameters(r, "abc", "client1", nil).toTemplateContext()

	var sb strings.Builder
	if err := tmpl.index.Execute(&sb, ctx); err != nil {
		t.Fatalf("index.Execute error: %v", err)
	}
	out := sb.String()
	// The raw-JS pc_config must appear unescaped inside the loadingParams script.
	if !strings.Contains(out, "\"bundlePolicy\":\"max-bundle\"") {
		t.Errorf("rendered index missing unescaped pc_config; got:\n%s", out[:min(len(out), 400)])
	}
	// The wss url sits in a single-quoted JS string, so html/template escapes the
	// slashes as \/ (valid JS that the browser parses back to wss://example.com/ws).
	if !strings.Contains(out, `wss:\/\/example.com\/ws`) {
		t.Error("rendered index missing (js-escaped) wss url")
	}

	sb.Reset()
	if err := tmpl.full.Execute(&sb, ctx); err != nil {
		t.Fatalf("full.Execute error: %v", err)
	}
	if !strings.Contains(sb.String(), "room is full") {
		t.Error("rendered full template missing 'room is full'")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
