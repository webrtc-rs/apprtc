// Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

package collider

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

// roomServer bundles the static web app and the room API handlers that used to
// live in the App Engine room server (apprtc.py). It shares the Collider's
// roomTable so message forwarding and occupancy are a single in-process model.
type roomServer struct {
	cfg       *Config
	tmpl      *templates
	roomTable *roomTable
	fileSrv   http.Handler
}

func newRoomServer(cfg *Config, rt *roomTable) (*roomServer, error) {
	tmpl, err := loadTemplates(cfg.WebRoot)
	if err != nil {
		return nil, err
	}
	return &roomServer{
		cfg:       cfg,
		tmpl:      tmpl,
		roomTable: rt,
		fileSrv:   http.FileServer(http.Dir(cfg.WebRoot)),
	}, nil
}

// writeJSON writes v as a JSON response.
func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(v); err != nil {
		log.Printf("Failed to encode JSON response: %v", err)
	}
}

// mainPage renders the room-selection landing page (apprtc.py::MainPage).
func (s *roomServer) mainPage(w http.ResponseWriter, r *http.Request) {
	params := s.cfg.buildRoomParameters(r, "", "", nil)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.index.Execute(w, params.toTemplateContext()); err != nil {
		log.Printf("Failed to render index: %v", err)
	}
}

// roomPage renders the call page for /r/{roomid}, or the "room full" page if the
// room already has two clients (apprtc.py::RoomPage).
func (s *roomServer) roomPage(w http.ResponseWriter, r *http.Request, roomID string) {
	if s.roomTable.occupancy(roomID) >= maxRoomCapacity {
		log.Printf("Room %s is full", roomID)
		params := s.cfg.buildRoomParameters(r, roomID, "", nil)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := s.tmpl.full.Execute(w, params.toTemplateContext()); err != nil {
			log.Printf("Failed to render full: %v", err)
		}
		return
	}
	params := s.cfg.buildRoomParameters(r, roomID, "", nil)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.index.Execute(w, params.toTemplateContext()); err != nil {
		log.Printf("Failed to render index: %v", err)
	}
}

// paramsPage returns the room-independent parameters (apprtc.py::ParamsPage).
func (s *roomServer) paramsPage(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.cfg.buildRoomParameters(r, "", "", nil))
}

// iceConfig returns the ICE/TURN server list (apprtc.py::IceConfigurationPage).
func (s *roomServer) iceConfigPage(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.cfg.iceConfig())
}

// joinResponse is the body of a /join reply (apprtc.py::JoinPage.write_response).
type joinResponse struct {
	Result string         `json:"result"`
	Params roomParameters `json:"params"`
}

// join handles POST /join/{roomid} (apprtc.py::JoinPage).
func (s *roomServer) join(w http.ResponseWriter, r *http.Request, roomID string) {
	clientID := generateRandom(8)
	isLoopback := r.URL.Query().Get("debug") == "loopback"

	isInitiator, messages, err := s.roomTable.join(roomID, clientID, isLoopback)
	if err != nil {
		// err.Error() is the AppRTC result code, e.g. FULL or DUPLICATE_CLIENT.
		writeJSON(w, joinResponse{Result: err.Error()})
		return
	}

	params := s.cfg.buildRoomParameters(r, roomID, clientID, &isInitiator)
	params.Messages = messages
	log.Printf("User %s joined room %s (initiator=%t)", clientID, roomID, isInitiator)
	writeJSON(w, joinResponse{Result: "SUCCESS", Params: params})
}

// leave handles POST /leave/{roomid}/{clientid} (apprtc.py::LeavePage).
func (s *roomServer) leave(w http.ResponseWriter, r *http.Request, roomID, clientID string) {
	s.roomTable.leave(roomID, clientID)
}

// message handles POST /message/{roomid}/{clientid} (apprtc.py::MessagePage).
// The store-while-alone / forward-when-paired logic that the App Engine server
// split between memcache and an HTTP POST to Collider is now a single
// in-process roomTable operation.
func (s *roomServer) message(w http.ResponseWriter, r *http.Request, roomID, clientID string) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.roomTable.saveOrSend(roomID, clientID, string(body)); err != nil {
		writeJSON(w, map[string]string{"result": err.Error()})
		return
	}
	writeJSON(w, map[string]string{"result": "SUCCESS"})
}

// --- prefix-route adapters: parse the path and dispatch -------------------

// handleRoot serves "/" — the landing page — or falls through to the static
// file server for asset paths (/js, /css, /images, *.html, robots.txt).
func (s *roomServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		s.mainPage(w, r)
		return
	}
	s.fileSrv.ServeHTTP(w, r)
}

// handleRoom serves GET /r/{roomid}.
func (s *roomServer) handleRoom(w http.ResponseWriter, r *http.Request) {
	p := trimColliderPath(strings.TrimPrefix(r.URL.Path, "/r"))
	if len(p) != 1 {
		http.NotFound(w, r)
		return
	}
	s.roomPage(w, r, p[0])
}

// handleJoin serves POST /join/{roomid}.
func (s *roomServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	p := trimColliderPath(strings.TrimPrefix(r.URL.Path, "/join"))
	if len(p) != 1 || r.Method != "POST" {
		http.NotFound(w, r)
		return
	}
	s.join(w, r, p[0])
}

// handleLeave serves POST /leave/{roomid}/{clientid}.
func (s *roomServer) handleLeave(w http.ResponseWriter, r *http.Request) {
	p := trimColliderPath(strings.TrimPrefix(r.URL.Path, "/leave"))
	if len(p) != 2 || r.Method != "POST" {
		http.NotFound(w, r)
		return
	}
	s.leave(w, r, p[0], p[1])
}

// handleMessage serves POST /message/{roomid}/{clientid}.
func (s *roomServer) handleMessage(w http.ResponseWriter, r *http.Request) {
	p := trimColliderPath(strings.TrimPrefix(r.URL.Path, "/message"))
	if len(p) != 2 || r.Method != "POST" {
		http.NotFound(w, r)
		return
	}
	s.message(w, r, p[0], p[1])
}
