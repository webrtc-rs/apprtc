// Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

package main

import (
	"collider/collider"
	"flag"
	"log"
	"strings"
)

var tls = flag.Bool("tls", false, "whether TLS is used: TLS is used for wss and https, otherwise ws and http")
var port = flag.Int("port", 80, "The TCP port that the server listens on: 443 for TLS, 80 for non-TLS")
var host = flag.String("host", "", "Public host:port used to build self URLs (wss/room links); defaults to the request Host")
var webRoot = flag.String("web-root", "../web_app", "Path to the web_app directory served as static assets")
var iceServerURLs = flag.String("ice-server-urls", "stun:stun.l.google.com:19302", "Comma-separated ICE/STUN/TURN urls returned from /v1alpha/iceconfig")
var headerMessage = flag.String("header-message", "", "Optional banner shown on every page")
var bypassJoinConfirmation = flag.Bool("bypass-join-confirmation", false, "Skip the join confirmation prompt")
var roomMaxAgeSec = flag.Int("room-max-age-sec", 60*60*24, "Idle room TTL in seconds (0 disables the sweeper)")

func splitCommaList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func main() {
	flag.Parse()

	cfg := &collider.Config{
		WebRoot:                *webRoot,
		Host:                   *host,
		ForceTLS:               *tls,
		IceServerUrls:          splitCommaList(*iceServerURLs),
		HeaderMessage:          *headerMessage,
		BypassJoinConfirmation: *bypassJoinConfirmation,
		RoomMaxAgeSec:          *roomMaxAgeSec,
	}

	log.Printf("Starting collider: tls = %t, port = %d, web-root = %s", *tls, *port, *webRoot)

	c, err := collider.NewCollider(cfg)
	if err != nil {
		log.Fatalf("Failed to create collider: %v", err)
	}
	c.Run(*port, *tls)
}
