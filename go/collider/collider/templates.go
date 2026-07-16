// Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

package collider

import (
	"html/template"
	"io/ioutil"
	"path/filepath"
	"strings"
)

// templateContext is the typed render context for index_template.html and
// full_template.html. Fields injected as raw JS/HTML (the Jinja "| safe"
// filter, or values placed directly into a <script> literal) are typed
// template.JS / template.HTML so the html/template auto-escaper inserts them
// verbatim instead of double-escaping the JSON.
type templateContext struct {
	RoomID              string
	RoomLink            string
	HeaderMessage       string
	WssURL              string
	WssPostURL          string
	IceServerURL        string
	IceServerTransports string

	ErrorMessages          template.JS
	WarningMessages        template.JS
	IsLoopback             template.JS
	MediaConstraints       template.JS
	OfferOptions           template.JS
	PcConfig               template.JS
	PcConstraints          template.JS
	BypassJoinConfirmation template.JS
	VersionInfo            template.JS
	IncludeLoopbackJS      template.HTML
}

func (p roomParameters) toTemplateContext() templateContext {
	return templateContext{
		RoomID:              p.RoomID,
		RoomLink:            p.RoomLink,
		HeaderMessage:       p.HeaderMessage,
		WssURL:              p.WssURL,
		WssPostURL:          p.WssPostURL,
		IceServerURL:        p.IceServerURL,
		IceServerTransports: p.IceServerTransports,

		ErrorMessages:          template.JS(mustJSON(p.ErrorMessages)),
		WarningMessages:        template.JS(mustJSON(p.WarningMessages)),
		IsLoopback:             template.JS(p.IsLoopback),
		MediaConstraints:       template.JS(p.MediaConstraints),
		OfferOptions:           template.JS(p.OfferOptions),
		PcConfig:               template.JS(p.PcConfig),
		PcConstraints:          template.JS(p.PcConstraints),
		BypassJoinConfirmation: template.JS(p.BypassJoinConfirmation),
		VersionInfo:            template.JS(p.VersionInfo),
		IncludeLoopbackJS:      template.HTML(p.IncludeLoopbackJS),
	}
}

// jinjaToGo rewrites the small, fixed set of Jinja2 constructs used by the
// AppRTC templates into Go html/template syntax. The variable set is closed
// (see web_app/html/*_template.html), so a literal replacer is sufficient and
// avoids pulling in a Jinja engine.
var jinjaToGo = strings.NewReplacer(
	// Control flow.
	"{% if header_message %}", "{{if .HeaderMessage}}",
	"{% if room_id %}", "{{if .RoomID}}",
	"{% endif %}", "{{end}}",

	// "| safe" JSON blobs (list these before the plain forms).
	"{{ media_constraints | safe }}", "{{.MediaConstraints}}",
	"{{ offer_options | safe }}", "{{.OfferOptions}}",
	"{{ pc_config | safe }}", "{{.PcConfig}}",
	"{{ pc_constraints | safe }}", "{{.PcConstraints}}",

	// Plain and raw-JS variables.
	"{{ room_link }}", "{{.RoomLink}}",
	"{{ room_id }}", "{{.RoomID}}",
	"{{ header_message }}", "{{.HeaderMessage}}",
	"{{ error_messages }}", "{{.ErrorMessages}}",
	"{{ warning_messages }}", "{{.WarningMessages}}",
	"{{ is_loopback }}", "{{.IsLoopback}}",
	"{{ ice_server_url }}", "{{.IceServerURL}}",
	"{{ ice_server_transports }}", "{{.IceServerTransports}}",
	"{{ wss_url }}", "{{.WssURL}}",
	"{{ wss_post_url }}", "{{.WssPostURL}}",
	"{{ bypass_join_confirmation }}", "{{.BypassJoinConfirmation}}",
	"{{ version_info }}", "{{.VersionInfo}}",
	"{{ include_loopback_js }}", "{{.IncludeLoopbackJS}}",
)

// templates holds the two parsed page templates.
type templates struct {
	index *template.Template
	full  *template.Template
}

// loadTemplates reads and converts the Jinja templates from <webRoot>/html.
func loadTemplates(webRoot string) (*templates, error) {
	index, err := parseTemplate(filepath.Join(webRoot, "html", "index_template.html"), "index")
	if err != nil {
		return nil, err
	}
	full, err := parseTemplate(filepath.Join(webRoot, "html", "full_template.html"), "full")
	if err != nil {
		return nil, err
	}
	return &templates{index: index, full: full}, nil
}

func parseTemplate(path, name string) (*template.Template, error) {
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return template.New(name).Parse(jinjaToGo.Replace(string(raw)))
}
