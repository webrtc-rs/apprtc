// Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

package collider

import (
	"errors"
	"fmt"
	"io"
	"log"
	"time"
)

const maxRoomCapacity = 2

type room struct {
	parent *roomTable
	id     string
	// A mapping from the client ID to the client object.
	clients         map[string]*client
	registerTimeout time.Duration
	// lastActive is updated on every access so the room table sweeper can reap
	// idle rooms (replaces the memcache TTL from the old room server).
	lastActive time.Time
}

func newRoom(p *roomTable, id string, to time.Duration) *room {
	return &room{parent: p, id: id, clients: make(map[string]*client), registerTimeout: to}
}

// client returns the client, or creates it if it does not exist and the room is not full.
func (rm *room) client(clientID string) (*client, error) {
	if c, ok := rm.clients[clientID]; ok {
		return c, nil
	}
	if len(rm.clients) >= maxRoomCapacity {
		log.Printf("Room %s is full, not adding client %s", rm.id, clientID)
		return nil, errors.New("Max room capacity reached")
	}

	var timer *time.Timer
	if rm.parent != nil {
		timer = time.AfterFunc(rm.registerTimeout, func() {
			if c := rm.clients[clientID]; c != nil {
				rm.parent.removeIfUnregistered(rm.id, c)
			}
		})
	}
	rm.clients[clientID] = newClient(clientID, timer)

	log.Printf("Added client %s to room %s", clientID, rm.id)

	return rm.clients[clientID], nil
}

// register binds a client to the ReadWriteCloser.
func (rm *room) register(clientID string, rwc io.ReadWriteCloser) error {
	c, err := rm.client(clientID)
	if err != nil {
		return err
	}
	if err = c.register(rwc); err != nil {
		return err
	}

	log.Printf("Client %s registered in room %s", clientID, rm.id)

	// Sends the queued messages from the other client of the room.
	if len(rm.clients) > 1 {
		for _, otherClient := range rm.clients {
			otherClient.sendQueued(c)
		}
	}
	return nil
}

// send sends the message to the other client of the room, or queues the message if the other client has not joined.
func (rm *room) send(srcClientID string, msg string) error {
	src, err := rm.client(srcClientID)
	if err != nil {
		return err
	}

	// Queue the message if the other client has not joined.
	if len(rm.clients) == 1 {
		return rm.clients[srcClientID].enqueue(msg)
	}

	// Send the message to the other client of the room.
	for _, oc := range rm.clients {
		if oc.id != srcClientID {
			return src.send(oc, msg)
		}
	}

	// The room must be corrupted.
	return errors.New(fmt.Sprintf("Corrupted room %+v", rm))
}

// remove closes the client connection and removes the client specified by the |clientID|.
//
// When the room server was a separate App Engine service, this also POSTed a
// /leave callback to keep memcache occupancy in sync. Now that the room server
// lives in this process, removal updates the in-process room directly and
// promotes the surviving client to initiator (port of
// apprtc.py::remove_client_from_room).
func (rm *room) remove(clientID string) {
	if c, ok := rm.clients[clientID]; ok {
		c.deregister()
		delete(rm.clients, clientID)
		log.Printf("Removed client %s from room %s", clientID, rm.id)

		// Promote the remaining client (if any) to initiator so it can accept
		// a new peer.
		for _, other := range rm.clients {
			other.isInitiator = true
		}
	}
}

// addClient allocates a new client for a /join request: it elects the initiator
// (first client in the room) and returns the messages queued by the other
// client so the joiner can replay the existing offer/ICE. Port of
// apprtc.py::add_client_to_room (the memcache compare-and-set retry loop
// collapses to the roomTable mutex held by the caller).
func (rm *room) addClient(clientID string, isLoopback bool) (isInitiator bool, messages []string, err error) {
	if _, ok := rm.clients[clientID]; ok {
		return false, nil, errors.New("DUPLICATE_CLIENT")
	}
	if len(rm.clients) >= maxRoomCapacity {
		return false, nil, errors.New("FULL")
	}

	isInitiator = len(rm.clients) == 0

	if !isInitiator {
		// Hand the joiner the initiator's queued offer/ICE and clear the queue.
		for _, other := range rm.clients {
			messages = append(messages, other.msgs...)
			other.msgs = nil
		}
	}

	c, err := rm.client(clientID)
	if err != nil {
		return false, nil, err
	}
	c.isInitiator = isInitiator

	if isLoopback {
		// Mirror the loopback debug path: add a second, non-initiator client.
		if lc, err := rm.client(loopbackClientID); err == nil {
			lc.isInitiator = false
		}
	}
	return isInitiator, messages, nil
}

// occupancy returns the number of clients in the room.
func (rm *room) occupancy() int {
	return len(rm.clients)
}

// empty returns true if there is no client in the room.
func (rm *room) empty() bool {
	return len(rm.clients) == 0
}

func (rm *room) wsCount() int {
	count := 0
	for _, c := range rm.clients {
		if c.registered() {
			count += 1
		}
	}
	return count
}
