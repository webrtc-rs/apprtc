# JavaScript object hierarchy

`AppController`: Connects the UI to `Call`. It owns `Call`, `InfoBox`, and `RoomSelection`; in SFU mode it also owns the publisher-keyed grid tile map and reconciles tiles after each negotiated subscribe offer.

`Call`: Owns local media, `SignalingChannel`, and the active `PeerConnectionClient`. During P2P→SFU upgrade it temporarily retains the old P2P client while creating the new SFU client, then closes P2P when SFU ICE connects. On an `sfu-downgrade` control it does the reverse: closes the SFU client and builds a fresh direct P2P client from the same local tracks, with the control-elected `is_initiator` making one side the offerer.

`SignalingChannel`: Wraps the browser WebSocket. V1 registration is silent; V2 waits for the authoritative `registered` control and stamps every send with the current signal epoch.

`PeerConnectionClient`: Wraps `RTCPeerConnection`, SDP, and trickle ICE. For V2 it implements perfect negotiation: the P2P initiator is impolite, the callee is polite, and every browser is polite toward the SFU. A colliding SFU subscribe offer rolls back/supersedes the browser offer, is answered with its `requestid`, and is followed by a fresh publish offer.

`InfoBox`: Wraps the information and statistics UI.

`RoomSelection`: Owns room selection and chooses the V1 or V2 route. The current checkbox defaults to V2.

`Storage`: Wraps browser local storage.

The shared page includes `full_template.html` for the P2P stage and `grid_template.html` for the SFU participant grid. `onModeChange_` switches between them in both directions: `sfu` swaps the full-screen remote for the grid, and `p2p` (on downgrade) tears the grid down and restores the full-screen stage. The self-view and controls keep their position throughout.
