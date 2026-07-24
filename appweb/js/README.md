# JavaScript object hierarchy

`AppController`: Connects the UI to `Call`. It owns `Call`, `InfoBox`, and `RoomSelection`; in SFU mode it also owns the publisher-keyed grid tile map and reconciles tiles after each negotiated subscribe offer.

`Call`: Owns local media, `SignalingChannel`, and the active `PeerConnectionClient`. Both mode transitions are make-before-break and symmetric: the outgoing client is retained for media continuity while the incoming one negotiates with the same local tracks, then closed once the incoming transport's ICE connects. During P2P→SFU upgrade it retains `p2pPcClient_`; on an `sfu-downgrade` control it retains `sfuPcClient_` (silencing its callbacks first, so a late event from the retired transport cannot act on the new client) and builds a direct P2P client, with the control-elected `is_initiator` making one side the offerer. A bounded timer completes the downgrade even if the peer never negotiates.

`SignalingChannel`: Wraps the browser WebSocket. V1 registration is silent; V2 waits for the authoritative `registered` control and stamps every send with the current signal epoch.

`PeerConnectionClient`: Wraps `RTCPeerConnection`, SDP, and trickle ICE. For V2 it implements perfect negotiation: the P2P initiator is impolite, the callee is polite, and every browser is polite toward the SFU. A colliding SFU subscribe offer rolls back/supersedes the browser offer, is answered with its `requestid`, and is followed by a fresh publish offer.

`InfoBox`: Wraps the information and statistics UI.

`RoomSelection`: Owns room selection and chooses the V1 or V2 route. The current checkbox defaults to V2.

`Storage`: Wraps browser local storage.

The shared page includes `full_template.html` for the P2P stage and `grid_template.html` for the SFU participant grid. `onModeChange_` switches between them in both directions: `sfu` swaps the full-screen remote for the grid, while `downgrading` holds the grid — its tiles now showing the retired SFU connection's last frames — and `p2p` tears the grid down and restores the full-screen stage once the direct remote video can play. The self-view and controls keep their position throughout.
