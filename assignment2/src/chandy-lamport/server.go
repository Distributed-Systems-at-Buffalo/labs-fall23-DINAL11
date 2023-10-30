package chandy_lamport

import (
	"fmt"
	"log"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	//snapshotstates map[int]*SnapshotState
	snapState *SyncMap
	//snapshotMessages []*SnapshotMessage // Keep track of snapshot messages sent during snapshot process
	//snapshotmessages map[string]int
	//SnMessage map[string]map[int]int
	inProgress map[int]map[string]bool
	done       map[int]map[string]bool
}

type Snapshot struct {
	id       int
	tokens   int
	messages []*SnapshotMessage
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		//make(map[int]*SnapshotState),
		NewSyncMap(),
		//make([]*SnapshotMessage, 0),
		//make(map[string]int),
		//make(map[string]map[int]int),
		make(map[int](map[string]bool)),
		make(map[int]map[string]bool),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
	//fmt.Printf("Added link from %s to %s\n", server.Id, dest.Id)

}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}

}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})

}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// HandleIncomingMessage processes messages received by the server.
	switch msg := message.(type) {
	case TokenMessage:
		server.handleTokenMessage(src, msg)
	case MarkerMessage:
		server.handleMarkerMessage(src, msg)
	default:
		//log.Fatal("Error unknown message: ", message)
		log.Fatalf("Server %v received unrecognized message: %v\n", server.Id, message)
	}
}

func (server *Server) handleTokenMessage(sender string, tokenMsg TokenMessage) {
	// #1 Update local state with additional tokens before processing the token message
	server.Tokens += tokenMsg.numTokens

	fmt.Printf("Token message handling message from %s \n", sender)

	// #2Iterate through snapState which is a SyncMap of int -> *Snapshot
	// inbuilt Range function lock the map and defers it after the use look in SyncMap.go
	// Range( key, interface{})      key = SID    interface = *Snapshot = shot
	server.snapState.Range(func(SID, shot interface{}) bool {
		// #3 extract all the variables for checking
		snapID, k1 := SID.(int)

		// check if the snapID has inProgress Map running,
		// only inProgress Map needs msg update
		inProgCheck, k2 := server.inProgress[snapID][sender]

		// check the legitimacy of interface data, is the format associated with modified Snapshot???
		inter, legit := shot.(*Snapshot)

		// All the checks above needs to be fulfilled inorder to push the token message further
		if k1 && k2 && legit && inProgCheck {
			// create a new TokenMsg associated with the src dest and message
			ssMsg := &SnapshotMessage{
				src:     sender,
				dest:    server.Id,
				message: tokenMsg,
			}

			// #4 Append the new Token message to the modified Snapshot state in Sync.Map
			inter.messages = append(inter.messages, ssMsg)
		}
		return true
	})
}

func (server *Server) handleMarkerMessage(sender string, markerMsg MarkerMessage) {
	// #1 extract snapshotID associated with the marker msg and check if the map for it already exists or not
	snapshotID := markerMsg.snapshotId

	// #2 Initialize SnState snapshot
	snState := &Snapshot{
		snapshotID,
		server.Tokens,
		make([]*SnapshotMessage, 0),
	}

	fmt.Printf("marker message handling message from %s \n", sender)

	// #3 check if already snapshoted
	// if snapshot exists, toggle the inProgress Map Boolean to False, and toggle the done Map
	// indicating completion of that snapshot from the sender
	// call HandleSnapshot to deal with non-existing snapshot
	_, alreadySnapshoted := server.snapState.LoadOrStore(snapshotID, snState)
	if alreadySnapshoted == false {
		// Call HandleSnapshot to deal with !alreadySnapshotted
		server.HandleSnapshot(sender, snapshotID)
	} else {
		server.inProgress[markerMsg.snapshotId][sender] = false
	}

	// #4 Eventually the process is snapshotted and the corresponding inProgress map is turn False = 0
	// Now toggle the Done Map and signify the snapshot completion
	server.done[markerMsg.snapshotId][sender] = true

	// #5 check if the completion message has been received from all the inboundLinks
	// compare the size of the done Map with the size of inboundLinks
	if len(server.done[snapshotID]) == len(server.inboundLinks) {
		// Notify simulator that snapshot is completed
		server.sim.NotifySnapshotComplete(server.Id, snapshotID)
	}

}

func (server *Server) HandleSnapshot(src string, snapshotId int) {
	// #1 Insert 2 new maps inProgress and done, like in server.startSnapshot
	server.inProgress[snapshotId] = make(map[string]bool)
	server.done[snapshotId] = make(map[string]bool)

	// #2 check if the sender is same as the current server
	// if it is same toggle the inProgress Map to False, process is completed
	// if not same toggle the inProgress Map to True, process is initiated

	// Get the sorted keys of server.inboundLinks
	keys := getSortedKeys(server.inboundLinks)
	// Iterate through the sorted keys and update server.inProgress map
	for _, so := range keys {
		if src != so {
			server.inProgress[snapshotId][so] = true
		} else {
			server.inProgress[snapshotId][so] = false
		}
	}

	// #3 Send Marker Message to all out bound links
	// Send MarkerMessage to neighbors to initiate snapshot recording process
	markerMsg := MarkerMessage{
		snapshotId: snapshotId,
	}
	server.SendToNeighbors(markerMsg)

}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME

	// #1 Let's insert 2 new maps of string and boolean to track the Snapshot process
	// true = 1 = the snapshot is under progress
	// false = 0 = the snapshot process is either done or not yet started
	// assign the completed process and toggle the done map
	// initialize the maps inProgress and done
	server.inProgress[snapshotId] = make(map[string]bool)
	server.done[snapshotId] = make(map[string]bool)

	// #2 collect Snapshot of the current server and store it in the SnState Sync map
	// SyncMap is opted as probably the race condition is affecting the 8 node and 10 node test case on AutoLab

	// Record the server's current tokens in the simulator's snapshot state
	tokens := make(map[string]int)
	tokens[server.Id] = server.Tokens

	// Initialize SnState snapshot
	snState := &Snapshot{
		snapshotId,
		server.Tokens,
		make([]*SnapshotMessage, 0),
	}
	// moved from map[int]*SnapshotState map to Sync Map ====>>> server.snapshotstates[snapshotId] = snState
	server.snapState.Store(snapshotId, snState) // Store an inbuilt function in sync.map.go

	// #3 for all the inboundlinks associated with the server toggle the map of inProgress to true = 1
	// Get the sorted keys of server.inboundLinks
	iter := getSortedKeys(server.inboundLinks)
	// Iterate through the sorted keys and update server.inProgress map
	for _, so := range iter {
		server.inProgress[snapshotId][so] = true
	}
	fmt.Printf("Snapshot %d started on server %s\n", snapshotId, server.Id)

	// #4 Send Marker Message to all out bound links
	// Send MarkerMessage to neighbors to initiate snapshot recording
	markerMsg := MarkerMessage{
		snapshotId: snapshotId,
	}
	server.SendToNeighbors(markerMsg)

	// #5 Notify Simulator the Snapshot has been completed
	//server.sim.NotifySnapshotComplete(server.Id, snapshotId)

}
