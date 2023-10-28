package chandy_lamport

import (
	"log"
	"strconv"
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
	snapshotstates map[int]*SnapshotState
	//snapshotMessages []*SnapshotMessage // Keep track of snapshot messages sent during snapshot process
	snapshotmessages map[string]int
	//SnMessage map[string]map[int]int
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

type snapshotState struct {
	id       int
	tokens   int
	messages []*SnapshotMessage
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		make(map[int]*SnapshotState),
		//make([]*SnapshotMessage, 0),
		make(map[string]int),
		//make(map[string]map[int]int),
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
	//
	// HandleIncomingMessage processes messages received by the server.
	switch msg := message.(type) {
	case TokenMessage:
		server.handleTokenMessage(src, msg)
	case MarkerMessage:
		server.handleMarkerMessage(src, msg)
	default:
		log.Printf("Server %v received unrecognized message: %v\n", server.Id, message)
	}
}

func (server *Server) handleTokenMessage(sender string, tokenMsg TokenMessage) {
	// Update local state before processing the token message
	server.Tokens += tokenMsg.numTokens

	//println(server.snapshotstates[0].messages)
	// Iterate through snapshot states and update messages
	for _, state := range server.snapshotstates {
		println("dinal")
		println(state.messages)
		println(server.Id)
		//key := sender

		Str := strconv.Itoa(state.id)
		Str = Str + "~" + sender

		//// Try the Boolean operation of Mapp
		//if _, z := server.SnMessage[sender][state.id]; !z {
		//	snapshotMsg := &SnapshotMessage{
		//		src:     sender,
		//		dest:    server.Id,
		//		message: tokenMsg,
		//	}
		//	// Append the new snapshot message to the snapshot state
		//	state.messages = append(state.messages, snapshotMsg)
		//
		//}

		// Check if a message from this sender already exists in the snapshot state
		if _, exists := server.snapshotmessages[Str]; !exists {
			// Create a new snapshot message from the token message
			snapshotMsg := &SnapshotMessage{
				src:     sender,
				dest:    server.Id,
				message: tokenMsg,
			}
			// Append the new snapshot message to the snapshot state
			state.messages = append(state.messages, snapshotMsg)
			// Update the snapshot messages map to mark that a message from this sender has been recorded
			//server.snapshotmessages[key] = true
		}
	}
}

func (server *Server) handleMarkerMessage(sender string, markerMsg MarkerMessage) {
	snapshotID := markerMsg.snapshotId
	alreadySnapshoted := server.snapshotstates[snapshotID]

	if alreadySnapshoted == nil {
		server.StartSnapshot(snapshotID)
		//println("N1 snapshot")
	}

	//fmt.Println("marker message handling")

	// Create a unique ID of maps
	Str := strconv.Itoa(snapshotID)
	Str = Str + "~" + sender

	//keys := sender
	server.snapshotmessages[Str] = 0

	/////try the Boolean version of Mapp
	//server.SnMessage[sender][snapshotID] = 0

}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME

	// #1 collect Snapshot of the current server and store it in the Snapshot map

	// Record the server's current tokens in the simulator's snapshot state
	tokens := make(map[string]int)
	tokens[server.Id] = server.Tokens

	// Initialize snapshot state
	snapState := &SnapshotState{
		snapshotId,
		tokens,
		make([]*SnapshotMessage, 0),
	}
	server.snapshotstates[snapshotId] = snapState

	// #2 Send Marker Message to all out bound links
	// Send MarkerMessage to neighbors to initiate snapshot recording

	//markerMsg := MarkerMessage{
	//	snapshotId: snapshotId,
	//}

	server.SendToNeighbors(MarkerMessage{snapshotId})

	// #3 Notify Simulator the Snapshot has been completed

	server.sim.NotifySnapshotComplete(server.Id, snapshotId)
	//fmt.Printf("Snapshot %d started on server %s\n", snapshotId, server.Id)

}
