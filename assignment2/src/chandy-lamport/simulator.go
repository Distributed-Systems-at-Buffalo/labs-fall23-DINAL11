package chandy_lamport

import (
	"fmt"
	"log"
	"math/rand"
)

// Max random delay added to packet delivery
const maxDelay = 5

// Simulator is the entry point to the distributed snapshot application.
//
// It is a discrete time simulator, i.e. events that happen at time t + 1 come
// strictly after events that happen at time t. At each time step, the simulator
// examines messages queued up across all the links in the system and decides
// which ones to deliver to the destination.
//
// The simulator is responsible for starting the snapshot process, inducing servers
// to pass tokens to each other, and collecting the snapshot state after the process
// has terminated.

type Simulator struct {
	time           int
	nextSnapshotId int
	servers        map[string]*Server
	logger         *Logger
	// TODO: ADD MORE FIELDS HERE
	Final map[int]map[string]chan bool
	ss    map[int]chan string
	//snapshotState map[string][]*SnapshotMessage // key = server ID
	//snapshotComplete bool
	//allch map[int]chan string
	//mu    sync.Mutex // Mutex for concurrent access
}

func NewSimulator() *Simulator {
	return &Simulator{
		0,
		0,
		make(map[string]*Server),
		NewLogger(),
		make(map[int]map[string]chan bool),
		make(map[int]chan string),
		//false,
		//make(map[int]chan string),
		//sync.Mutex{}, // Initialize the mutex
	}
}

// Return the receive time of a message after adding a random delay.
// Note: since we only deliver one message to a given server at each time step,
// the message may be received after the time step returned in this function.

func (sim *Simulator) GetReceiveTime() int {
	return sim.time + 1 + rand.Intn(5)
}

// Add a server to this simulator with the specified number of starting tokens

func (sim *Simulator) AddServer(id string, tokens int) {
	server := NewServer(id, tokens, sim)
	sim.servers[id] = server
}

// Add a unidirectional link between two servers

func (sim *Simulator) AddForwardLink(src string, dest string) {
	server1, ok1 := sim.servers[src]
	server2, ok2 := sim.servers[dest]
	if !ok1 {
		log.Fatalf("Server %v does not exist\n", src)
	}
	if !ok2 {
		log.Fatalf("Server %v does not exist\n", dest)
	}
	server1.AddOutboundLink(server2)
}

// Run an event in the system

func (sim *Simulator) InjectEvent(event interface{}) {
	switch event := event.(type) {
	case PassTokenEvent:
		src := sim.servers[event.src]
		src.SendTokens(event.tokens, event.dest)
		fmt.Printf("Injected PassTokenEvent: src=%s, dest=%s, tokens=%d\n", event.src, event.dest, event.tokens)

	case SnapshotEvent:
		sim.StartSnapshot(event.serverId)
		fmt.Printf("Injected SnapshotEvent: serverId=%s\n", event.serverId)

	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Advance the simulator time forward by one step, handling all send message events
// that expire at the new time step, if any.

func (sim *Simulator) Tick() {
	sim.time++
	//fmt.Printf("Time Step: %d\n", sim.time)

	sim.logger.NewEpoch()
	// Note: to ensure deterministic ordering of packet delivery across the servers,
	// we must also iterate through the servers and the links in a deterministic way
	for _, serverId := range getSortedKeys(sim.servers) {
		server := sim.servers[serverId]
		//fmt.Printf("Processing server: %s\n", serverId)

		for _, dest := range getSortedKeys(server.outboundLinks) {
			link := server.outboundLinks[dest]
			//fmt.Printf("Processing link from %s to %s\n", serverId, dest)

			// Deliver at most one packet per server at each time step to
			// establish total ordering of packet delivery to each server
			if !link.events.Empty() {
				//fmt.Println("donot reach here")
				e := link.events.Peek().(SendMessageEvent)
				fmt.Printf("Next event from %s to %s: receiveTime=%d, message=%v\n", e.src, e.dest, e.receiveTime, e.message)

				if e.receiveTime <= sim.time {
					link.events.Pop()
					fmt.Println(e.receiveTime)
					//fmt.Printf("Delivering message from %s to %s\n", e.src, e.dest)

					sim.logger.RecordEvent(
						sim.servers[e.dest],
						ReceivedMessageEvent{e.src, e.dest, e.message})
					sim.servers[e.dest].HandlePacket(e.src, e.message)
					break
				}
			}
		}
	}
}

// Start a new snapshot process at the specified server

func (sim *Simulator) StartSnapshot(serverId string) {
	snapshotId := sim.nextSnapshotId
	sim.nextSnapshotId++

	// TODO: IMPLEMENT ME
	// #1 initialize the Channel Map map by making the new string channel map
	// and iterate throught the servers in simulator to initilaize the boolean channel
	channelMap := make(map[string]chan bool)
	for serId, _ := range sim.servers {
		channelMap[serId] = make(chan bool)
	}

	// #2 assign the channelMap to Final simulator
	sim.Final[snapshotId] = channelMap

	// #3 Iterate through the servers map in simulator, check if the servers are legit and if not return the process
	// and throw Fatal error
	server, legit := sim.servers[serverId]
	if legit == false {
		// Error: Server ID doesn't exist
		log.Fatalf("Error: Server ID '%s' doesn't exist.\n", serverId)
		return
	}

	// #3 Start the snapshot process on the specified server
	server.StartSnapshot(snapshotId)
	fmt.Printf("Started snapshot %d at server: %s\n", snapshotId, serverId)

	// #4 also record the event of calling the startsnapshot in simulator to calling the startsnapshot in server
	sim.logger.RecordEvent(server, StartSnapshot{serverId, snapshotId})

}

// Callback for servers to notify the simulator that the snapshot process has
// completed on a particular server

func (sim *Simulator) NotifySnapshotComplete(serverId string, snapshotId int) {
	sim.logger.RecordEvent(sim.servers[serverId], EndSnapshot{serverId, snapshotId})
	// TODO: IMPLEMENT ME

	// NotifySnapshotComplete function is called once the Marker message are received from all the
	// inboundlinks concerned to that server, changed the location from server.startsnapshot to handleMarkermessage

	// #1
	// Check if the channel for the snapshotID linked with the serverId exists or not,
	// if doesn't exist return the function
	sim.Final[snapshotId][serverId] = func() chan bool {
		ch, exists := sim.Final[snapshotId][serverId]
		if exists {
			return ch
		}
		fmt.Printf("The Final channel with the %d and %s doens't exist\n", snapshotId, serverId)
		return make(chan bool)
	}()

	// #2 Iterate through the servers map in simulator, check if the servers are legit and if not return the process
	// and throw Fatal error
	_, legit := sim.servers[serverId]
	if legit == false {
		// Error: Server ID doesn't exist
		log.Fatalf("Error: Server ID '%s' doesn't exist.\n", serverId)
		return
	}
	//newly added
	//if len(sim.ss[snapshotId]) == len(sim.Final[snapshotId]){
	//	sim.Final[snapshotId][serverId] <- true
	//
	//}

	// #3 Send the [serverID] boolean now, through the channel to signal completion of the snapshot
	sim.Final[snapshotId][serverId] <- true

	// Print the snapshot completion of the snapshotID linked with that serverId
	fmt.Printf("Snapshot %d completed at server: %s\n", snapshotId, serverId)
}

// CollectSnapshot collects and merges snapshot state from all the servers.
// This function blocks until the snapshot process has completed on all servers.

func (sim *Simulator) CollectSnapshot(snapshotId int) *SnapshotState {
	//snap := SnapshotState{snapshotId, make(map[string]int), make([]*SnapshotMessage, 0)}
	//// TODO: IMPLEMENT METHOD

	//
	// Get the total number of channels in the snapshot state for the specified snapshot ID
	totalChannels := len(sim.Final[snapshotId])

	// Create a counter to keep track of completed servers
	completedServers := 0

	// Iterate through the channels and wait for completion signals
	for serverID, ch := range sim.Final[snapshotId] {
		// Wait for the signal from the channel
		<-ch

		// Increment the completedServers counter
		completedServers++

		// Print the snapshot completion of the snapshotID linked with that serverId
		fmt.Printf("Snapshot %d completed at server: %s\n", snapshotId, serverID)
	}

	// Check if all servers have completed the snapshot process
	if completedServers == totalChannels {
		token := make(map[string]int)
		messages := make([]*SnapshotMessage, 0)

		// Iterate through the servers
		for _, server := range sim.servers {
			// Retrieve the snapshot state for the specified snapshot ID
			if snapshot, found := server.snapState.Load(snapshotId); found {
				// Check if the snapshot is of type *Snapshot
				if snapshot, ok := snapshot.(*Snapshot); ok {
					// Extract tokens and messages from the snapshot and store them in respective maps
					token[server.Id] = snapshot.tokens
					messages = append(messages, snapshot.messages...)
				}
			}
		}

		snap := SnapshotState{snapshotId, token, messages}

		println("collect snapshot complete")

		return &snap
	}

	// Return nil if all servers have not completed the snapshot process
	return nil
}

// RecordSnapshotMessage records a snapshot message for a server in the snapshot state
//func (sim *Simulator) RecordSnapshotMessage(src string, dest string, message interface{}) {
//
//	snapMessage := &SnapshotMessage{src, dest, message}
//	serverMessages, ok := sim.snapshotState[dest]
//	if !ok {
//		serverMessages = []*SnapshotMessage{snapMessage}
//	} else {
//		serverMessages = append(serverMessages, snapMessage)
//	}
//	//added new
//	sim.snapshotState[dest] = serverMessages
//
//	fmt.Printf("Recorded snapshot message: %s -> %s: %v\n", src, dest, message)
//
//}
