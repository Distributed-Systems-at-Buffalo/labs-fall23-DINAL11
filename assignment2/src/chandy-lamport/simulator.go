package chandy_lamport

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
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
	snapshotState    map[string][]*SnapshotMessage // key = server ID
	snapshotComplete bool
	allch            map[int]chan string
	mu               sync.Mutex // Mutex for concurrent access

}

func NewSimulator() *Simulator {
	return &Simulator{
		0,
		0,
		make(map[string]*Server),
		NewLogger(),
		make(map[string][]*SnapshotMessage),
		false,
		make(map[int]chan string),
		sync.Mutex{}, // Initialize the mutex
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
		//fmt.Printf("Injected PassTokenEvent: src=%s, dest=%s, tokens=%d\n", event.src, event.dest, event.tokens)

	case SnapshotEvent:
		sim.StartSnapshot(event.serverId)
		//fmt.Printf("Injected SnapshotEvent: serverId=%s\n", event.serverId)

	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Advance the simulator time forward by one step, handling all send message events
// that expire at the new time step, if any.
func (sim *Simulator) Tick() {
	sim.time++
	fmt.Printf("Time Step: %d\n", sim.time)

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
				//fmt.Printf("Next event from %s to %s: receiveTime=%d, message=%v\n", e.src, e.dest, e.receiveTime, e.message)

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
	sim.logger.RecordEvent(sim.servers[serverId], StartSnapshot{serverId, snapshotId})

	// TODO: IMPLEMENT ME

	// Start the snapshot process on the specified server
	server := sim.servers[serverId]
	server.StartSnapshot(snapshotId)
	//fmt.Printf("Started snapshot %d at server: %s\n", snapshotId, serverId)

}

// Callback for servers to notify the simulator that the snapshot process has
// completed on a particular server
func (sim *Simulator) NotifySnapshotComplete(serverId string, snapshotId int) {
	sim.logger.RecordEvent(sim.servers[serverId], EndSnapshot{serverId, snapshotId})
	// TODO: IMPLEMENT ME

	// Check if the channel for the snapshotID exists
	sim.allch[snapshotId] = func() chan string {
		ch, exists := sim.allch[snapshotId]
		if exists {
			return ch
		}
		return make(chan string, 40)
	}()
	//fmt.Printf("Snapshot %d completed at server: %s\n", snapshotId, serverId)

	// Send the serverID through the channel to signal completion
	sim.allch[snapshotId] <- serverId
}

// CollectSnapshot collects and merges snapshot state from all the servers.
// This function blocks until the snapshot process has completed on all servers.
func (sim *Simulator) CollectSnapshot(snapshotId int) *SnapshotState {
	snap := SnapshotState{snapshotId, make(map[string]int), make([]*SnapshotMessage, 0)}
	//// TODO: IMPLEMENT ME

	println("call collectsnapshot")
	for range sim.servers {
		<-sim.allch[snapshotId]
		//fmt.Printf("Received snapshot completion signal from a server %s.\n ", Server{Id: })
	}

	// Merge snapshot state from all servers
	//snap := SnapshotState{snapshotId, make(map[string]int), make([]*SnapshotMessage, 0)}
	for _, server := range sim.servers {
		serverSnapshot := server.snapshotstates[snapshotId]
		for id, token := range serverSnapshot.tokens {
			snap.tokens[id] = token
		}
		snap.messages = append(snap.messages, serverSnapshot.messages...)
	}

	println("collect snapshot complete")
	return &snap
}

// RecordSnapshotMessage records a snapshot message for a server in the snapshot state
func (sim *Simulator) RecordSnapshotMessage(src string, dest string, message interface{}) {

	snapMessage := &SnapshotMessage{src, dest, message}
	serverMessages, ok := sim.snapshotState[dest]
	if !ok {
		serverMessages = []*SnapshotMessage{snapMessage}
	} else {
		serverMessages = append(serverMessages, snapMessage)
	}
	//added new
	sim.snapshotState[dest] = serverMessages

	fmt.Printf("Recorded snapshot message: %s -> %s: %v\n", src, dest, message)

}
