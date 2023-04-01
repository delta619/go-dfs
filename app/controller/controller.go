package main

import (
	"app/messages"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prologic/bitcask"
)

var metadb, _ = bitcask.Open("./meta")
var chunkDB, _ = bitcask.Open("./chunk")
var nodesMetaDB, _ = bitcask.Open("./node")

type Chunk struct {
	ChunkName      string
	PrimaryNode    string
	SecondaryNodes []string
}

var single_heartbeat_ping = 5

var (
	registrationMap = make(map[string]int)
	timestampMap    = make(map[string]time.Time)
	someMapMutex    = sync.RWMutex{}
	mutex           = &sync.Mutex{}
)

var automatic_deregestration_time = 7

var node_down_notification_channel = make(chan string, 1)

func removeInactiveNodesAutomatically() {

	go listenForReplicationOn()
	// in this technique of managing active/failed nodes, we simply check for every timestamp for registered hosts
	for {
		time.Sleep(7 * time.Second)
		for key, value := range timestampMap {
			if time.Since(value) > time.Duration(6*time.Second) && registrationMap[key] == 1 {
				deregister(key)
			}
		}
	}
}
func listenForReplicationOn() {
	chunks_list_for_replication := make(chan string, 100)
	go replicateChunks(chunks_list_for_replication)

	for {
		NODE_DOWN := <-node_down_notification_channel
		chunks, err := getArrayJsonValue(nodesMetaDB, NODE_DOWN) // get list of affected chunks from DB
		check(err)
		fmt.Println("\n", "LOG:", "Chunks affected. -> ", chunks)
		for _, chunkName := range chunks {
			chunks_list_for_replication <- strings.Join([]string{chunkName, NODE_DOWN}, ",")
		}
	}
}

func replicateChunks(chunks_list_for_replication chan string) {
	for {
		chunk_with_down_host := <-chunks_list_for_replication

		chunk_Name := strings.Split(chunk_with_down_host, ",")[0]
		down_node := strings.Split(chunk_with_down_host, ",")[1]

		chunk, err := getChunkMetaFromDB(chunk_Name)
		check(err)

		primary_node := chunk["PrimaryNode"].(string)
		secondary_nodes := chunk["SecondaryNodes"].([]string)

		excluding_nodes := make([]string, 0)

		if primary_node != down_node {
			excluding_nodes = append(excluding_nodes, primary_node)
		}

		for _, secondary_node := range secondary_nodes {
			if secondary_node != down_node {
				excluding_nodes = append(excluding_nodes, secondary_node)
			}
		}
		fmt.Println("\n", "LOG:", "Excluding nodes. -> ", excluding_nodes)
		assignOtherNodes(nodeHandlers[excluding_nodes[0]], chunk_Name, excluding_nodes) // tell any of the current node to pass the data to other node

	}

	// replicaSender
}

func handleRetrieveRequest(msgHandler *messages.MessageHandler, msg *messages.RetrieveRequest) {

	file_name := msg.GetFileName()
	fmt.Println("\n", "LOG:", "Retreival request of .", file_name)

	chunks_list_names, err := getArrayValue(metadb, file_name)
	// fmt.Printf("Chunks count of %s -> %d \n", file_name, len(chunks_list_names))

	chunk_nodes := make([]string, 0)

	chunks := []*messages.Chunk{}

	for _, chunk_name := range chunks_list_names {
		chunkMeta, err := getChunkMetaFromDB(chunk_name)
		check(err)
		// fmt.Printf("LOG:", "Chunk meta out is .%#v\n", chunkMeta)

		primaryNode := chunkMeta["PrimaryNode"].(string)
		chunk_nodes = append(chunk_nodes, primaryNode)

		chunk := messages.Chunk{
			ChunkName:      chunk_name,
			PrimaryNode:    primaryNode,
			SecondaryNodes: chunkMeta["SecondaryNodes"].([]string),
			FileName:       file_name,
		}
		chunks = append(chunks, &chunk)
	}

	chunkPtrs := make([]*messages.Chunk, len(chunks))

	for i, chunk := range chunks {
		chunkPtrs[i] = chunk
	}

	if err != nil {
		panic(err)
	}

	// Send the primary node for each chunk to the client
	payload := messages.RetrieveResponse{
		ChunkNames: chunks_list_names,
		ChunkNodes: chunk_nodes,
		FileName:   file_name,
		Chunks:     chunkPtrs,
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_RetrieveResponse{RetrieveResponse: &payload},
	}
	msgHandler.Send(wrapper)
	fmt.Println("\n", "LOG:", "Retreival response sent")
}

func handleUploadFileMetaRequest(msgHandler *messages.MessageHandler, msg *messages.UploadFileMetaRequest) {
	file_name := msg.GetFileName()
	chunkNames := msg.GetChunkNames()

	updateToMeta(file_name, chunkNames)
}

func updateToMeta(file_name string, newChunkNames []string) {
	chunkNamesStr := strings.Join(newChunkNames, ",")
	// fmt.Printf("-> %s\n", chunkNamesStr)

	key := []byte(file_name)
	value := []byte(chunkNamesStr)
	// fmt.Printf("Saved file meta - %s\n", chunkNamesStr)

	metadb.Put(key, value)
}

func handleChunkSaved(nodeHandler *messages.MessageHandler, msg *messages.ChunkSaved) {

	chunkName := msg.GetChunkName()
	primaryNode := msg.GetNode()

	// After getting confirmation, assign the secondary nodes and push the ChunkReplicaRoute to node

	assignOtherNodes(nodeHandler, chunkName, []string{primaryNode})

}

func assignOtherNodes(nodeHandler *messages.MessageHandler, chunkName string, existingNodes []string) {
	active_nodes, _ := getActiveNodes()

	if len(active_nodes) < 2 {
		fmt.Println("\n", "LOG:", "No active nodes for replication")
		return
	}

	new_nodes := make([]string, 0)

	for _, new_node := range active_nodes {

		if len(existingNodes)+len(new_nodes) == 3 { // #assign_total_nodes to maintain replication factor
			break
		}

		existing := false
		for _, existingNode := range existingNodes {
			if new_node == existingNode {
				existing = true
			}
		}
		if !existing {
			new_nodes = append(new_nodes, new_node)
		}
	}
	fmt.Println("\n", "LOG:", "new nodes. -> ", new_nodes)

	var new_primary_node string
	var new_secondary_nodes []string

	if len(new_nodes) == 2 { // First time upload case
		new_primary_node = existingNodes[0]
		new_secondary_nodes = new_nodes

	} else { // node failure case

		new_primary_node = new_nodes[0]
		new_secondary_nodes = existingNodes
	}

	chunk := Chunk{
		ChunkName:      chunkName,
		PrimaryNode:    new_primary_node,
		SecondaryNodes: new_secondary_nodes,
	}

	ok := saveOrUpdateChunkTODB(chunkDB, chunk)
	if !ok {
		fmt.Println("Chunk save failed")
		return
	}
	fmt.Println("\n", "LOG:", "chunk updated in chunkdb. -> ", chunk)

	updateNodeStatusToDB(chunk)

	fmt.Println("\n", "LOG:", "Node statuses updated. -> ")

	// send info to replicate chunk to other nodes
	payload := messages.ChunkReplicaRoute{
		ChunkName:  chunkName,
		OtherNodes: new_nodes,
	}

	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ChunkReplicaRoute{ChunkReplicaRoute: &payload},
	}
	nodeHandler.Send(wrapper)
	fmt.Println("\n", "LOG:", "Sent replication information of Chunk ", chunkName, "wrapper- ", wrapper)

}

func updateNodeStatusToDB(chunk Chunk) {
	// only save using the hostname of machine
	// host := strings.Split(chunk.PrimaryNode, ":")[0]
	// secondaryHost1 := strings.Split(chunk.SecondaryNodes[0], ":")[0]
	// secondaryHost2 := strings.Split(chunk.SecondaryNodes[1], ":")[1]

	primary_node := chunk.PrimaryNode
	secondary_node1 := chunk.SecondaryNodes[0]
	secondary_node2 := chunk.SecondaryNodes[1]

	addChunkToNode(nodesMetaDB, primary_node, chunk.ChunkName)
	addChunkToNode(nodesMetaDB, secondary_node1, chunk.ChunkName)
	addChunkToNode(nodesMetaDB, secondary_node2, chunk.ChunkName)
	// updated the meta

	// _, err := getArrayJsonValue(nodesMetaDB, primary_node)
	// check(err)

}

func addChunkToNode(db *bitcask.Bitcask, node string, chunkName string) error {
	// Get the current list of chunks stored for the node
	data, err := db.Get([]byte(node))
	if err != nil && err != bitcask.ErrKeyNotFound {
		return err
	}

	// Initialize a new list if the node was not found
	var chunks []string
	if err == bitcask.ErrKeyNotFound {
		chunks = []string{}
	} else {
		// Decode the current list from JSON
		err = json.Unmarshal(data, &chunks)
		if err != nil {
			return err
		}
	}

	// Check if chunkName is already in the list
	for _, name := range chunks {
		if name == chunkName {
			// Chunk already exists, no need to add it again
			return nil
		}
	}

	// Add the chunk name to the list
	chunks = append(chunks, chunkName)

	// Encode the updated list as JSON and store it in the key
	newData, err := json.Marshal(chunks)
	if err != nil {
		return err
	}
	err = db.Put([]byte(node), newData)
	if err != nil {
		return err
	}

	return nil
}

func handle_CHUNK_ROUTE_requests(msgHandler *messages.MessageHandler, msg *messages.ChunkRouteRequest) {
	// get active node
	assigned_node, err := get_route_node_for_chunk()
	// get port from active_host[0]

	success := true
	if err != 0 {
		success = false
	}
	// fmt.Println("\n", "LOG:", "ChunkRouteResponse res.", assigned_node, err, success)

	route_response_payload := messages.ChunkRouteResponse{
		Success:     success,
		ChunkName:   msg.GetChunkName(),
		Node:        assigned_node,
		CurrentPart: msg.GetCurrentPart(),
		TotalParts:  msg.GetTotalParts(),
		FileName:    msg.GetFileName(),
		ChunkSize:   msg.GetChunkSize(),
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ChunkRouteResponse{ChunkRouteResponse: &route_response_payload},
	}
	msgHandler.Send(wrapper)
}

/////////////////////

func check(err error) {
	if err != nil {
		fmt.Print("Error occurred: ")
		panic(err)
	}
}

func validateIncomingFile(fileName string, fileSize int) (bool, string) {
	if checkDiskSpace("./") < uint64(fileSize) {
		return false, "Not much space available on the memory"
	}
	if _, err := os.Stat("./server/" + fileName); err == nil {
		fmt.Println(fileName + " file already exists")
		return false, fileName + " file already exists"
	}
	return true, "error"
}

func validateOutgoingFile(fileName string) (bool, string) {
	msg := ""

	if _, err := os.Stat("./server/" + fileName); err == nil {
		return true, msg
	} else if os.IsNotExist(err) {
		fmt.Println("file does not exist on the server")
		msg = "file does not exist on the server"
	}

	return false, msg
}

func calculateChecksum(filename string) ([]byte, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("Error reading file: %v", err)
	}

	hash := sha256.Sum256(data)

	return hash[:], nil
}

func checkDiskSpace(path string) uint64 {
	var stat syscall.Statfs_t
	err := syscall.Statfs(path, &stat)
	if err != nil {
		fmt.Printf("Error checking disk space: %v\n", err)
		return 9999999999999
	}

	avail := stat.Bavail * uint64(stat.Bsize)
	return avail
}

func createDir(storagePath string) error {
	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		err = os.Mkdir(storagePath, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}

func get_route_node_for_chunk() (string, int) {

	for node, value := range registrationMap {

		if value == 1 {
			return node, 0
		}
	}
	return "No active nodes", 1
}
func fillEmptyKeys(metadataPath string) error {
	// Check if the file is empty
	fileInfo, err := os.Stat(metadataPath)
	if os.IsNotExist(err) {
		// Create an empty file if it doesn't exist
		_, err := os.Create(metadataPath)
		if err != nil {
			return err
		}
		// Get file info again after creating the file
		fileInfo, err = os.Stat(metadataPath)
	}

	if fileInfo.Size() == 0 {
		// Initialize the metadata with default values
		metadata := make(map[string]interface{})
		metadata["chunks"] = make(map[string]Chunk)
		metadata["files"] = make(map[string][]string)
		// Serialize the metadata to JSON
		metadataBytes, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		// Write the JSON data to the file
		err = ioutil.WriteFile(metadataPath, metadataBytes, 0644)
		if err != nil {
			return err
		}
	}
	return nil
}

// ReadFileChunks reads the contents of a JSON file and returns a FileChunks struct
func Readfile_chunks_list(file_name string) ([]string, error) {

	file_chunks_list, err := getArrayValue(metadb, file_name)

	if err != nil {
		return nil, err
	}

	return file_chunks_list, nil
}

//////////////////////// LISTING ///////////////////////////////

func handleListRequest(msgHandler *messages.MessageHandler) {
	filesList := make([]string, 0)

	keys := metadb.Keys()

	for key := range keys {
		filesList = append(filesList, string(key))
	}
	statusList := make([]int64, len(filesList))
	for i := range statusList {
		statusList[i] = int64(0)
	}

	payload := messages.ListResponse{
		FileNames:  filesList,
		StatusList: statusList,
	}

	wrapper := messages.Wrapper{
		Msg: &messages.Wrapper_ListResponse{ListResponse: &payload},
	}

	msgHandler.Send(&wrapper)
	fmt.Println("\n", "LOG:", "Sent Ls information of client ", filesList, " ", statusList)

}

//////////////////////// UTILS /////////////////////////////////

func validateHeartbeat(msgHandler *messages.MessageHandler, host string, beat bool) {
	// if we send an isIssuccess-false then client seends to send a Registration request
	isRegistered := registrationMap[host]
	isSuccess := true
	message := "#"

	if beat == false {
		isSuccess = false
		// for handling node side interrupt
		deregister(host)
		fmt.Println("Remote host - " + host + " got terminated unexpectedly.")
		msgHandler.Close()
		return
	}

	if isRegistered == 0 {
		isSuccess = false
		message = "Host not registered yet."
		// fmt.Println("Host - " + host + " not registered yet.")
	}

	// we can use optionally the below technique in which the delay of heartbeat is cheked only after any new heartbeat from that host arrives.
	someMapMutex.Lock()

	if time.Since(timestampMap[host]) > time.Duration(25*time.Second) && registrationMap[host] == 1 {
		isSuccess = false
		message = "Timestamp delay exceeded. Please register again the next time."
		deregister(host)
	}
	someMapMutex.Unlock()

	if isSuccess {
		// fmt.Println("Heartbeart updated successfully. " + host)
		updateTimeStamp(host)
	}

	// use message temporary
	_ = message
	// create a response payload
	payload := messages.Heartbeat{Host: host, Beat: isSuccess}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_Heartbeat{Heartbeat: &payload},
	}
	msgHandler.Send(wrapper)
}
func getActiveNodes() ([]string, int) {
	// someMapMutex.Lock()
	// defer someMapMutex.Unlock()
	var activeHosts []string
	for host, status := range registrationMap {
		if status == 1 {
			activeHosts = append(activeHosts, host)
		}
	}

	count := int(0)

	if len(activeHosts) < 0 {
		count = 0
	}
	return activeHosts, count
}
func updateTimeStamp(host string) {
	someMapMutex.Lock()

	timestampMap[host] = time.Now()
	// fmt.Println("Ping received from host - " + host + " - Updated timestamp")

	someMapMutex.Unlock()
}

var nodeHandlers = make(map[string]*messages.MessageHandler)

func register(hostname string, msg_handler *messages.MessageHandler) {
	someMapMutex.Lock()

	unique_node := hostname
	registrationMap[unique_node] = 1
	timestampMap[unique_node] = time.Now()
	fmt.Println("Registered host - " + unique_node)
	nodeHandlers[unique_node] = msg_handler
	someMapMutex.Unlock()
}
func deregister(hostname string) {
	someMapMutex.Lock()

	registrationMap[hostname] = 0
	fmt.Println("Host deregistered - Replication starting." + hostname)
	node_down_notification_channel <- hostname
	someMapMutex.Unlock()
}

// ////////////// Node connector /////////////////////////
var nodeConnections = make(map[string]net.Conn)

func createNodeConnection(host string, port string) (net.Conn, error) {
	addr := fmt.Sprintf("%s:%s", host, port)
	conn, ok := nodeConnections[addr]
	if ok {
		// connection already exists, return it
		return conn, nil
	}

	// connection doesn't exist, create a new one
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	someMapMutex.Lock()

	nodeConnections[addr] = conn

	someMapMutex.Unlock()

	return conn, nil
}

func main() {
	// test()

	listeningPortForNodes := os.Args[1]
	listenerForNodes, err := net.Listen("tcp", ":"+listeningPortForNodes)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	fmt.Println("Listening for Nodes ... on", listeningPortForNodes)
	go removeInactiveNodesAutomatically()

	go func() {
		for {
			// fmt.Println("waiting for request on orion01", listeningPortForNodes)
			if nodeConnection, err := listenerForNodes.Accept(); err == nil {
				dataServiceHandler := messages.NewMessageHandler(nodeConnection)
				// only handles one client at a time:
				go handleNodes(dataServiceHandler)
			}
		}
	}()
	// ------------------------------------------------------------------------ //
	listeningPortForClient := "21999"
	listenerForClient, err := net.Listen("tcp", ":"+listeningPortForClient)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	fmt.Println("Listening for Client ... on", listeningPortForClient)

	go func() {
		for {
			// fmt.Println("waiting for request on orion01", listeningPortForClient)
			if clientConnection, err := listenerForClient.Accept(); err == nil {
				dataServiceHandler := messages.NewMessageHandler(clientConnection)
				// only handles one client at a time:
				go handleClient(dataServiceHandler)
			}
		}
	}()

	select {}
}

func handleClient(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()
	for {
		wrapper, err := msgHandler.Receive()
		// fmt.Println("got the client")
		if err != nil {
			fmt.Println("%v\n", err)
			fmt.Println("%x\n", wrapper)

		}
		check(err)

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_RetrieveRequest: /*client*/
			handleRetrieveRequest(msgHandler, msg.RetrieveRequest)
		case *messages.Wrapper_UploadFileMetaRequest: /*client*/
			handleUploadFileMetaRequest(msgHandler, msg.UploadFileMetaRequest)
		case *messages.Wrapper_ChunkRouteRequest: /*client*/
			handle_CHUNK_ROUTE_requests(msgHandler, msg.ChunkRouteRequest)
		case *messages.Wrapper_StoreRequest: /*client*/
			payload := messages.StoreResponse{Success: true, Message: "You you can store the file"}
			wrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_StoreResponse{StoreResponse: &payload},
			}
			msgHandler.Send(wrapper)
		case *messages.Wrapper_ListRequest:
			handleListRequest(msgHandler)
		default:
			fmt.Println("Client connection closing")

			return
		}

	}
}

func handleNodes(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()

	for {
		wrapper, err := msgHandler.Receive()

		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		check(err)
		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_ChunkSaved: /*node*/
			go handleChunkSaved(msgHandler, msg.ChunkSaved)
		case *messages.Wrapper_Heartbeat: /*node*/
			validateHeartbeat(msgHandler, msg.Heartbeat.GetHost(), msg.Heartbeat.GetBeat())
		case *messages.Wrapper_Register: /*node*/
			register(msg.Register.GetHost(), msgHandler)
		default:
			fmt.Println("Some node closed")
			return

		}

	}
}

////////------------ DB OPERATIONS ------------------------------\\\\\\\\\\\\\

func test() {
}

func getArrayValue(b *bitcask.Bitcask, key string) ([]string, error) {
	val, err := metadb.Get([]byte(key))
	res, err := strings.Split(string(val), ","), nil

	if err != nil {
		return nil, err
	}

	return res, nil
}

func getArrayJsonValue(nodeMetaDB *bitcask.Bitcask, key string) ([]string, error) {
	val, err := nodeMetaDB.Get([]byte(key))
	check(err)
	var valUnMarshalled = make([]string, 0)
	json.Unmarshal(val, &valUnMarshalled)

	return valUnMarshalled, nil
}

func getChunkMetaFromDB(key string) (map[string]interface{}, error) {
	// Get the value from the database based on the key

	bytes, err := chunkDB.Get([]byte(key))
	check(err)
	// Decode the JSON object from bytes
	var chunk_obj map[string]interface{}
	err = json.Unmarshal(bytes, &chunk_obj)
	if err != nil {
		return nil, err
	}
	if chunk_obj["SecondaryNodes"] != nil {
		secondary_nodes, ok := chunk_obj["SecondaryNodes"].(string)
		if !ok {
			return nil, fmt.Errorf("SecondaryNodes is not a string")
		}
		nodes := strings.Split(secondary_nodes, ",")
		chunk_obj["SecondaryNodes"] = nodes
	}

	return chunk_obj, nil
}

func saveOrUpdateChunkTODB(chunkDB *bitcask.Bitcask, new_chunk_meta Chunk) bool {
	chunkName := new_chunk_meta.ChunkName

	var updated_chunk_metadata = make(map[string]interface{})

	updated_chunk_metadata["PrimaryNode"] = new_chunk_meta.PrimaryNode
	if new_chunk_meta.SecondaryNodes != nil {
		updated_chunk_metadata["SecondaryNodes"] = strings.Join(new_chunk_meta.SecondaryNodes, ",")
	}

	updated_chunkMetaMarshalled, err := json.Marshal(updated_chunk_metadata)

	check(err)
	// fmt.Printf("What saved was %s - %s\n", chunkName, updated_chunkMetaMarshalled)
	chunkDB.Put([]byte(chunkName), []byte(updated_chunkMetaMarshalled))

	return true
}
