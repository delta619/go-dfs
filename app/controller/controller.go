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

type Chunk struct {
	ChunkName      string
	PrimaryNode    string
	SecondaryNodes string
}

var single_heartbeat_ping = 5

var (
	registrationMap = make(map[string]int)
	timestampMap    = make(map[string]time.Time)
	someMapMutex    = sync.RWMutex{}
	mutex           = &sync.Mutex{}
)

var automatic_deregestration_time = 7

// var timeout_for_heartbeat = 5

func removeInactiveNodesAutomatically() {
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

func handleRetrieveRequest(msgHandler *messages.MessageHandler, msg *messages.RetrieveRequest) {
	fmt.Println("\n", "LOG:", "Retreival request.")
	file_name := msg.GetFileName()

	chunks_list_names, err := getArrayValue(metadb, file_name)
	fmt.Printf("Chunks count of %s -> %d \n", file_name, len(chunks_list_names))

	chunk_nodes := make([]string, 0)
	for i, chunk_name := range chunks_list_names {
		fmt.Printf("finding route of chunk - %d\n", i)
		chunkMeta, err := getChunkMetaFromDB(chunk_name)
		check(err)

		primaryNode := chunkMeta["PrimaryNode"].(string)
		chunk_nodes = append(chunk_nodes, primaryNode)
	}
	fmt.Println("size 2 - ", len(chunk_nodes))

	if err != nil {
		panic(err)
	}

	// Send the primary node for each chunk to the client
	payload := messages.RetrieveResponse{
		ChunkNames: chunks_list_names,
		ChunkNodes: chunk_nodes,
		FileName:   file_name,
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_RetrieveResponse{RetrieveResponse: &payload},
	}
	msgHandler.Send(wrapper)
	fmt.Println("\n", "LOG:", "Retreival response sent")
	fmt.Println("\n", "LOG:", "Retreival response sent")
}

func handleChunkResponse(nodeMsgHandler *messages.MessageHandler, msg *messages.ChunkResponse) {
	storagePath := "./storage/"
	createDir(storagePath)

	chunkName := msg.GetChunkName()
	fmt.Println("Chunk get then save intiate received for", chunkName)

	chunkData := msg.GetChunkData()
	// write the chunk to the file
	err := ioutil.WriteFile(storagePath+chunkName, chunkData, 0644)
	check(err)
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
	fmt.Printf("Saved file meta - %s\n", chunkNamesStr)

	metadb.Put(key, value)
}

func handleChunkSaved(msgHandler *messages.MessageHandler, msg *messages.ChunkSaved) {

	chunkName := msg.GetChunkName()
	node := msg.GetNode()

	chunk := Chunk{
		ChunkName:      chunkName,
		PrimaryNode:    node,
		SecondaryNodes: "temp1,temp2",
	}
	ok := saveOrUpdateChunkTODB(chunkDB, chunk)
	if !ok {
		fmt.Println("Chunk save failed")
		return
	}
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
func getActiveHosts() ([]string, int) {
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
func register(hostname string) {
	someMapMutex.Lock()

	unique_node := hostname
	registrationMap[unique_node] = 1
	timestampMap[unique_node] = time.Now()
	fmt.Println("Registered host - " + unique_node)

	someMapMutex.Unlock()
}
func deregister(hostname string) {
	someMapMutex.Lock()

	registrationMap[hostname] = 0
	fmt.Println("Host deregistered - " + hostname)

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
	// go removeInactiveNodesAutomatically()

	go func() {
		for {
			// fmt.Println("waiting for request on orion01", listeningPortForNodes)
			if nodeConnection, err := listenerForNodes.Accept(); err == nil {
				nodMessageHandler := messages.NewMessageHandler(nodeConnection)
				// only handles one client at a time:
				go handleNodes(nodMessageHandler)
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
				nodMessageHandler := messages.NewMessageHandler(clientConnection)
				// only handles one client at a time:
				go handleClient(nodMessageHandler)
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
			register(msg.Register.GetHost())

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

func getChunkMetaFromDB(key string) (map[string]interface{}, error) {
	// Get the value from the database based on the key

	bytes, err := chunkDB.Get([]byte(key))
	check(err)
	// Decode the JSON object from bytes
	var value map[string]interface{}
	err = json.Unmarshal(bytes, &value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func saveOrUpdateChunkTODB(chunkDB *bitcask.Bitcask, new_chunk_meta Chunk) bool {
	chunkName := new_chunk_meta.ChunkName

	var upadted_chunk_metadata = make(map[string]interface{})

	upadted_chunk_metadata["PrimaryNode"] = new_chunk_meta.PrimaryNode
	updated_chunkMetaMarshalled, err := json.Marshal(upadted_chunk_metadata)
	check(err)
	fmt.Printf("What saved was %s - %s\n", chunkName, updated_chunkMetaMarshalled)
	chunkDB.Put([]byte(chunkName), []byte(updated_chunkMetaMarshalled))

	return true
}
