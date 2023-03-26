package main

import (
	"app/messages"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Chunk struct {
	PrimaryNode    string   `json:"primaryNode"`
	SecondaryNodes []string `json:"secondaryNodes"`
}

type FileMeta map[string][]string

type FileChunks struct {
	Files  map[string][]string `json:"files"`
	Chunks map[string]Chunk    `json:"chunks"`
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

func handleRequests(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()

	for {
		wrapper, err := msgHandler.Receive()
		check(err)

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_RetrieveRequest: /*client*/
			// fmt.Println("RetrieveRequest received from Client")
			handleRetrieveRequest(msgHandler, msg.RetrieveRequest)
		case *messages.Wrapper_UploadFileMetaRequest: /*client*/
			// fmt.Println("UploadFileMetaRequest received from Client")
			handleUploadFileMetaRequest(msgHandler, msg.UploadFileMetaRequest)
		case *messages.Wrapper_ChunkSaved: /*node*/
			handleChunkSaved(msgHandler, msg.ChunkSaved)
		case *messages.Wrapper_ChunkRouteRequest: /*client*/
			// fmt.Println("ChunkRouteRequest received from Client")
			handle_CHUNK_ROUTE_requests(msgHandler, msg.ChunkRouteRequest)
		case *messages.Wrapper_StoreRequest: /*client*/
			// fmt.Println("Client wants to Store", msg.StoreRequest.GetFileName())
			payload := messages.StoreResponse{Success: true, Message: "You you can store the file"}
			wrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_StoreResponse{StoreResponse: &payload},
			}
			msgHandler.Send(wrapper)
		case *messages.Wrapper_Heartbeat: /*node*/
			// fmt.Println("Heartbeat received from Node")
			// handleNodeRequests(wrapper)
			validateHeartbeat(msgHandler, msg.Heartbeat.GetHost(), msg.Heartbeat.GetBeat())
		case *messages.Wrapper_Register: /*node*/
			fmt.Println("Register received from Node")

			register(msg.Register.GetHost())

		}

	}
}

func handleRetrieveRequest(msgHandler *messages.MessageHandler, msg *messages.RetrieveRequest) {
	file_name := msg.GetFileName()

	fileChunks, err := ReadFileChunks("metadata.json")
	check(err)
	chunk_names := fileChunks.Files[file_name]
	node_names := make([]string, 0)

	for _, chunkName := range chunk_names {

		// append chunk name
		node_names = append(node_names, fileChunks.Chunks[chunkName].PrimaryNode)
	}

	// Send the primary node for each chunk to the client
	payload := messages.RetrieveResponse{
		ChunkNames: chunk_names,
		ChunkNodes: node_names,
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_RetrieveResponse{RetrieveResponse: &payload},
	}
	msgHandler.Send(wrapper)
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

	err := fillEmptyKeys("metadata.json") // validate metadata.json file
	check(err)

	err = addMetaFileToJSONFileMutex("metadata.json", file_name, chunkNames)
	check(err)
}

func handleChunkSaved(msgHandler *messages.MessageHandler, msg *messages.ChunkSaved) {

	metadataPath := "./metadata.json"

	chunkName := msg.GetChunkName()
	node := msg.GetNode()

	chunk := Chunk{
		PrimaryNode:    node,
		SecondaryNodes: []string{"temp1", "temp2"},
	}

	err := fillEmptyKeys(metadataPath) // validate metadata.json file
	check(err)

	err = addChunkToJSONFileMutex(metadataPath, chunkName, chunk)
	check(err)
}
func handle_CHUNK_ROUTE_requests(msgHandler *messages.MessageHandler, msg *messages.ChunkRouteRequest) {
	// get active node
	active_hosts := getActiveHosts()
	// get port from active_host[0]
	host := active_hosts[0]
	port := strings.Split(host, ":")[1]
	_ = port
	route_response_payload := messages.ChunkRouteResponse{
		Success:   true,
		ChunkName: msg.GetChunkName(),
		Host:      host,
		Port:      "21001", // service port of nodes predefined
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ChunkRouteResponse{ChunkRouteResponse: &route_response_payload},
	}

	msgHandler.Send(wrapper)
}

func addMetaFileToJSONFile(jsonFilePath string, fileName string, chunkNames []string) error {
	// Read existing JSON data from the file

	jsonData, err := ioutil.ReadFile(jsonFilePath)

	if err != nil {
		return err
	}

	// Parse the existing JSON data into a FileChunks struct
	var fileChunks FileChunks
	err = json.Unmarshal(jsonData, &fileChunks)
	if err != nil {
		return err
	}

	// Add the new file and its associated chunk names to the Files map in the FileChunks struct
	fileChunks.Files[fileName] = chunkNames

	// Serialize the updated FileChunks struct to JSON
	updatedJSON, err := json.Marshal(fileChunks)
	if err != nil {
		return err
	}

	// Write the updated JSON data to the file

	err = ioutil.WriteFile(jsonFilePath, updatedJSON, 0644)

	if err != nil {
		return err
	}

	return nil
}
func addChunkToJSONFile(jsonFilePath string, chunkName string, chunk Chunk) error {
	// Read existing JSON data from the file
	jsonData, err := ioutil.ReadFile(jsonFilePath)
	if err != nil {
		return err
	}

	// Parse the existing JSON data into a FileChunks struct
	var fileChunks FileChunks
	err = json.Unmarshal(jsonData, &fileChunks)
	if err != nil {
		return err
	}

	// Add the new chunk to the Chunks map in the FileChunks struct
	fileChunks.Chunks[chunkName] = chunk

	// Serialize the updated FileChunks struct to JSON
	updatedJSON, err := json.Marshal(fileChunks)
	if err != nil {
		return err
	}

	// Write the updated JSON data to the file
	err = ioutil.WriteFile(jsonFilePath, updatedJSON, 0644)
	if err != nil {
		return err
	}

	return nil
}

func addMetaFileToJSONFileMutex(jsonFilePath string, fileName string, chunkNames []string) error {
	mutex.Lock()
	defer mutex.Unlock()

	// call the original function
	return addMetaFileToJSONFile(jsonFilePath, fileName, chunkNames)
}
func addChunkToJSONFileMutex(jsonFilePath string, chunkName string, chunk Chunk) error {
	mutex.Lock()
	defer mutex.Unlock()

	// call the original function
	return addChunkToJSONFile(jsonFilePath, chunkName, chunk)
}

/////////////////////

func check(err error) {
	if err != nil {
		fmt.Println("Error occurred")
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

func create_random_node_ids() []string {

	rand.Seed(time.Now().UnixNano())

	nums := make(map[int]bool)
	result := make([]string, 0)

	for len(nums) < 3 {
		n := rand.Intn(10)
		if n < 10 && !nums[n] {
			nums[n] = true
			result = append(result, strconv.Itoa(n))
		}
	}
	return result
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
func ReadFileChunks(filePath string) (*FileChunks, error) {
	fillEmptyKeys("./metadata.json")
	jsonData, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var fileChunks FileChunks
	err = json.Unmarshal(jsonData, &fileChunks)
	if err != nil {
		return nil, err
	}

	return &fileChunks, nil
}

// UpdateFileChunks updates a FileChunks struct and writes it to a JSON file
func UpdateFileChunks(filePath string, fileChunks *FileChunks) error {
	jsonData, err := json.MarshalIndent(fileChunks, "", "    ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(filePath, jsonData, 0644)
	if err != nil {
		return err
	}

	return nil
}

func modifyJSON() {
	// Read the file chunks from the JSON file
	fileChunks, err := ReadFileChunks("metadata.json")
	if err != nil {
		// Handle error
	}

	// Modify the file chunks as desired
	fileChunks.Files["example1.txt"] = []string{"chunk1", "chunk2", "chunk3", "chunk4"}

	// Save the updated file chunks to the JSON file
	err = UpdateFileChunks("metadata.json", fileChunks)
	if err != nil {
		// Handle error
	}

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
		return
	}

	if isRegistered == 0 {
		isSuccess = false
		message = "Host not registered yet."
		fmt.Println("Host - " + host + " not registered yet.")
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
func getActiveHosts() []string {
	someMapMutex.Lock()
	defer someMapMutex.Unlock()
	var activeHosts []string
	for host, status := range registrationMap {
		if status == 1 {
			activeHosts = append(activeHosts, host)
		}
	}
	return activeHosts
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
	port := os.Args[1]
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	go removeInactiveNodesAutomatically()

	for {
		fmt.Println("waiting for request on orion02", port)
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			// only handles one client at a time:
			go handleRequests(msgHandler)
		}
	}
}
