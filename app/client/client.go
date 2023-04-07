package main

import (
	"app/messages"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"runtime/debug"
	"sync"
	"time"
)

var TEST_FILES_DIRECTORY = "/bigdata/mmalensek/project1/"

// var TEST_FILES_DIRECTORY = "./"

// var TEST_FILES_DIRECTORY = "/bigdata/students/amalla2/ASHU_TEST/"
var SANDBOX = "/bigdata/students/amalla2/SANDBOX/"

var OUTPUT = "/bigdata/students/amalla2/OUTPUT/"

var GO_startChunkWriter = 1
var GO_getChunkFromNode = 1
var GO_get_the_routes = 1
var GO_push_to_node = 1

// var OUTPUT = "./out/"
var CHUNK_IN_MB = 7

var UPLOADED_FILES = 0
var DOWNLOADED_CHUNKS = 0
var TOTAL_CHUNKS = int64(0)

var CHUNK_SIZE = int64(CHUNK_IN_MB * 1024 * 1024) // Depends on CHUNK_IN_MB

func clean() {
	// Reset global variables
	UPLOADED_FILES = 0
	DOWNLOADED_CHUNKS = 0
	TOTAL_CHUNKS = 0

	// Clear node handlers map
	nodeHandlersMutex.Lock()
	defer nodeHandlersMutex.Unlock()
	for addr := range nodeHandlers {
		nodeHandler := nodeHandlers[addr]
		nodeHandler.Close()
		delete(nodeHandlers, addr)
	}
}

func check(err error) {
	if err != nil {
		fmt.Println("Error occurred:", err)
		debug.PrintStack()

		os.Exit(1)
		return
	}
}

var controllerHandler *messages.MessageHandler

var nodeHandlers = make(map[string]*messages.MessageHandler)

var nodeHandlersMutex sync.Mutex

func getNodeHandler(addr string) (*messages.MessageHandler, bool) {
	nodeHandlersMutex.Lock()
	defer nodeHandlersMutex.Unlock()
	nodeHandler, ok := nodeHandlers[addr]
	return nodeHandler, ok
}

func setNodeHandler(addr string, handler *messages.MessageHandler) {
	nodeHandlersMutex.Lock()
	defer nodeHandlersMutex.Unlock()
	nodeHandlers[addr] = handler
}

func createNodeHandler(node string) (*messages.MessageHandler, bool, error) {
	addr := fmt.Sprintf("%s", node)
	nodeHandler, ok := getNodeHandler(addr)
	if ok {
		// node handler already exists, return it
		return nodeHandler, true, nil
	}

	// node handler doesn't exist, create a new one
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, false, err
	}
	nodeHandler = messages.NewMessageHandler(conn)
	setNodeHandler(addr, nodeHandler)

	return nodeHandler, false, nil
}

func getSize(FileName string) int64 {
	fileInfo, err := os.Stat(FileName)
	if err != nil {
		fmt.Println("Error getting file info:", err)
		return 0
	}
	// fmt.Printf("Size of file %s is %d bytes\n", FileName, fileInfo.Size())
	return fileInfo.Size()
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type Chunk_for_channel struct {
	ChunkName      string
	FileName       string
	ChunkData      []byte
	currentPart    int64
	TotalParts     int64
	ChunkSize      int64
	PrimaryNode    string
	SecondaryNodes []string
	UseSecondary   int // if 0 then primary, otherwise the index of secondary nodes
}

var chunkPushChannel_Chunk = make(chan Chunk_for_channel, 3)
var ready_to_push_to_node = make(chan Chunk_for_channel, 1)
var chunk_retreive_channel = make(chan Chunk_for_channel, 100)
var chunk_waiting_to_be_saved = make(chan Chunk_for_channel, 100)
var some_chunk_got_saved_channel = make(chan Chunk_for_channel, 100)
var all_chunks_received_notification_channel = make(chan bool, 100)

var chunk_reading_rate_limiter_channel = make(chan bool, 1)
var all_chunks_uploaded_notification_channel = make(chan int, 999)
var all_chunks_downloaded_notification_channel = make(chan bool, 999)

// notifications of actions
var put_action_completed_notification = make(chan bool, 999)
var get_action_completed_notification = make(chan bool, 999)

var file_constructed_notification = make(chan bool, 999)

var wg sync.WaitGroup

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func createRoutedChunksForUpload(file_name string) {

	fullFilePath := TEST_FILES_DIRECTORY + file_name

	fileSize := getSize(fullFilePath)
	fmt.Println("\n", "LOG:", "File size.", fileSize)

	var numChunks = int64(0)
	if fileSize < CHUNK_SIZE {
		numChunks = int64(1)
	} else {
		numChunks = int64(math.Ceil(float64(fileSize) / float64(CHUNK_SIZE)))

	}
	TOTAL_CHUNKS = numChunks
	fmt.Println("\n", "LOG:", "Num of chunks.", numChunks)

	// pseudo chunking
	fileChunks := make([]string, numChunks)

	for i := 0; i < GO_get_the_routes; i++ {
		go get_the_routes()

	}
	for i := 0; i < GO_push_to_node; i++ {
		go push_to_node()

	}

	READING_COUNTER := 0

	sem := make(chan struct{}, 1)
	go func() {

		for i := int64(0); i < numChunks; i++ {
			sem <- struct{}{} // acquire semaphore
			go func(part int64) {
				// fmt.Println("\n", "LOG:", "Reading.", part, TOTAL_CHUNKS)
				chunk_reading_rate_limiter_channel <- true
				chunkData := readChunkData(file_name, part, CHUNK_SIZE)
				READING_COUNTER += 1
				chunkMD5 := fmt.Sprintf("%x", md5.Sum(chunkData))
				fileChunks[part] = chunkMD5
				chunk_for_channel := Chunk_for_channel{
					ChunkName:   chunkMD5,
					currentPart: part,
					TotalParts:  numChunks,
					ChunkSize:   CHUNK_SIZE,
					FileName:    file_name,
				}
				<-chunk_reading_rate_limiter_channel
				chunkPushChannel_Chunk <- chunk_for_channel
				<-sem // release semaphore
			}(i)
		}
	}()

	calculated := <-all_chunks_uploaded_notification_channel
	fmt.Println("All chunks uploaded, ", calculated)

	wg.Wait()

	payload := messages.UploadFileMetaRequest{
		FileName:   file_name,
		ChunkNames: fileChunks,
	}
	wrapper := messages.Wrapper{
		Msg: &messages.Wrapper_UploadFileMetaRequest{UploadFileMetaRequest: &payload},
	}
	controllerHandler.Send(&wrapper)
	fmt.Println("\n", "LOG:", "Meta sent", numChunks)

	put_action_completed_notification <- true
}

func get_the_routes() {
	for {

		chunk := <-chunkPushChannel_Chunk
		// time.Sleep(100 * time.Millisecond)
		chunk_payload := messages.ChunkRouteRequest{
			ChunkName:   chunk.ChunkName,
			CurrentPart: chunk.currentPart,
			TotalParts:  chunk.TotalParts,
			ChunkSize:   chunk.ChunkSize,
			FileName:    chunk.FileName,
		}

		wrapper := messages.Wrapper{
			Msg: &messages.Wrapper_ChunkRouteRequest{ChunkRouteRequest: &chunk_payload},
		}
		controllerHandler.Send(&wrapper)
	}

}
func push_to_node() {
	for {
		chunk := <-ready_to_push_to_node
		// fmt.Println("\n", "LOG:", "Connecting to node", chunk.PrimaryNode, chunk.ChunkName)

		nodeHandler, _, err := createNodeHandler(chunk.PrimaryNode)
		check(err)

		chunk_with_data := readChunkData(chunk.FileName, chunk.currentPart, chunk.ChunkSize)
		chunk_payload := messages.UploadChunkRequest{
			ChunkName: chunk.ChunkName,
			FileName:  chunk.FileName,
			ChunkData: chunk_with_data,
		}

		wrapper := messages.Wrapper{
			Msg: &messages.Wrapper_UploadChunkRequest{UploadChunkRequest: &chunk_payload},
		}
		nodeHandler.Send(&wrapper)

		UPLOADED_FILES += 1
		fmt.Printf("⬆ : %d/%d %s \n", UPLOADED_FILES, TOTAL_CHUNKS, chunk.ChunkName)
		check(err)
		if UPLOADED_FILES >= int(chunk.TotalParts) {
			all_chunks_uploaded_notification_channel <- int(TOTAL_CHUNKS)
		}
		// Close the connection after use to avoid resource leakage
	}
}

func readChunkData(FileName string, currentPart int64, ChunkSize int64) []byte {
	startByte := currentPart * ChunkSize
	fullPath := TEST_FILES_DIRECTORY + FileName
	file, err := os.Open(fullPath)

	fileSize := getSize(fullPath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		panic(err)
	}
	defer file.Close()

	endByte := startByte + int64(ChunkSize) // End of the chunk
	if endByte > fileSize {
		endByte = fileSize
	}

	chunk := make([]byte, endByte-startByte)

	_, err = file.ReadAt(chunk, startByte)

	if err != nil && err != io.EOF {
		fmt.Printf("Error reading chunk from file: %v\n", err)
		panic(err)
	}
	return chunk
}

func putAction(file_name string) {

	payload := messages.StoreRequest{
		FileName: file_name, FileSize: 1,
	}
	wrapper := messages.Wrapper{
		Msg: &messages.Wrapper_StoreRequest{StoreRequest: &payload},
	}
	controllerHandler.Send(&wrapper)

}

func getAction(file_name string) {

	// make a store request
	payload := messages.RetrieveRequest{
		FileName: file_name,
	}
	wrapper := messages.Wrapper{
		Msg: &messages.Wrapper_RetrieveRequest{RetrieveRequest: &payload},
	}
	// Store request sent to controller
	controllerHandler.Send(&wrapper)

}

var retrieveChunkManager = make(map[string]Chunk_for_channel)

func retrieveAllChunks(chunks []*messages.Chunk) {
	// Get the file name and initialize arrays to store chunk names and primary nodes
	file_name := chunks[0].GetFileName()

	chunk_names := make([]string, 0)
	chunksPrimaryNodes := make([]string, 0)
	chunksSecondaryNodes := make([][]string, 0)

	// Loop over all chunks and append their names and primary nodes to the corresponding arrays
	for _, chunk := range chunks {
		chunk_names = append(chunk_names, chunk.GetChunkName())
		chunksPrimaryNodes = append(chunksPrimaryNodes, chunk.GetPrimaryNode())
		chunksSecondaryNodes = append(chunksSecondaryNodes, chunk.GetSecondaryNodes())
	}

	fmt.Println(chunk_names)
	fmt.Println("---------------")
	fmt.Println(chunksPrimaryNodes)

	TOTAL_CHUNKS = int64(len(chunks))

	go waitForAllChunks(file_name, chunk_names)
	// meanwhile
	go getChunkFromNode()
	// meanwhile
	go startChunkWriter()
	// meanwhile
	go constructFileWhenReady(file_name, chunk_names)
	// meanwhile
	for i, _ := range chunk_names {

		chunk := Chunk_for_channel{
			ChunkName:      chunk_names[i],
			PrimaryNode:    chunksPrimaryNodes[i],
			SecondaryNodes: chunksSecondaryNodes[i],
			UseSecondary:   0,
		}

		retrieveChunkManager[chunk_names[i]] = chunk // store metadata of each chunk
		chunk_retreive_channel <- chunk              // push each chunk request in the channel
	}

}

func waitForAllChunks(file_name string, ChunkNames []string) {

	for {

		// fmt.Println("\n", "Blocked at:", "file_constructed_notification.")
		<-file_constructed_notification

		// fmt.Println("\n", "Blocked at:", "get_action_completed_notification.")
		get_action_completed_notification <- true

	}
}

func getChunkFromNode() {
	for {
		chunk := <-chunk_retreive_channel
		// fmt.Println("\n", "LOG:", "Asking node for the chunk", chunk.ChunkName)

		selectedNode := chunk.PrimaryNode

		if chunk.UseSecondary > 0 {
			selectedNode = chunk.SecondaryNodes[chunk.UseSecondary-1]
		}
		fmt.Printf("LOG: Asking %s from %s \n", chunk.ChunkName, selectedNode)

		nodeHandler, isExisting, err := createNodeHandler(selectedNode)
		if err != nil {
			fmt.Println("\n", "LOG:", "Node down !, Cant Get Chunk", chunk.ChunkName, chunk.PrimaryNode)
			go func() {

				if chunk.UseSecondary == 0 {

					chunk.UseSecondary = 1
					fmt.Println("\n", "LOG:", "Node down !, Trying Secondary node1-", chunk.UseSecondary, chunk.ChunkName, chunk.SecondaryNodes)

					chunk_retreive_channel <- chunk

				} else if chunk.UseSecondary != len(chunk.SecondaryNodes) {
					chunk.UseSecondary += 1
					fmt.Println("\n", "LOG:", "Node down !, Trying Secondary node2-", chunk.UseSecondary, chunk.ChunkName, chunk.PrimaryNode)

					chunk_retreive_channel <- chunk

				} else {
					fmt.Println("\n", "LOG:", "Chunk irretrievable, FAILED", chunk.ChunkName, chunk.PrimaryNode, chunk.SecondaryNodes, chunk.UseSecondary)
					// Show some Error and come out
					os.Exit(0)
				}
			}()
			continue
		}

		payload := messages.ChunkRequest{
			ChunkName: chunk.ChunkName,
		}

		wrapper := messages.Wrapper{
			Msg: &messages.Wrapper_ChunkRequest{ChunkRequest: &payload},
		}

		nodeHandler.Send(&wrapper)

		if !isExisting {
			// fmt.Println("\n", "LOG:", "Go wait", chunk.ChunkName)
			go waitForChunkFromNode(nodeHandler)
		}
	}
}

func constructFileWhenReady(file_name string, ChunkNames []string) {
	for {
		// Create a new file with the given name
		// fmt.Println("\n", "Blocked at:", "all_chunks_downloaded_notification_channel.")
		<-all_chunks_downloaded_notification_channel

		fmt.Printf("⏳ Constructing %s\n", file_name)
		fullOutputPath := OUTPUT + file_name
		f, err := os.Create(fullOutputPath)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		// Append each chunk to the file
		for _, ChunkName := range ChunkNames {
			chunkPath := SANDBOX + ChunkName
			chunkFile, err := os.Open(chunkPath)
			if err != nil {
				panic(err)
			}
			defer chunkFile.Close()

			// Copy the chunk data to the new file
			_, err = io.Copy(f, chunkFile)
			if err != nil {
				panic(err)
			}
		}
		fmt.Printf("✅ %s constructed \n", file_name)

		// calc md5
		file, err := os.Open(fullOutputPath)
		if err != nil {
			fmt.Print(err)
		}
		defer file.Close()
		hash := md5.New()
		if _, err := io.Copy(hash, file); err != nil {
			log.Fatal(err)
		}

		hashBytes := hash.Sum(nil)

		// Print the hash as a hex string
		fmt.Printf("\nMD5 hash: %x\n", hashBytes)

		file_constructed_notification <- true
	}

}

// sends a request to a nodeHandler asking for a specific chunk

func waitForChunkFromNode(nodeHandler *messages.MessageHandler) {
	// fmt.Println("\n", "LOG:", "Listening from node", nodeHandler)

	for {
		wrapper, err := nodeHandler.Receive()
		check(err)
		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_ChunkResponse:
			go func() {

				chunk := Chunk_for_channel{
					ChunkName: msg.ChunkResponse.GetChunkName(),
					ChunkData: msg.ChunkResponse.GetChunkData(),
				}
				// fmt.Println("\n", "LOG:", "Receiving chunk", chunk.ChunkName)
				chunk_waiting_to_be_saved <- chunk
			}()
		}
	}
}

func startChunkWriter() {
	for {

		// fmt.Println("\n", "LOG:", "Debug A ")
		// fmt.Println("\n", "Blocked at:", "chunk_waiting_to_be_saved.")
		chunkMeta := <-chunk_waiting_to_be_saved
		// fmt.Println("\n", "LOG:", "Writing chunk.", chunkMeta.ChunkName)
		// fmt.Println("\n", "LOG:", "Debug B ")

		chunkData := chunkMeta.ChunkData

		chunkPath := SANDBOX + chunkMeta.ChunkName

		err := os.MkdirAll(SANDBOX, os.ModePerm)
		if err != nil {
			panic(err)
		}

		err = os.WriteFile(chunkPath, chunkData, 0644)
		if err != nil {
			panic(err)
		}

		chunkMD5 := fmt.Sprintf("%x", md5.Sum(chunkData))

		if chunkMD5 != chunkMeta.ChunkName {
			fmt.Printf("WARNING: %s MD5 verify failed, trying other nodes\n", chunkMeta.ChunkName)
			chunk := retrieveChunkManager[chunkMeta.ChunkName]
			chunk.UseSecondary++
			chunk_retreive_channel <- chunk

			continue // reinserted into retreival channel with a next node use
		}

		// chunk := Chunk_for_channel{
		// 	ChunkName:  chunkMeta.ChunkName,
		// 	ChunkData: chunkData,
		// }

		// `some_chunk_got_saved_channel` <- chunk
		DOWNLOADED_CHUNKS += 1
		fmt.Printf("⬇ : %d/%d %s \n", DOWNLOADED_CHUNKS, TOTAL_CHUNKS, chunkMeta.ChunkName)
		if DOWNLOADED_CHUNKS >= int(TOTAL_CHUNKS) {
			all_chunks_downloaded_notification_channel <- true
		}
	}

}

func listFilesOnServer() {
	// create a payload of action
	payload := messages.ListRequest{}

	wrapper := messages.Wrapper{
		Msg: &messages.Wrapper_ListRequest{ListRequest: &payload},
	}
	controllerHandler.Send(&wrapper)
	return
}

var ls_command_completed_notification_channel = make(chan bool, 1)

func handleListResponse(filenames []string, statusList []int64) {
	if len(filenames) == 0 {
		fmt.Printf("\n No files found.\n")
	}
	for i, file_name := range filenames {
		status := ""
		if statusList[i] <= 0 {
			status = "(Not available for some time)"
		}
		fmt.Printf("\n📂 %d - %s %s\n", i+1, file_name, status)
		time.Sleep(100 * time.Millisecond)
	}
	ls_command_completed_notification_channel <- true
	return
}

func handleDelete(file_name string) {
	payload := messages.DeleteRequest{
		FileName: file_name,
	}
	wrapper := messages.Wrapper{
		Msg: &messages.Wrapper_DeleteRequest{DeleteRequest: &payload},
	}

	controllerHandler.Send(&wrapper)
}

var file_delete_complete_notification_channel = make(chan bool, 1)

func handleDeleteResponse(file_name string, success bool) {
	file_delete_complete_notification_channel <- success
}

// stores the assembled file

func main() {
	// connect to a server on localhost 619
	controllerConnection, err := net.Dial("tcp", "orion01:21999")
	check(err)

	controllerHandler = messages.NewMessageHandler(controllerConnection)

	go handleController()

	for {
		fmt.Printf("\nSELECT OPTION\n---------------------|\n1 -> GET\n2 -> PUT\n3 -> ls\n4 -> Delete\n5 -> ChunkSize (%d MB)\n", CHUNK_IN_MB)
		var action int
		_, err := fmt.Scanln(&action)
		if err != nil {
			fmt.Println("Error reading action:", err)
			continue
		}
		switch action {
		case 1: //GET
			// transfer file code here
			fmt.Print("Enter FileName: ")
			var FileName string
			_, err := fmt.Scanln(&FileName)
			check(err)
			getAction(FileName)
			result := <-get_action_completed_notification
			if !result {
				fmt.Printf("File not found\n")
			} else {
				fmt.Printf("Done!\n")
			}
			clean()
			continue
		case 2: //PUT
			// transfer file code here
			fmt.Print("Enter FileName: ")
			var FileName string
			_, err := fmt.Scanln(&FileName)
			check(err)
			putAction(FileName)
			<-put_action_completed_notification
			clean()
			continue
		case 3:
			// exit code here
			listFilesOnServer()
			<-ls_command_completed_notification_channel
			continue
		case 4:
			fmt.Printf("Enter file name to delete ")
			var FileName string
			_, err := fmt.Scanln(&FileName)
			check(err)
			go handleDelete(FileName)
			success := <-file_delete_complete_notification_channel
			fmt.Printf("\n%s delete %ssuccessful\n", FileName, func() string {
				if success {
					return ""
				} else {
					return "un"
				}
			}())
		case 5:
			var chunkSize int
			fmt.Println("Enter chunk size in MB - ")

			_, err := fmt.Scanln(&chunkSize)
			check(err)
			CHUNK_IN_MB = int(chunkSize)
			CHUNK_SIZE = int64(CHUNK_IN_MB * 1024 * 1024) // Depends on CHUNK_IN_MB

			continue
		default:
			fmt.Println("Invalid action. Exiting...")
			return
		}
	}
	/////

	// putAction()
	// time.Sleep(3 * time.Second)
	// getAction()

	//////////////

	select {}
	controllerHandler.Close()

}

// /// UTILS ///////
func createDir(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = os.Mkdir(path, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}

func handleController() {
	for {
		wrapper, err := controllerHandler.Receive()
		check(err)

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_RetrieveResponse:
			// fmt.Println("\n", "LOG:", "Retreival response. chunks count", len(msg.RetrieveResponse.ChunkNames))
			if !msg.RetrieveResponse.Success {
				get_action_completed_notification <- false
			} else {
				go retrieveAllChunks(msg.RetrieveResponse.GetChunks())
			}
			continue
		case *messages.Wrapper_StoreResponse:
			if msg.StoreResponse.GetSuccess() == true {
				fmt.Println("\nAllowed to upload file")
				go createRoutedChunksForUpload(msg.StoreResponse.FileName)
			} else {
				fmt.Println(msg.StoreResponse.Message)
				put_action_completed_notification <- true
			}
			continue
		case *messages.Wrapper_ChunkRouteResponse:

			ChunkName := msg.ChunkRouteResponse.GetChunkName()

			chunk := Chunk_for_channel{
				ChunkName:   ChunkName,
				PrimaryNode: msg.ChunkRouteResponse.GetNode(),
				currentPart: msg.ChunkRouteResponse.GetCurrentPart(),
				TotalParts:  msg.ChunkRouteResponse.GetTotalParts(),
				ChunkSize:   msg.ChunkRouteResponse.GetChunkSize(),
				FileName:    msg.ChunkRouteResponse.GetFileName(),
			}

			if msg.ChunkRouteResponse.GetSuccess() {

				// fmt.Println("\n", "LOG:", "ChunkRouteResponse response.\n", msg.ChunkRouteResponse)
				// fmt.Println("\n", "LOG:", "ChunkRouteResponse success.", chunk.ChunkName)

				ready_to_push_to_node <- chunk
			} else {
				fmt.Println("\n", "LOG:", "ChunkRouteResponse failed.", chunk.ChunkName)
			}
			continue
		case *messages.Wrapper_ListResponse:
			go handleListResponse(msg.ListResponse.GetFileNames(), msg.ListResponse.GetStatusList())
			continue
		case *messages.Wrapper_DeleteResponse:
			go handleDeleteResponse(msg.DeleteResponse.GetFileName(), msg.DeleteResponse.GetSuccess())
		default:
			fmt.Println("Controller Disconnected")
			return

		}
	}

}
