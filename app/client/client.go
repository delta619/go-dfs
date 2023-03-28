package main

import (
	"app/messages"
	"crypto/md5"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime/debug"
	"sync"
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
var CHUNK_IN_MB = 1

var UPLOADED_FILES = 0
var DOWNLOADED_FILES = 0
var TOTAL_CHUNKS = int64(0)

var CHUNK_SIZE = int64(CHUNK_IN_MB * 1024 * 1024) // 20 MB

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

func getChunkFromNode() {
	for {
		chunk := <-chunk_retreive_channel
		// fmt.Println("\n", "LOG:", "Asking node for the chunk", chunk.chunkName, chunk.primaryNode)

		nodeHandler, isExisting, err := createNodeHandler(chunk.primaryNode)
		if err != nil {
			fmt.Println("\n", "LOG:", "Node down !, Cant Get Chunk", chunk.chunkName, chunk.primaryNode)
		}
		check(err)

		payload := messages.ChunkRequest{
			ChunkName: chunk.chunkName,
		}
		wrapper := messages.Wrapper{
			Msg: &messages.Wrapper_ChunkRequest{ChunkRequest: &payload},
		}
		nodeHandler.Send(&wrapper)

		if !isExisting {
			go listenFromNode(nodeHandler)
		}
	}
}

func getSize(filename string) int64 {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		fmt.Println("Error getting file info:", err)
		return 0
	}
	// fmt.Printf("Size of file %s is %d bytes\n", filename, fileInfo.Size())
	return fileInfo.Size()
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type Chunk_for_channel struct {
	chunkName      string
	fileName       string
	chunkBytes     []byte
	currentPart    int64
	totalParts     int64
	chunkSize      int64
	primaryNode    string
	SecondaryNodes []string
}

var chunkPushChannel_Chunk = make(chan Chunk_for_channel, 1)
var ready_to_push_to_node = make(chan Chunk_for_channel, 1)
var chunk_retreive_channel = make(chan Chunk_for_channel, 1)
var chunk_waiting_to_be_saved = make(chan Chunk_for_channel, 1)
var some_chunk_got_saved_channel = make(chan Chunk_for_channel, 1)
var all_chunks_received_notification_channel = make(chan string, 1)

var chunk_reading_rate_limiter_channel = make(chan bool, 5)

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

	sem := make(chan struct{}, 5)
	go func() {

		for i := int64(0); i < numChunks; i++ {
			wg.Add(1)
			sem <- struct{}{} // acquire semaphore
			go func(part int64) {
				// fmt.Println("\n", "LOG:", "Reading.", part, TOTAL_CHUNKS)
				chunk_reading_rate_limiter_channel <- true
				chunkData := readChunkBytes(file_name, part, CHUNK_SIZE)
				READING_COUNTER += 1
				chunkMD5 := fmt.Sprintf("%x", md5.Sum(chunkData))
				fileChunks[part] = chunkMD5
				chunk_for_channel := Chunk_for_channel{
					chunkName:   chunkMD5,
					currentPart: part,
					totalParts:  numChunks,
					chunkSize:   CHUNK_SIZE,
					fileName:    file_name,
				}
				<-chunk_reading_rate_limiter_channel
				chunkPushChannel_Chunk <- chunk_for_channel
				wg.Done()
				<-sem // release semaphore
			}(i)
		}
	}()

	wg.Wait()

	payload := messages.UploadFileMetaRequest{
		FileName:   file_name,
		ChunkNames: fileChunks,
	}
	wrapper := messages.Wrapper{
		Msg: &messages.Wrapper_UploadFileMetaRequest{UploadFileMetaRequest: &payload},
	}
	controllerHandler.Send(&wrapper)
	// fmt.Println("\n", "LOG:", "Meta sent", numChunks)

}

func get_the_routes() {
	for {

		chunk := <-chunkPushChannel_Chunk
		// time.Sleep(100 * time.Millisecond)
		chunk_payload := messages.ChunkRouteRequest{
			ChunkName:   chunk.chunkName,
			CurrentPart: chunk.currentPart,
			TotalParts:  chunk.totalParts,
			ChunkSize:   chunk.chunkSize,
			FileName:    chunk.fileName,
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
		// fmt.Println("\n", "LOG:", "Connecting to node", chunk.primaryNode, chunk.chunkName)

		nodeHandler, _, err := createNodeHandler(chunk.primaryNode)
		check(err)

		chunk_with_data := readChunkBytes(chunk.fileName, chunk.currentPart, chunk.chunkSize)
		chunk_payload := messages.UploadChunkRequest{
			ChunkName: chunk.chunkName,
			FileName:  chunk.fileName,
			ChunkData: chunk_with_data,
		}

		wrapper := messages.Wrapper{
			Msg: &messages.Wrapper_UploadChunkRequest{UploadChunkRequest: &chunk_payload},
		}
		nodeHandler.Send(&wrapper)

		UPLOADED_FILES += 1
		fmt.Printf("⬆ : %d/%d %s \n", UPLOADED_FILES, TOTAL_CHUNKS, chunk.chunkName)
		check(err)
		// Close the connection after use to avoid resource leakage
	}
}

func readChunkBytes(fileName string, currentPart int64, chunkSize int64) []byte {
	startByte := currentPart * chunkSize
	fullPath := TEST_FILES_DIRECTORY + fileName
	file, err := os.Open(fullPath)

	fileSize := getSize(fullPath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		panic(err)
	}
	defer file.Close()

	endByte := startByte + int64(chunkSize) // End of the chunk
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

func putAction() {

	file_name := os.Args[1]

	payload := messages.StoreRequest{
		FileName: file_name, FileSize: 1,
	}
	wrapper := messages.Wrapper{
		Msg: &messages.Wrapper_StoreRequest{StoreRequest: &payload},
	}
	controllerHandler.Send(&wrapper)

}

func getAction() {
	file_name := os.Args[1]
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

func retreiveAllChunks(file_name string, chunkNames []string, chunkNodes []string) {
	TOTAL_CHUNKS = int64(len(chunkNames))
	// run 50 routunes
	for i := 0; i < GO_getChunkFromNode; i++ {
		go getChunkFromNode()
	}
	for i := 0; i < GO_startChunkWriter; i++ {
		go startChunkWriter()
	}

	for i, _ := range chunkNames {

		chunk := Chunk_for_channel{
			chunkName:   chunkNames[i],
			primaryNode: chunkNodes[i],
		}

		chunk_retreive_channel <- chunk
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go waitForAllChunks(file_name, chunkNames, &wg)
	wg.Wait()
	fmt.Println("\n", "LOG:", "All chunks saved.", file_name)

	constructFile(file_name, chunkNames)

}

func constructFile(file_name string, chunkNames []string) {
	// Create a new file with the given name

	fmt.Printf("⏳ Constructing %s\n", file_name)
	fullOutputPath := OUTPUT + file_name
	f, err := os.Create(fullOutputPath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Append each chunk to the file
	for _, chunkName := range chunkNames {
		chunkPath := SANDBOX + chunkName
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
	data, err := os.ReadFile(fullOutputPath)
	file_md5 := fmt.Sprintf("%x", md5.Sum(data))

	os.WriteFile(OUTPUT+file_md5, data, 0644)

	fmt.Printf("✅ %s constructed \n", file_name)
}

// sends a request to a nodeHandler asking for a specific chunk

func listenFromNode(nodeHandler *messages.MessageHandler) {
	// fmt.Println("\n", "LOG:", "Listening from node", nodeHandler)

	for {
		wrapper, err := nodeHandler.Receive()
		check(err)
		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_ChunkResponse:

			chunk := Chunk_for_channel{
				chunkName:  msg.ChunkResponse.GetChunkName(),
				chunkBytes: msg.ChunkResponse.GetChunkData(),
			}
			// fmt.Println("\n", "LOG:", "Received chunk", chunk.chunkName)

			chunk_waiting_to_be_saved <- chunk
		}

	}
}

func waitForAllChunks(file_name string, chunks []string, wg *sync.WaitGroup) {
	defer wg.Done()

	chunk_received := make(map[string]bool)

	for {
		chunk := <-some_chunk_got_saved_channel
		// fmt.Println("\n", "LOG:", "Chunk Saved notification.", chunk.chunkName)

		chunk_received[chunk.chunkName] = true
		all_received := true
		for _, chunk := range chunks {
			if !chunk_received[chunk] {
				all_received = false
			}
		}
		if all_received {
			all_chunks_received_notification_channel <- file_name
			return
		}
	}
}

func startChunkWriter() {
	for {

		chunkMeta := <-chunk_waiting_to_be_saved
		// fmt.Println("\n", "LOG:", "Writing chunk.", chunkMeta.chunkName)

		chunkData := chunkMeta.chunkBytes

		chunkPath := SANDBOX + chunkMeta.chunkName
		err := os.WriteFile(chunkPath, chunkData, 0644)
		if err != nil {
			panic(err)
		}

		chunk := Chunk_for_channel{
			chunkName:  chunkMeta.chunkName,
			chunkBytes: chunkData,
		}

		some_chunk_got_saved_channel <- chunk
		DOWNLOADED_FILES += 1
		fmt.Printf("⬇ : %d/%d %s \n", DOWNLOADED_FILES, TOTAL_CHUNKS, chunkMeta.chunkName)
	}

}

// stores the assembled file

func main() {
	setup()
	// connect to a server on localhost 619
	controllerConnection, err := net.Dial("tcp", "orion01:21999")
	check(err)

	controllerHandler = messages.NewMessageHandler(controllerConnection)

	go handleController()

	// for {
	// 	fmt.Print("---------------------|\n1 -> GET\n2 -> PUT\n3 -> ls\n4 -> exit\n")
	// 	var action int
	// 	_, err := fmt.Scanln(&action)
	// 	if err != nil {
	// 		fmt.Println("Error reading action:", err)
	// 		continue
	// 	}
	// 	switch action {
	// 	case 1: //GET
	// 		// transfer file code here
	// 		// fmt.Print("Enter filename: ")
	// 		// var filename string
	// 		// _, err := fmt.Scanln(&filename)
	// 		getAction()
	// 		if err != nil {
	// 			fmt.Println("Error reading filename:", err)
	// 			continue
	// 		}
	// 		continue
	// 	case 2: //PUT

	// 		putAction()

	// 		continue
	// 	case 3:
	// 		// exit code here
	// 		fmt.Println("ls command is run.")
	// 		continue
	// 	default:
	// 		fmt.Println("Invalid action. Exiting...")
	// 		return
	// 	}
	// }
	/////////

	putAction()
	// time.Sleep(2 * time.Second)
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
			fmt.Println("\n", "LOG:", "Retreival response.", msg.RetrieveResponse)
			retreiveAllChunks(msg.RetrieveResponse.GetFileName(), msg.RetrieveResponse.GetChunkNames(), msg.RetrieveResponse.GetChunkNodes())
			return
		case *messages.Wrapper_StoreResponse:
			if msg.StoreResponse.GetSuccess() == true {
				fmt.Println("Allowed to upload file")
				createRoutedChunksForUpload(os.Args[1])
			} else {
				fmt.Println("File already present on server")

			}
		case *messages.Wrapper_ChunkRouteResponse:

			chunkName := msg.ChunkRouteResponse.GetChunkName()

			chunk := Chunk_for_channel{
				chunkName:   chunkName,
				primaryNode: msg.ChunkRouteResponse.GetNode(),
				currentPart: msg.ChunkRouteResponse.GetCurrentPart(),
				totalParts:  msg.ChunkRouteResponse.GetTotalParts(),
				chunkSize:   msg.ChunkRouteResponse.GetChunkSize(),
				fileName:    msg.ChunkRouteResponse.GetFileName(),
			}

			if msg.ChunkRouteResponse.GetSuccess() {

				// fmt.Println("\n", "LOG:", "ChunkRouteResponse response.\n", msg.ChunkRouteResponse)
				// fmt.Println("\n", "LOG:", "ChunkRouteResponse success.", chunk.chunkName)

				ready_to_push_to_node <- chunk
			} else {
				fmt.Println("\n", "LOG:", "ChunkRouteResponse failed.", chunk.chunkName)
			}
		default:
			fmt.Println("Unexpected message received")
			return

		}
	}
}

func setup() {
	// createDir(OUTPUT)
	// createDir(SANDBOX)
}
