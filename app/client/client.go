package main

import (
	"app/messages"
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
)

var chunkSize = 1 * 1024 * 1024 // 20 MB

func check(err error) {
	if err != nil {
		fmt.Println("Error occurred:", err)
		debug.PrintStack()

		os.Exit(1)
		return
	}
}

var controllerHandler *messages.MessageHandler

var nodeConnections = make(map[string]net.Conn)

var controllerHandlerMutex sync.Mutex
var nodeConnectionsMutex sync.Mutex

func getNodeConnections() map[string]net.Conn {
	nodeConnectionsMutex.Lock()
	defer nodeConnectionsMutex.Unlock()
	return nodeConnections
}

func setNodeConnection(addr string, conn net.Conn) {
	nodeConnectionsMutex.Lock()
	defer nodeConnectionsMutex.Unlock()
	nodeConnections[addr] = conn
}

func createNodeConnection(host string, port string) (net.Conn, error) {
	addr := fmt.Sprintf("%s:%s", host, port)
	conn, ok := getNodeConnections()[addr]
	if ok {
		// connection already exists, return it
		return conn, nil
	}

	// connection doesn't exist, create a new one

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	setNodeConnection(addr, conn)

	return conn, nil
}

func sendToNode(chunk_name string, host string, port string) {
	//1. connect to the node
	nodeConnection, err := createNodeConnection(host, port)
	check(err)

	nodeHandler := messages.NewMessageHandler(nodeConnection)

	//2. read the chunk from local
	chunk_data, err := ioutil.ReadFile("./storage/" + chunk_name)
	if err != nil {
		fmt.Errorf("Chunk was not found in local")
	}

	//3. Send the chunk data as a request to store in the node
	chunk_payload := messages.UploadChunkRequest{
		ChunkName: chunk_name,
		FileName:  os.Args[1],
		ChunkData: chunk_data,
	}
	wrapper := messages.Wrapper{
		Msg: &messages.Wrapper_UploadChunkRequest{UploadChunkRequest: &chunk_payload},
	}
	nodeHandler.Send(&wrapper)

	// Done storing the chunk
	fmt.Println("⬆: UPLOAD_CHUNK : ", host+port, chunk_name)
}
func createRoutedChunksForUpload(file_name string) {

	//1. create directory for the chunks
	sandboxPath := "./storage/"
	createDir(sandboxPath)

	//2. Read the Main file
	file_data, err := ioutil.ReadFile(file_name)
	if err != nil {
		log.Fatal(err)
	}

	//3. Calculate the number of chunks
	numChunks := int(math.Ceil(float64(len(file_data)) / float64(chunkSize)))
	fmt.Printf("Number of chunks  %d\n", numChunks)

	chunkNames := make([]string, numChunks)

	//4. Loop to store each chunk in the local
	var wg sync.WaitGroup
	for i := 0; i < numChunks; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start := i * chunkSize
			end := int(math.Min(float64(start+chunkSize), float64(len(file_data))))
			chunkData := file_data[start:end]
			chunkName := fmt.Sprintf("%x", md5.Sum(chunkData))

			chunkNames[i] = chunkName

			//5. writing every chunk file to local
			err := ioutil.WriteFile(sandboxPath+chunkName, chunkData, 0644)
			if err != nil {
				log.Fatal(err)
			}

		}(i)
	}
	wg.Wait()
	println("All chunks individually saved to local")

	// 6 send the chunk details to controller and ask which Node to put it in.
	// time.Sleep(1 * time.Second)

	concurrency := make(chan struct{}, 1)

	for i := 0; i < numChunks; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			concurrency <- struct{}{}

			chunk_payload := messages.ChunkRouteRequest{
				ChunkName: chunkNames[i],
			}
			wrapper := messages.Wrapper{
				Msg: &messages.Wrapper_ChunkRouteRequest{ChunkRouteRequest: &chunk_payload},
			}
			//7 Send the chunk details and asking which node to put it in.
			controllerHandler.Send(&wrapper)

			// 8 Wait for each request's response
			routeResponseReceived := false
			for {
				if routeResponseReceived {
					routeResponseReceived = false
					break
				}
				//9 when we receive the response
				wrapper, err := controllerHandler.Receive()
				check(err)

				switch msg := wrapper.Msg.(type) {
				case *messages.Wrapper_ChunkRouteResponse:
					routeResponseReceived = true
					if msg.ChunkRouteResponse.GetSuccess() {
						//10 we get the node's host and port and proceed to upload that chunk to the node.
						sendToNode(msg.ChunkRouteResponse.GetChunkName(), strings.Split(msg.ChunkRouteResponse.GetHost(), ":")[0], msg.ChunkRouteResponse.GetPort())
					} else {
						fmt.Printf("Route get failed for chunk %s\n", msg.ChunkRouteResponse.GetChunkName())
						routeResponseReceived = true
					}
				default:
					fmt.Println("Unexpected message received FREFDCGR")
					fmt.Println(msg)
				}
			}
			<-concurrency
		}(i)
	}
	wg.Wait()
	fmt.Println("All chunks uploaded")
	//11. after uploading all the chunks, we send the details of all the chunks of this file controller
	payload := messages.UploadFileMetaRequest{
		FileName:   file_name,
		ChunkNames: chunkNames,
	}
	wrapper := messages.Wrapper{
		Msg: &messages.Wrapper_UploadFileMetaRequest{UploadFileMetaRequest: &payload},
	}

	controllerHandler.Send(&wrapper)

}

func putAction() {

	file_name := os.Args[1]

	//1. Asking if we can store this file
	payload := messages.StoreRequest{
		FileName: file_name, FileSize: 1,
	}
	wrapper := messages.Wrapper{
		Msg: &messages.Wrapper_StoreRequest{StoreRequest: &payload},
	}
	//2. Store request sent to controller
	controllerHandler.Send(&wrapper)

	fileUploadingCompleted := false
	//3. Listening for response
	for {
		if fileUploadingCompleted {
			break
		}
		wrapper, err := controllerHandler.Receive()
		check(err)

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_StoreResponse:
			//4. if i get a success, te i procees for next steps
			if msg.StoreResponse.GetSuccess() == true {
				fmt.Println("Allowed to upload file")
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					createRoutedChunksForUpload(file_name)
				}()
				wg.Wait()
				fileUploadingCompleted = true
				break
			}

		default:
			fmt.Println("Unexpected message received HTYGR")

			return
			break
		}
		// this for loop should break after prepareUploading is done
	}
	fmt.Println("ENDOF PUTACTION", file_name)

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

	temp := 0
	for {
		if temp == 1 {
			break
		}
		wrapper, err := controllerHandler.Receive()
		check(err)

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_RetrieveResponse:
			retreiveAllChunks(file_name, msg.RetrieveResponse.GetChunkNames(), msg.RetrieveResponse.GetChunkNodes())
			return
		default:
			fmt.Println("Unexpected message received ASDGGTR")

			return
			break

		}
	}

}

func retreiveAllChunks(file_name string, chunkNames []string, chunkNodes []string) {
	// create a map to store the chunks
	chunks := make(map[string][]byte)

	// send a request for each chunk to the corresponding node
	for i, chunkName := range chunkNames {
		node := chunkNodes[i]
		chunk, err := sendChunkRequest(node, chunkName)
		if err != nil {
			// handle error
			return
		}
		chunks[chunkName] = chunk
		fmt.Println("⬇️ ", node, chunkName)
	}

	// assemble the chunks
	var fileBytes []byte
	for _, chunkName := range chunkNames {
		chunk, ok := chunks[chunkName]
		if !ok {
			// handle error
			return
		}
		fileBytes = append(fileBytes, []byte(chunk)...)
	}

	err := storeFile(file_name, fileBytes)
	check(err)

}

// sends a request to a nodeHandler asking for a specific chunk
func sendChunkRequest(node string, chunkName string) ([]byte, error) {

	host := strings.Split(node, ":")[0]
	port := strings.Split(node, ":")[1] //TODO: known handling

	nodeConnection, err := createNodeConnection(host, port)
	check(err)

	nodeHandler := messages.NewMessageHandler(nodeConnection)

	// println("asking for chunk from ", node)
	payload := messages.ChunkRequest{
		ChunkName: chunkName,
	}
	wrapper := messages.Wrapper{
		Msg: &messages.Wrapper_ChunkRequest{ChunkRequest: &payload},
	}

	nodeHandler.Send(&wrapper)

	exit := 0
	for {
		if exit == 1 {
			break
		}
		wrapper, err := nodeHandler.Receive()
		check(err)

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_ChunkResponse:
			exit = 1
			return msg.ChunkResponse.GetChunkData(), nil
		default:
			fmt.Println("Unexpected message received YSFYHREVCD")

			return []byte{0}, err

		}
	}
	return []byte{0}, nil
}

// stores the assembled file
func storeFile(file_name string, fileBytes []byte) error {
	// TODO: implement storeFile
	outputPath := "./output/"
	createDir(outputPath)

	// write fileBytes to file_name
	err := ioutil.WriteFile(outputPath+file_name, fileBytes, 0644)
	check(err)
	println("✅ ", file_name+" saved in "+outputPath)

	return nil
}

func main() {

	// connect to a server on localhost 619
	controllerConnection, err := net.Dial("tcp", "orion02:21619")
	check(err)

	controllerHandler = messages.NewMessageHandler(controllerConnection)

	// for {
	//     fmt.Print("---------------------|\n1 -> GET\n2 -> PUT\n3 -> ls\n4 -> exit\n")
	//     var action int
	//     _, err := fmt.Scanln(&action)
	//     if err != nil {
	//         fmt.Println("Error reading action:", err)
	//         continue
	//     }
	//     switch action {
	// 		case 1: //GET
	// 			// transfer file code here
	// 			fmt.Print("Enter filename: ")
	// 			var filename string
	// 			_, err := fmt.Scanln(&filename)
	// 			if err != nil {
	// 				fmt.Println("Error reading filename:", err)
	// 				continue
	// 			}
	// 			continue
	// 		case 2: //PUT

	// 			putAction()

	// 			continue
	// 		case 3:
	// 			// exit code here
	// 			fmt.Println("ls command is run.")
	// 			continue
	// 		default:
	// 			fmt.Println("Invalid action. Exiting...")
	// 			return
	//     }
	// }
	///////////////

	putAction()
	// time.Sleep(3 * time.Second)
	getAction()

	//////////////
	controllerHandler.Close()

}

// /// UTILS ///////
func createDir(sandboxPath string) error {
	if _, err := os.Stat(sandboxPath); os.IsNotExist(err) {
		err = os.Mkdir(sandboxPath, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}
