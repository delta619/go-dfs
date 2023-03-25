package main

import (
	"app/messages"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
	"io/ioutil"
    "crypto/md5"
    "crypto/sha256"
    "syscall"
	"math"
	"encoding/json"
	"errors"
	"math/rand"
	"strconv"
)

type FileMeta struct {
	FileName string   `json:"filename"`
	Chunks   []string `json:"chunks"`
}

type Chunk struct {
	ChunkName      string   `json:"chunkname"`
	PrimaryNode    string   `json:"primaryNode"`
	SecondaryNodes []string `json:"secondaryNodes"`
}

type Data struct {
    Chunks   []Chunk    `json:"chunks"`
    FileMeta []FileMeta `json:"fileMeta"`
}






var single_heartbeat_ping = 5

var (
	registrationMap = make(map[string]int)
	timestampMap    = make(map[string]time.Time)
	someMapMutex    = sync.RWMutex{}
)

func removeInactiveNodesAutomatically() {
	// in this technique of managing active/failed nodes, we simply check for every timestamp for registered hosts
	for {
		time.Sleep(1 * time.Second)
		for key, value := range timestampMap {
			if time.Since(value) > time.Duration(15*time.Second) && registrationMap[key] == 1 {
				deregister(key)
			}
		}
	}
}

func handleClient(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()

	for {
		wrapper, err := msgHandler.Receive()
		if err != nil {
			// log the error and wait for a while before retrying
			log.Printf("Error receiving message: %v", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}

		switch msg := wrapper.Msg.(type) {
			case *messages.Wrapper_Heartbeat:
				fmt.Println("Heartbeat received from Node")
				// handleNodeRequests(wrapper)
				validateHeartbeat(msgHandler, msg.Heartbeat.GetHost(), msg.Heartbeat.GetBeat())
			case *messages.Wrapper_Reg:
				fmt.Println("Register received from Node")

				register(msg.Reg.GetHost())
			
			case *messages.Wrapper_FileTransfer:
				fmt.Println("Req received from Client")

				handleClientRequests(msg.FileTransfer)
			case *messages.Wrapper_FileRequest:
				fmt.Println("Req received from CLient get put")

				if msg.FileRequest.Action == "put" {
					success_value, msg_value := validateIncomingFile(msg.FileRequest.GetFileName(), int(msg.FileRequest.GetFileSize()))
					res := messages.Response{Success: success_value, Message: msg_value}
					wrapper := &messages.Wrapper{
						Msg: &messages.Wrapper_FileResponse{FileResponse: &res},
					}
					msgHandler.Send(wrapper)

				} else if msg.FileRequest.Action == "get" {
					if !msg.FileRequest.GetAcknowledged() {
						success_value, msg_value := validateOutgoingFile(msg.FileRequest.GetFileName())
						res := messages.Response{Success: success_value, Message: msg_value}
						wrapper := &messages.Wrapper{
							Msg: &messages.Wrapper_FileResponse{FileResponse: &res},
						}
						msgHandler.Send(wrapper)
					} else {
						data, err := os.ReadFile("./server/" + msg.FileRequest.GetFileName())
						check(err)
						msg := messages.FileTransfer{FileName: msg.FileRequest.GetFileName(), FileData: data}
						wrapper = &messages.Wrapper{
							Msg: &messages.Wrapper_FileTransfer{FileTransfer: &msg},
						}

						msgHandler.Send(wrapper)

					}
				} else {
					fmt.Println("FileRequest is damanaged")
				}
			// case *messages.Wrapper_Act:
				
			// 	// pass the request to the handleClientRequests
			// 	handleClientRequests(msg.Act)



			}
		// reset the retry count if the message was successfully processed
	}
}

func handleClientRequests(msg *messages.FileTransfer ) {
	storagePath := "./storage/"
	outputPath := "./output/"
	metadataPath := "./metadata.json"
	createDir(storagePath)
	createDir(outputPath)

	err := fillEmptyKeys(metadataPath) // validate metadata.json file
	if err != nil {
		// Handle error
		fmt.Println(err)
	}

	file_name:= msg.GetFileName()
	file_data:= msg.GetFileData()
	chunkSize := 20 * 1024 * 1024 // 10 MB

	storeEachFile(storagePath, file_name, file_data, chunkSize)
	retrieveMainFile(file_name)
}


func storeEachFile(storagePath string, file_name string,  file_data []byte, chunkSize int){


	numChunks := int(math.Ceil(float64(len(file_data)) / float64(chunkSize)))
	fmt.Printf("Number of chunks  %d\n", numChunks)


	chunks := make([]Chunk, 0)
	chunkNames := []string{}

	for i := 0; i < numChunks; i++ {
		start := i * chunkSize
		end := int(math.Min(float64(start+chunkSize), float64(len(file_data))))
		chunkData := file_data[start:end]
		chunkName := fmt.Sprintf("%x", md5.Sum(chunkData))
		node_ids := create_random_node_ids()

		chunk := Chunk{
			ChunkName:	chunkName,
			// ChunkData:   string(chunkData),
			PrimaryNode: node_ids[0],
			SecondaryNodes : node_ids[1:],
		}
			chunks = append(chunks, chunk)
		chunkNames = append(chunkNames, chunkName)
		addOrUpdateChunksToJSONFile("metadata.json", chunk)
		fmt.Printf("Chunk added to meta json -  %s\n", chunkName)

		// Write the chunk to the storage
		err := ioutil.WriteFile(storagePath + chunkName, chunkData, 0644)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Chunk written to storage directory  -  %s\n", chunkName)

	}
	
	
	// Add the new FileMeta object to the existing JSON file
	err := addOrUpdateFileMetaToJSONFile("metadata.json", file_name, chunkNames)
	if err != nil {
		log.Fatal(err)

	}
}

func retrieveMainFile(file_name string){
	// Deserialize the JSON file

	fmt.Printf("-------Meta retrieval---------\n")

	chunkNamesOfFile, err := findChunkNamesUsingFileName("metadata.json", file_name)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Chunk names:", chunkNamesOfFile)
	}
	fmt.Printf("-------Chunking retrieval---------\n")
	chunks, err := extractChunksUsingChunkNames("metadata.json", chunkNamesOfFile)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		for _, chunk := range chunks {
			fmt.Println("Chunk name:", chunk.ChunkName)
			fmt.Println("Primary node:", chunk.PrimaryNode)
			fmt.Println("Secondary nodes:", chunk.SecondaryNodes)
		}
	}
	fmt.Printf("-------Reconstruction---------\n")
	
	reconstructFile(file_name, chunks)


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
	if err != nil {
		fmt.Println("Error in fillEmptyKeys")
		return err
	}
	
    if fileInfo.Size() == 0 {
        // Initialize the metadata with default values
        metadata := make(map[string]interface{})
        metadata["chunks"] = []interface{}{}
        metadata["fileMeta"] = []interface{}{}
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

// write
func addOrUpdateFileMetaToJSONFile(metadataFilePath string, file_name string, chunkNames []string) error {

	fileMeta := FileMeta{
		FileName: file_name,
		Chunks: chunkNames,
	}

	
	jsonData, err := ioutil.ReadFile(metadataFilePath)
    if err != nil {
        return err
    }

    // Parse the existing JSON data into a map
    var jsonMap map[string]interface{}
    err = json.Unmarshal(jsonData, &jsonMap)
    if err != nil {
        return err
    }

    // Extract the "fileMeta" array from the map
    fileMetaArray, exists := jsonMap["fileMeta"].([]interface{})
    if !exists {
        return errors.New("fileMeta not found in JSON data")
    }

    // Check if a FileMeta object with the same filename already exists in the array
    for i, v := range fileMetaArray {
        fm, ok := v.(map[string]interface{})
        if !ok {
            continue
        }
        fn, ok := fm["filename"].(string)
        if !ok {
            continue
        }
        if fn == fileMeta.FileName {
            // Replace the existing FileMeta object with the new one
            newFileMetaBytes, err := json.Marshal(fileMeta)
            if err != nil {
                return err
            }
            var newFileMeta map[string]interface{}
            err = json.Unmarshal(newFileMetaBytes, &newFileMeta)
            if err != nil {
                return err
            }
            fileMetaArray[i] = newFileMeta
            // Update the "fileMeta" array in the map
            jsonMap["fileMeta"] = fileMetaArray
            // Serialize the updated JSON data
            updatedJSON, err := json.Marshal(jsonMap)
            if err != nil {
                return err
            }
            // Write the updated JSON data to the file
            err = ioutil.WriteFile(metadataFilePath, updatedJSON, 0644)
            if err != nil {
                return err
            }
            return nil
        }
    }

    // Append the new FileMeta object to the array
    newFileMetaBytes, err := json.Marshal(fileMeta)
    if err != nil {
        return err
    }
    var newFileMeta map[string]interface{}
    err = json.Unmarshal(newFileMetaBytes, &newFileMeta)
    if err != nil {
        return err
    }
    fileMetaArray = append(fileMetaArray, newFileMeta)

    // Update the "fileMeta" array in the map
    jsonMap["fileMeta"] = fileMetaArray

    // Serialize the updated JSON data
    updatedJSON, err := json.Marshal(jsonMap)
    if err != nil {
        return err
    }

    // Write the updated JSON data to the file
    err = ioutil.WriteFile(metadataFilePath, updatedJSON, 0644)
    if err != nil {
        return err
    }

    return nil
}

func addOrUpdateChunksToJSONFile(jsonFilePath string, chunk Chunk) error {
    // Read the existing JSON file
    jsonData, err := ioutil.ReadFile(jsonFilePath)
    if err != nil {
        return err
    }

    // Parse the existing JSON data into a map
    var jsonMap map[string]interface{}
    err = json.Unmarshal(jsonData, &jsonMap)
    if err != nil {
        return err
    }

    // Extract the "chunks" array from the map
    chunksArray, exists := jsonMap["chunks"].([]interface{})
    if !exists {
        return errors.New("chunks not found in JSON data")
    }

    // Check if a Chunk object with the same ChunkName already exists in the array
    for i, v := range chunksArray {
        c, ok := v.(map[string]interface{})
        if !ok {
            continue
        }
        cn, ok := c["chunkname"].(string)
        if !ok {
            continue
        }
        if cn == chunk.ChunkName {
            // Replace the existing Chunk object with the new one
            newChunkBytes, err := json.Marshal(chunk)
            if err != nil {
                return err
            }
            var newChunk map[string]interface{}
            err = json.Unmarshal(newChunkBytes, &newChunk)
            if err != nil {
                return err
            }
            chunksArray[i] = newChunk
            // Update the "chunks" array in the map
            jsonMap["chunks"] = chunksArray
            // Serialize the updated JSON data
            updatedJSON, err := json.Marshal(jsonMap)
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
    }

    // Append the new Chunk object to the array
    newChunkBytes, err := json.Marshal(chunk)
    if err != nil {
        return err
    }
    var newChunk map[string]interface{}
    err = json.Unmarshal(newChunkBytes, &newChunk)
    if err != nil {
        return err
    }
    chunksArray = append(chunksArray, newChunk)

    // Update the "chunks" array in the map
    jsonMap["chunks"] = chunksArray

    // Serialize the updated JSON data
    updatedJSON, err := json.Marshal(jsonMap)
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

func findChunkNamesUsingFileName(metadataFile string, filename string) ([]string, error) {
    // Read JSON data from metadata file
    jsonData, err := ioutil.ReadFile(metadataFile)
    if err != nil {
        return nil, err
    }

    // Unmarshal JSON data into a Data struct
    var data Data
    err = json.Unmarshal(jsonData, &data)
    if err != nil {
        return nil, err
    }

    // Find the file metadata corresponding to the specified filename
    var fileMeta *FileMeta
    for i := 0; i < len(data.FileMeta); i++ {
        if data.FileMeta[i].FileName == filename {
            fileMeta = &data.FileMeta[i]
            break
        }
    }

    // Return error if file metadata not found
    if fileMeta == nil {
        return nil, fmt.Errorf("file not found in metadata: %s", filename)
    }

    // Return the list of chunk names corresponding to the file
    return fileMeta.Chunks, nil
}

func extractChunksUsingChunkNames(filePath string, chunkNames []string) ([]Chunk, error) {
    // Read JSON data from file
    jsonData, err := ioutil.ReadFile(filePath)
    if err != nil {
        return nil, err
    }

    // Unmarshal JSON data into a Data struct
    var data Data
    err = json.Unmarshal(jsonData, &data)
    if err != nil {
        return nil, err
    }

    // from all the chunks in the json file, extract the ones that are in the chunkNames array
	var chunks []Chunk
	for _, chunkName := range chunkNames {
		for _, chunk := range data.Chunks {
			if chunk.ChunkName == chunkName {
				chunks = append(chunks, chunk)
			}
		}
	}

	return chunks, nil
}


func reconstructFile(file_name string, chunks []Chunk) {
	

	// Create output file
	outFile, err := os.Create("./output/"+file_name)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer outFile.Close()

	// Write chunk data to output file
	for _, chunk := range chunks {
		// GET CHUNK DATA FROM STORAGE
		

		_, err = outFile.Write(getChunckData(chunk.ChunkName, chunk.PrimaryNode))
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	fmt.Println("File reconstructed successfully!")

}


// temp function to get chunk data from storage
func getChunckData(chunkName string, primaryNode string) ([]byte) {
	// get chunk data from primary node
	// send request to primary node
	// get response from primary node
	// write response to file
	// return
	fmt.Printf("Getting chunk data from primary node: %s\n", primaryNode)
	chunkData, err := ioutil.ReadFile("./storage/"+chunkName)
	if err != nil {
		fmt.Println(err)
		return nil
	}

return chunkData
}






func validateHeartbeat(msgHandler *messages.MessageHandler, host string, beat bool) {
	// if we send an isIssuccess-false then client seends to send a Registration request
	isRegistered := registrationMap[host]
	isSuccess := true
	message := "#"

	if beat == false {
		isSuccess = false
		// for handling client side interrupt
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

	// create a response payload
	payload := messages.Response{Success: isSuccess, Host: host, Message: message}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_Res{Res: &payload},
	}
	msgHandler.Send(wrapper)
}

func updateTimeStamp(host string) {
	someMapMutex.Lock()

	timestampMap[host] = time.Now()
	fmt.Println("Ping received from host - " + host + " - Updated timestamp")

	someMapMutex.Unlock()

}

func register(hostname string) {
	someMapMutex.Lock()

	unique_client_node := hostname
	registrationMap[unique_client_node] = 1
	timestampMap[unique_client_node] = time.Now()
	fmt.Println("Registered host - " + unique_client_node)

	someMapMutex.Unlock()

}

func deregister(hostname string) {
	someMapMutex.Lock()

	registrationMap[hostname] = 0
	fmt.Println("Host deregistered - " + hostname)

	someMapMutex.Unlock()

}

func main() {
	port:= os.Args[1];
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	for {
		fmt.Println("waiting for client on", port)
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			// only handles one client at a time:
			go handleClient(msgHandler)
		}
	}
}

/////////////////////

func check(err error) {
	if err != nil {
		fmt.Println("Error occurred")
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

func create_random_node_ids() ([]string){
		
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