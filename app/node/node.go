package main

import (
	"app/messages"
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var DATASTORE = "/bigdata/students/amalla2/DATASTORE/"
var NUM_OF_WORKERS = 1
var heartbeartController *messages.MessageHandler
var REQUESTS = uint64(0)

func check(err error) {
	if err != nil {
		fmt.Println("Error occurred")
		panic(err)
	}
}

type Chunk struct {
	chunkName string
	chunkData []byte
	handler   *messages.MessageHandler
}

var chunk_saving_channel = make(chan Chunk, 1)
var chunk_upload_channel = make(chan Chunk, 1)

func handleChunkSavingRequest() {
	// initialize a wait group and a semaphore channel with a buffer size of 5
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)

	for {
		chunk := <-chunk_saving_channel
		// acquire the semaphore before executing the goroutine
		sem <- struct{}{}
		wg.Add(1)
		func(chunk Chunk) {
			defer func() {
				// release the semaphore after the goroutine is done
				<-sem
				wg.Done()
			}()

			// fmt.Println("\n", "LOG:", "Saving chunk", chunk.chunkName)
			if (chunk.chunkName == "") || len(chunk.chunkData) == 0 {
				panic("omg")
			}
			err := ioutil.WriteFile(DATASTORE+chunk.chunkName, chunk.chunkData, 0644)
			if err != nil {
				log.Fatal(err)
			}
			// inform the controller that the chunk was saved
			this_node := os.Args[1] + ":" + os.Args[2]
			ChunkSavedPayload := messages.ChunkSaved{ChunkName: chunk.chunkName, Node: this_node, Replication: false, MakePrimary: true}
			wrapper := &messages.Wrapper{ // send the chunk saved message to the controller
				Msg: &messages.Wrapper_ChunkSaved{ChunkSaved: &ChunkSavedPayload},
			}
			heartbeartController.Send(wrapper)
			fmt.Println("⬇ ", this_node, chunk.chunkName)
		}(chunk)
	}
}

func handleChunkDownloadRequest() {
	for {
		chunkMeta := <-chunk_upload_channel
		clientHandler := chunkMeta.handler

		chunk_data, err := ioutil.ReadFile(DATASTORE + chunkMeta.chunkName)
		check(err)

		//check md5
		chunkMD5 := fmt.Sprintf("%x", md5.Sum(chunk_data))

		if chunkMD5 != chunkMeta.chunkName {
			// send back a dummy file
			// meanwhile tell controller to make seconday node send me the fresh replacement
			payload := messages.ChunkProxyPush{
				ChunkName: chunkMeta.chunkName,
				DestNode:  os.Args[1] + ":" + os.Args[2],
			}

			wrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_ChunkProxyPush{
					ChunkProxyPush: &payload,
				},
			}
			heartbeartController.Send(wrapper)
		}

		response_chunk_payload := messages.ChunkResponse{ChunkName: chunkMeta.chunkName, ChunkData: chunk_data}
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_ChunkResponse{ChunkResponse: &response_chunk_payload},
		}
		clientHandler.Send(wrapper)
		this_node := os.Args[1] + ":" + os.Args[2]

		fmt.Printf("⬆ %s - %s\n", this_node, chunkMeta.chunkName)
	}
}
func handleDataServices(msgHandler *messages.MessageHandler) {
	for {
		wrapper, err := msgHandler.Receive()
		REQUESTS++
		if err != nil {
			// log the error and wait for a while before retrying
			fmt.Printf("Error receiving message: %v\n", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_UploadChunkRequest: /*node*/
			// fmt.Println("UploadChunkRequest received for file", msg.UploadChunkRequest.GetChunkName())
			// save the chunk to disk
			chunk_data := msg.UploadChunkRequest.GetChunkData()
			chunk_name := msg.UploadChunkRequest.GetChunkName()

			chunk := Chunk{
				chunkName: chunk_name,
				chunkData: chunk_data,
			}

			chunk_saving_channel <- chunk

		case *messages.Wrapper_ChunkRequest: /*controller*/
			chunk_name := msg.ChunkRequest.GetChunkName()
			chunkMeta := Chunk{
				chunkName: chunk_name,
				handler:   msgHandler,
			}
			chunk_upload_channel <- chunkMeta

		case *messages.Wrapper_PutChunkReplica:
			chunkName := msg.PutChunkReplica.GetChunkName()
			chunkData := msg.PutChunkReplica.GetChunkData()

			handlePutChunkReplica(chunkName, chunkData)
		default:
			return
		}

	}

}

func handlePutChunkReplica(chunkName string, chunkData []byte) {

	ioutil.WriteFile(DATASTORE+chunkName, chunkData, 0644)
	fmt.Printf("🌥️ Replica %s saved to  - %s:%s \n", chunkName, os.Args[1], os.Args[2])

}

func sendReplicasNow(chunkName string, other_nodes []string) {

	hostname := os.Args[1]
	port := os.Args[2]

	fmt.Printf("%s, replicated to %s  \n", chunkName, strings.Join(other_nodes, ","))

	for _, other_node := range other_nodes {
		inter_node_connection, err := net.Dial("tcp", other_node)
		check(err)
		inter_node_connection_handler := messages.NewMessageHandler(inter_node_connection)

		chunk_data, err := ioutil.ReadFile(DATASTORE + chunkName)
		chunkMD5 := fmt.Sprintf("%x", md5.Sum(chunk_data))

		if chunkMD5 != chunkName {
			fmt.Printf("MD5 failed for replica rectification %s on node %s", chunkName, hostname)
			return
		}

		putChunkReplicaPayload := messages.PutChunkReplica{ChunkName: chunkName, ChunkData: chunk_data}

		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_PutChunkReplica{PutChunkReplica: &putChunkReplicaPayload},
		}

		inter_node_connection_handler.Send(wrapper)
		fmt.Printf("I - %s:%s, Sent replica to %s \n", hostname, port, other_node)

	}

}

///////////////// DELETE ///////////////////

func handleDeleteChunk(msgHandler *messages.MessageHandler, chunkName string) {
	success := true
	err := os.Remove(DATASTORE + chunkName)
	// fmt.Printf("Log: deleted %s %s\n", os.Args[1]+":"+os.Args[2], chunkName)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("Warning: trying to delete a chunk which doesnt exists\n")
		} else {
			success = false
		}
	}
	// fmt.Println("Chunk deleted successfully")

	payload := messages.DeleteChunkAck{
		ChunkName: chunkName,
		Success:   success,
		Node:      os.Args[1] + ":" + os.Args[2],
	}
	wrapper := messages.Wrapper{
		Msg: &messages.Wrapper_DeleteChunkAck{DeleteChunkAck: &payload},
	}
	msgHandler.Send(&wrapper)
}

var chunks_count_uploaded = 0

func worker(heartbeartController *messages.MessageHandler, host string) {

	for i := 0; i < NUM_OF_WORKERS; i++ {
		go handleChunkSavingRequest()
		go handleChunkDownloadRequest()

	}

	for {
		// fmt.Println("Waiting for message from Controller")
		wrapper, err := heartbeartController.Receive()
		REQUESTS++
		if err != nil {
			// log the error and wait for a while before retrying
			fmt.Printf("Error receiving message: %v", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_Heartbeat:
			// fmt.Print("Response to my heartbeat from Controller")
			heartbeat := msg.Heartbeat
			if heartbeat.GetBeat() == false {

				// fmt.Println(" - Failed, Registering again")
				register(heartbeartController, host)
				continue
			}

			// fmt.Print("Response to my heartbeat from Controller")

		case *messages.Wrapper_ChunkReplicaRoute:
			chunkName := msg.ChunkReplicaRoute.ChunkName
			otherNodes := msg.ChunkReplicaRoute.OtherNodes

			sendReplicasNow(chunkName, otherNodes)
		case *messages.Wrapper_DeleteChunk:
			handleDeleteChunk(heartbeartController, msg.DeleteChunk.GetChunkName())
		default:
			return

		}
		// reset the retry count if the message was successfully processed
	}

}

func updateMyHeartbeatAuto(heartbeartController *messages.MessageHandler, host string) {
	for {
		var stat syscall.Statfs_t
		path := "/bigdata/students/amalla2/DATASTORE"
		syscall.Statfs(path, &stat)

		available_size := stat.Bavail * uint64(stat.Bsize) / 1024 / 1024

		// create heartbeat payload and send it after every 5sec
		heartbeat := messages.Heartbeat{Host: host, Beat: true, DiskSpace: available_size, Requests: REQUESTS}
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_Heartbeat{Heartbeat: &heartbeat},
		}
		heartbeartController.Send(wrapper)

		// wait for 5 seconds before sending the next heartbeat
		time.Sleep(5 * time.Second)
	}
}

func register(heartbeartController *messages.MessageHandler, host string) {
	// payload for registering incase of exceeded timelimit or first time registration
	register := messages.Register{Host: host}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_Register{Register: &register},
	}
	heartbeartController.Send(wrapper)
}

func handleOSSignals(host string, heartbeartController *messages.MessageHandler) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh

	switch sig {
	case syscall.SIGTERM:
		heartbeat := messages.Heartbeat{Host: host, Beat: false}
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_Heartbeat{Heartbeat: &heartbeat},
		}
		heartbeartController.Send(wrapper)
	case syscall.SIGINT:
		heartbeat := messages.Heartbeat{Host: host, Beat: false}
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_Heartbeat{Heartbeat: &heartbeat},
		}
		heartbeartController.Send(wrapper)
	}
	fmt.Println("Interrupt handled successfully")
	heartbeartController.Close()
	os.Exit(0)

}

////////

func createDir(storagePath string) error {
	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		err = os.Mkdir(storagePath, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if len(os.Args) > 3 {
		setup()
	}
	host := os.Args[1] // my hostname
	port := os.Args[2] // my port
	hostname := host + `:` + port

	localAddr, err := net.ResolveTCPAddr("tcp", host+":"+"21620") // Ping/Heartbeart port our machine. Different from file service port
	if err != nil {
		fmt.Println("Error resolving local address:", err.Error())
		return
	}
	// fmt.Println("Local address is " + localAddr.String())

	remoteAddr, err := net.ResolveTCPAddr("tcp", "orion01:21619")
	if err != nil {
		fmt.Println("Error resolving remote address:", err.Error())
		return
	}
	// fmt.Println("Remote address is " + remoteAddr.String())

	conn, err := net.DialTCP("tcp", localAddr, remoteAddr)
	if err != nil {
		fmt.Println("Error dialing:", err.Error())
		return
	}

	defer conn.Close()

	heartbeartController = messages.NewMessageHandler(conn)
	defer heartbeartController.Close()

	go updateMyHeartbeatAuto(heartbeartController, hostname)
	go handleOSSignals(hostname, heartbeartController)
	go worker(heartbeartController, hostname)

	///////////////////////

	listener, err := net.Listen("tcp", ":"+port) // for clients, those need to connect with our 21001
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	for {
		fmt.Println("\nwaiting for client request on", port)
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			// only handles one client at a time:
			go handleDataServices(msgHandler)
		}
	}

	select {}
	defer listener.Close()

}

func setup() {
	appendDir := os.Args[3]
	DATASTORE = "/bigdata/students/amalla2/" + appendDir + "/"
	createDir(DATASTORE)

}
