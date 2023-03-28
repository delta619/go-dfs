package main

import (
	"app/messages"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var DATASTORE = "/bigdata/students/amalla2/DATASTORE/"
var NUM_OF_WORKERS = 5
var heartbeartController *messages.MessageHandler

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

var chunk_saving_channel = make(chan Chunk, 50)
var chunk_upload_channel = make(chan Chunk, 50)

func handleChunkSavingRequest() {
	// initialize a wait group and a semaphore channel with a buffer size of 5
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)

	for {
		chunk := <-chunk_saving_channel
		// acquire the semaphore before executing the goroutine
		sem <- struct{}{}
		wg.Add(1)
		go func(chunk Chunk) {
			defer func() {
				// release the semaphore after the goroutine is done
				<-sem
				wg.Done()
			}()

			fmt.Println("\n", "LOG:", "Saving chunk", chunk.chunkName)
			err := ioutil.WriteFile(DATASTORE+chunk.chunkName, chunk.chunkData, 0644)
			if err != nil {
				log.Fatal(err)
			}
			// inform the controller that the chunk was saved
			this_node := os.Args[1] + ":" + os.Args[2]
			ChunkSavedPayload := messages.ChunkSaved{ChunkName: chunk.chunkName, Node: this_node, Success: true}
			wrapper := &messages.Wrapper{ // send the chunk saved message to the controller
				Msg: &messages.Wrapper_ChunkSaved{ChunkSaved: &ChunkSavedPayload},
			}
			heartbeartController.Send(wrapper)
			fmt.Println("⬇ ", this_node, chunk.chunkName)
		}(chunk)
	}
	wg.Wait()
}

func handleChunkDownloadRequest() {
	for {
		chunkMeta := <-chunk_upload_channel
		clientHandler := chunkMeta.handler

		chunk_data, err := ioutil.ReadFile(DATASTORE + chunkMeta.chunkName)
		check(err)

		response_chunk_payload := messages.ChunkResponse{ChunkName: chunkMeta.chunkName, ChunkData: chunk_data}
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_ChunkResponse{ChunkResponse: &response_chunk_payload},
		}
		clientHandler.Send(wrapper)
		this_node := os.Args[1] + ":" + os.Args[2]

		fmt.Printf("⬆ %s - %s\n", this_node, chunkMeta.chunkName)
	}
}
func handleRequests(msgHandler *messages.MessageHandler) {
	for {
		wrapper, err := msgHandler.Receive()
		if err != nil {
			// log the error and wait for a while before retrying
			fmt.Printf("Error receiving message: %v\n", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_UploadChunkRequest: /*node*/
			fmt.Println("UploadChunkRequest received for file", msg.UploadChunkRequest.GetChunkName())
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
		}
	}

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
			// fmt.Println(" Up ")

		case *messages.Wrapper_Register:
			// fmt.Println("Registration confirmation received from Controller")
		case *messages.Wrapper_FileRequest:
			// fmt.Println("File req received from Controller.")
		}
		// reset the retry count if the message was successfully processed
	}

}

func updateMyHeartbeatAuto(heartbeartController *messages.MessageHandler, host string) {
	for {
		// create heartbeat payload and send it after every 5sec
		heartbeat := messages.Heartbeat{Host: host, Beat: true}
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
	setup()
	host := os.Args[1] // my hostname
	port := os.Args[2] // my port
	hostname := host + `:` + port

	localAddr, err := net.ResolveTCPAddr("tcp", host+":"+"21619") // Ping/Heartbeart port our machine. Different from file service port
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
			go handleRequests(msgHandler)
		}
	}

	select {}

	heartbeartController.Close()

}

func setup() {
	// createDir(DATASTORE)

}
