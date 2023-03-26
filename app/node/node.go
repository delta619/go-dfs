package main

import (
	"app/messages"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var heartbeartController *messages.MessageHandler

func check(err error) {
	if err != nil {
		fmt.Println("Error occurred")
	}
}

var CHUNK_SERVICE_PORT = "21001" // all chunk related services are available on this port only

func handleRequests(msgHandler *messages.MessageHandler) {
	for {
		// fmt.Println("Waiting for message from Controller")
		wrapper, err := msgHandler.Receive()
		if err != nil {
			// log the error and wait for a while before retrying
			fmt.Printf("Error receiving message: %v", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_UploadChunkRequest: /*node*/
			// fmt.Println("UploadChunkRequest received for file", msg.UploadChunkRequest.GetChunkName())
			// save the chunk to disk
			chunk_data := msg.UploadChunkRequest.GetChunkData()
			chunk_name := msg.UploadChunkRequest.GetChunkName()

			createDir("./storage/")
			err := ioutil.WriteFile("./storage/"+chunk_name, chunk_data, 0644)
			if err != nil {
				log.Fatal(err)
			}
			// inform the controller that the chunk was saved

			// fmt.Println("Chunk saved to disk")
			// fmt.Println("Sending confirmation to Controller")
			ChunkSavedPayload := messages.ChunkSaved{ChunkName: chunk_name, Node: os.Args[1] + ":" + CHUNK_SERVICE_PORT, Success: true}
			wrapper := &messages.Wrapper{ // send the chunk saved message to the controller
				Msg: &messages.Wrapper_ChunkSaved{ChunkSaved: &ChunkSavedPayload},
			}
			heartbeartController.Send(wrapper)
		case *messages.Wrapper_ChunkRequest: /*controller*/

			// fmt.Println("ChunkRequest received for file", msg.ChunkRequest.GetChunkName())
			//read the chunk from disk
			chunk_data, err := ioutil.ReadFile("./storage/" + msg.ChunkRequest.GetChunkName())
			check(err)

			response_chunk_payload := messages.ChunkResponse{ChunkName: msg.ChunkRequest.GetChunkName(), ChunkData: chunk_data}
			wrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_ChunkResponse{ChunkResponse: &response_chunk_payload},
			}
			// fmt.Printf("Sending chunk %s to controller\n", msg.ChunkRequest.GetChunkName())
			msgHandler.Send(wrapper)

		}
		// reset the retry count if the message was successfully processed
	}

}

func worker(heartbeartController *messages.MessageHandler, host string) {
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
			fmt.Println(" Up ")

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
	host := os.Args[1] // my hostname
	port := os.Args[2] // my port
	hostname := host + `:` + port

	localAddr, err := net.ResolveTCPAddr("tcp", host+":"+port) // for orion, it should be orion01,02,03 : 21619, to communicate with controller ONLY
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

	go worker(heartbeartController, hostname)

	go updateMyHeartbeatAuto(heartbeartController, hostname)
	go handleOSSignals(hostname, heartbeartController)
	///////////////////////

	listener, err := net.Listen("tcp", ":"+"21001") // for clients, those need to connect with our 21001
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	for {
		fmt.Println("waiting for service request on", "21001")
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			// only handles one client at a time:
			go handleRequests(msgHandler)
		}
	}

	select {}

	heartbeartController.Close()

}
