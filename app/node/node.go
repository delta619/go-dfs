package main

import (
	"app/messages"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func check(err error) {
	if err != nil {
		fmt.Println("Error occurred")
	}
}

func main() {
	host := os.Args[1] // my hostname
	port := os.Args[2] // my port
	hostname := host + `:` + port

	localAddr, err := net.ResolveTCPAddr("tcp", host+":"+port)
	if err != nil {
		fmt.Println("Error resolving local address:", err.Error())
		return
	}
	// fmt.Println("Local address is " + localAddr.String())

	remoteAddr, err := net.ResolveTCPAddr("tcp", "localhost:619")
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

	msgHandler := messages.NewMessageHandler(conn)

	// go worker(msgHandler, hostname);

	go updateMyHeartbeatAuto(msgHandler, hostname)
	go handleOSSignals(hostname, msgHandler)

	select {}

	msgHandler.Close()

}

func worker(){
	
}

func updateMyHeartbeatAuto(msgHandler *messages.MessageHandler, host string) {
	for {
		// create heartbeat payload and send it after every 5sec
		heartbeat := messages.Heartbeat{Host: host, Beat: true}
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_Heartbeat{Heartbeat: &heartbeat},
		}
		msgHandler.Send(wrapper)
		// awaiting response from server
		wrapper_res, _ := msgHandler.Receive()
		if wrapper_res.GetRes().GetIsSuccess() {
			fmt.Println("Heartbeat updated by server - " + host)
		} else {
			fmt.Println("Heartbeat rejected by server. Registering ... " + host)
			// payload for registering incase of exceeded timelimit
			register := messages.Register{Host: host}
			wrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_Reg{Reg: &register},
			}
			msgHandler.Send(wrapper)
		}
		time.Sleep(5 * time.Second)
	}
}

func handleOSSignals(host string, msgHandler *messages.MessageHandler) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh

	switch sig {
	case syscall.SIGTERM:
		heartbeat := messages.Heartbeat{Host: host, Beat: false}
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_Heartbeat{Heartbeat: &heartbeat},
		}
		msgHandler.Send(wrapper)
	case syscall.SIGINT:
		heartbeat := messages.Heartbeat{Host: host, Beat: false}
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_Heartbeat{Heartbeat: &heartbeat},
		}
		msgHandler.Send(wrapper)
	}
	fmt.Println("Interrupt handled successfully")
	msgHandler.Close()
	os.Exit(0)

}
