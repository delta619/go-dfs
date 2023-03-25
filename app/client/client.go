package main

import (
	"app/messages"
	"fmt"
	"net"
	"os"
	"io/ioutil"
)

func check(err error) {
	if err != nil {
		fmt.Println("Error occurred")
	}

}

func main() {

	file_name := os.Args[1]
	// connect to a server on localhost 619
	conn, err := net.Dial("tcp", "localhost:619")
	check(err)

	msgHandler := messages.NewMessageHandler(conn)
	
	//read file file_name and store the content in file_data


	// read file file_name and store the content in 

    data, err := ioutil.ReadFile(file_name)
    if err != nil {
        fmt.Println("Error reading file:", err)
        return
    }

    // fmt.Printf("File content: %s", data)

	filedata := []byte(data);

	// send request to controller to push a file
	file_transfer := messages.FileTransfer{FileName: file_name,
	FileData: filedata, FileSize: 123, 
	Checksum: filedata,
}
	
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_FileTransfer{FileTransfer: &file_transfer},
	}
	msgHandler.Send(wrapper)

	defer conn.Close()


	msgHandler.Close()

}
