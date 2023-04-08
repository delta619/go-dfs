# Bigdata projct 1 - DFS

The HDFS i am aiming to acheive through this project is a way to break down the complex tasks of storing huge file into chunks and essentialy storing those into *multiple storage nodes*, i.e. **Orion nodes**

## Execution

 1. Run the controller by executing `go run controller.go 21619`
 2. Connect all the storage nodes to the controller by executing `./start.sh` *, it will spin up 12 storage node instance on orion cluster respectively.*
 3. As our DFS is now up and running, we can start the Client using `go run client.go` 
 4. Optional - to spin up a specific storage node, `go run node.go orionxx 21xxx storageDir/data`

## Files

While storing a file to our HDFS, Each file will be split into multiple chunks of specified size (7MB)  then finally stored to multiple nodes to maintain replication factor.
## Failure handling

In case of any failure of data node, the controller will get aware of it from the heartbeart timestamps and accordingly maintain the replication factor of the file chunks.
The presistent storage (in our case, BitCask) is used to manage the list of nodes assigned to a chunk.

## File retrieval
The controller will fetch the chunks from the respective nodes, assemble it in the controller node and then send it back to the client who requested it.

