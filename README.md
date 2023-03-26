# Bigdata projct 1

The HDFS i am aiming to acheive through this project is a way to break down the complex tasks of storing huge file into chunks and essentialy storing those into *multiple storage nodes*, i.e. **Orion nodes**
# Files

While storing a file to our HDFS, Each file will be split into multiple chunks of specified size (1.5MB)  then finally stored to multiple nodes to maintain replication factor.
## Failure handling

In case of any failure of data node, the controller will get aware of it using heartbeats and accordingly select the secondary node for the file chunk which it was supposed to retrieve.
The presistent storage (in our case, SQLite) will play a role of Name node in this case.
## File retrieval

The controller will fetch the chunks from the respective nodes, assemble it in the controller node and then send it back to the client who requested it.