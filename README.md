# naive-go-raft
Super simplified naive Golang implementation of Raft cluster concensus algorithm(https://raft.github.io/) in a simulator environment: a super simplified cluster with a handler that simulates the network. 

### How to use?
- Build the module using `go build`.
- `Curl` the end-points listed in `server.go` to send requests to the simulator at `localhost:8080`.

#### End-points:
- startHandler: Start the simulator, with `size` parameter being the size of the cluster.
  - `curl http://localhost:8080/startHandler?size=10`
- listNodes:    Get the status of all the nodes in the cluster. 
- stopLeader:   Stop the leader node.
- stopHandler:  Stop the simulator.
- getCommits:   Get the commit record of a single node, with `id:int` parameter begin the id of the node.
  - `curl http://localhost:8080/getCommits?id=1`
- getEvents:    Get the event record of a single node, with `id:int` parameter begin the id of the node.
  - `curl http://localhost:8080/getEvents?id=2`
- stopNode:     Stop a specific node, with `id:int` parameter begin the id of the node.
- startNode:    Start a specific node, with `id:int` parameter begin the id of the node.
- addPart:      Add a new partition (Untested)
- removePart:   Remove a partition (Untested)
- clientSend:   Simulates request sending from client, will be handled by the leader node, with `msg:string` being the content of the request.
  - `curl http://localhost:8080/clientSend?msg=hello`

### Bugs
- Fails to start sometimes. (no leader elected somehow)
- Deadlock. 
- Timeout condition.

### Todo:
- Parition merging
- Failed leader
- Code style fixing.
- Remove debug printing.
- Timeout fixing.