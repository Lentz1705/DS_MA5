# DS MA 5
Distributed Systems Mandatory Assignment 5

## Structure
*client/client.go* is the client

*grpc/* contains the .proto and generated go code

*server/node.go* is the node/replicant

*main.go* is the entry point to start the nodes/auction

## how to run
#### First build the node binary for the launcher to use
```bash
go build -o server/node server/node.go
```
IMPORTANT! On windows add file extension to the output `-o server/node.exe`

#### To start the auction run main.go with N amount of nodes 
```bash
go run main.go -n 5
```
when the launcher is running use `kill [id]` to terminate a subprocess and simulate a crash
or `stop` to kill all nodes and stop all subprocesses 

#### Finally each client can be started in a terminal with 
```bash
go run ./client/
```
type `bid [amount]` to participate in the auction and give a bid 
or `result` to get the current highest bid/winner if the auction is over

