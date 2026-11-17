package main

import (
	"bufio"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

var cmd []*exec.Cmd
var amountOfProcess = 5

func main() {
	cmd = make([]*exec.Cmd, amountOfProcess+1)
	for i := 1; i <= amountOfProcess; i++ {
		cmd[i] = exec.Command("./server/node",
			"-id", strconv.Itoa(i),
			"-totalNodes", strconv.Itoa(amountOfProcess),
		)
		log.Println(cmd[i].Args)
		cmd[i].Stderr = os.Stderr
		cmd[i].Stdout = os.Stdout
		if err := cmd[i].Start(); err != nil {
			log.Println(err)
		}
	}
	loop()
}

func killAll() {
	for i := 1; i <= amountOfProcess; i++ {
		cmd[i].Process.Kill()
	}
}

func loop() {
	var scanner = bufio.NewScanner(os.Stdin)
	for {
		eof := scanner.Scan()
		err := scanner.Err()
		if !eof || err != nil {
			killAll()
			return
		}
		args := strings.Split(scanner.Text(), " ")
		switch args[0] {
		case "kill":
			if args[1] == "all" {
				killAll()
			}
			idx, err := strconv.Atoi(args[1])
			if err != nil {
				break
			}
			cmd[idx].Process.Kill()
		case "stop":
			killAll()
			return
		default:

		}
	}
}
