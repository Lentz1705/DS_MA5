package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

func main() {
	var cmd *exec.Cmd
	var amountOfProcess = 5
	for i := 0; i < amountOfProcess; i++ {
		id := fmt.Sprintf("-id %d", i)
		totalNodes := fmt.Sprintf("-totalNodes %d", amountOfProcess)
		cmd = exec.Command("./bin/node", id, totalNodes)
	}
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		log.Println(err)
	}
	loop()
}

func loop() {
	var scanner = bufio.NewScanner(os.Stdin)
	for {
		eof := scanner.Scan()
		err := scanner.Err()
		if !eof || err != nil {
			// kill and return
			return
		}
		args := strings.Split(scanner.Text(), " ")
		switch args[0] {
		case "kill":
			// kill process at id args[1]
		case "stop":
			// kill all and return
			return
		default:

		}
	}
}
