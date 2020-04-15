package main

import (
	"bufio"
	"fmt"
	"os"
)

func printPrompt() {
	fmt.Print("db> ")
}

func main() {
	printPrompt()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		command := scanner.Text()

		if command == ".exit" {
			os.Exit(0)
		} else if len(command) > 0 {
			fmt.Println("Unrecognized command ", command)
		}
		printPrompt()
	}

	if err := scanner.Err(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Error reading input")
	}
}
