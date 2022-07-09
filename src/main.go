package main

import (
	"bufio"
	"fmt"
	"os"
)

func printPrompt() {
	fmt.Print("db > ")
}

func readInput(inputScanner *bufio.Scanner) {
	inputScanner.Scan()
	bytesRead := len(inputScanner.Bytes())

	if bytesRead <= 0 {
		fmt.Println("Error reading input")
		os.Exit(1)
	}
}

func main() {
	inputScanner := bufio.NewScanner(os.Stdin)
	for {
		printPrompt()
		readInput(inputScanner)

		if inputScanner.Text() == ".exit" {
			os.Exit(0)
		} else {
			fmt.Printf("Unrecognized command %q.\n", inputScanner.Bytes())
		}
	}
}
