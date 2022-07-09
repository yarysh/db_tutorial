package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
)

type MetaCommandResult int

const (
	MetaCommandSuccess = MetaCommandResult(iota)
	MetaCommandUnrecognizedCommand
)

type PrepareResult int

const (
	PrepareSuccess = PrepareResult(iota)
	PrepareUnrecognizedStatement
)

type StatementType int

const (
	StatementInsert = StatementType(iota)
	StatementSelect
)

type Statement struct {
	Type StatementType
}

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

func doMetaCommand(inputScanner *bufio.Scanner) MetaCommandResult {
	if bytes.Equal(inputScanner.Bytes(), []byte(".exit")) {
		os.Exit(0)
	}
	return MetaCommandUnrecognizedCommand
}

func prepareStatement(inputScanner *bufio.Scanner, statement *Statement) PrepareResult {
	if bytes.Equal(inputScanner.Bytes()[0:6], []byte("insert")) {
		statement.Type = StatementInsert
		return PrepareSuccess
	}
	if bytes.Equal(inputScanner.Bytes(), []byte("select")) {
		statement.Type = StatementSelect
		return PrepareSuccess
	}

	return PrepareUnrecognizedStatement
}

func executeStatement(statement Statement) {
	switch statement.Type {
	case StatementInsert:
		fmt.Println("This is where we would do an insert.")
		break
	case StatementSelect:
		fmt.Println("This is where we would do a select.")
		break
	}
}

func main() {
	inputScanner := bufio.NewScanner(os.Stdin)
	for {
		printPrompt()
		readInput(inputScanner)

		if inputScanner.Bytes()[0] == '.' {
			switch doMetaCommand(inputScanner) {
			case MetaCommandSuccess:
				continue
			case MetaCommandUnrecognizedCommand:
				fmt.Printf("Unrecognized command %q\n", inputScanner.Bytes())
				continue
			}
		}

		statement := Statement{}
		switch prepareStatement(inputScanner, &statement) {
		case PrepareSuccess:
			break
		case PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of %q.\n", inputScanner.Bytes())
			continue
		}

		executeStatement(statement)
		fmt.Println("Executed.")
	}
}
