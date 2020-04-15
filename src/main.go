package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type MetaCommandResult int

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota
	META_COMMAND_UNRECOGNIZED_COMMAND
)

type PrepareResult int

const (
	PREPARE_SUCCESS PrepareResult = iota
	PREPARE_UNRECOGNIZED_STATEMENT
)

type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

type Statement struct {
	t StatementType
}

func doMetaCommand(input string) MetaCommandResult {
	if input == ".exit" {
		os.Exit(0)
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
}

func prepareStatement(input string, statement *Statement) PrepareResult {
	if input[:6] == "insert" {
		statement.t = STATEMENT_INSERT
		return PREPARE_SUCCESS
	}
	if input == "select" {
		statement.t = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}
	return PREPARE_UNRECOGNIZED_STATEMENT
}

func executeStatement(statement *Statement) {
	switch statement.t {
	case STATEMENT_INSERT:
		fmt.Println("This is where we would do an insert.")
		break
	case STATEMENT_SELECT:
		fmt.Println("This is where we would do a select.")
		break
	}
}

func printPrompt() {
	fmt.Print("db> ")
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	for {
		printPrompt()
		input, err := reader.ReadString('\n')
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Error reading input.")
			os.Exit(1)
		}
		input = strings.TrimSpace(input)
		if len(input) == 0 {
			continue
		}

		if input[0] == '.' {
			switch doMetaCommand(input) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s'.\n", input)
				continue
			}
		}

		statement := &Statement{}
		switch prepareStatement(input, statement) {
		case PREPARE_SUCCESS:
			break
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", input)
			continue
		}

		executeStatement(statement)
		fmt.Println("Executed.")
	}
}
