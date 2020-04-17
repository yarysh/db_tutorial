package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"unsafe"
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
	PREPARE_SYNTAX_ERROR
)

type ExecuteResult int

const (
	EXECUTE_SUCCESS = iota
	EXECUTE_TABLE_FULL
)

type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
)

type Row struct {
	Id       int32
	Username [COLUMN_USERNAME_SIZE]rune
	Email    [COLUMN_EMAIL_SIZE]rune
}

type Statement struct {
	t   StatementType
	row Row
}

const (
	TABLE_MAX_PAGES = 100
	PAGE_SIZE       = 4096
	ROW_SIZE        = unsafe.Sizeof(Row{})
	ROWS_PER_PAGE   = int(PAGE_SIZE / ROW_SIZE)
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type Table struct {
	numRows int
	pages   [TABLE_MAX_PAGES]*bytes.Buffer
}

func printRow(row *Row) {
	if *row == (Row{}) {
		return
	}
	fmt.Printf("(%d, %s, %s)\n", row.Id, string(row.Username[:]), string(row.Email[:]))
}

func serializeRow(source *Row, destination *bytes.Buffer) {
	err := binary.Write(destination, binary.BigEndian, source)
	if err != nil {
		panic(fmt.Sprintf("Can't serialize row '%s'.\n", err))
	}
}

func deserializeRow(source *bytes.Buffer, destination *Row) {
	_ = binary.Read(source, binary.BigEndian, destination)
}

func rowSlot(table *Table, rowNum int) *bytes.Buffer {
	pageNum := rowNum / ROWS_PER_PAGE
	page := table.pages[pageNum]
	if page == nil {
		table.pages[pageNum] = bytes.NewBuffer(make([]byte, 0, PAGE_SIZE))
		page = table.pages[pageNum]
	}
	return page
}

func freeTable(table *Table) {
	for i := 0; table.pages[i] != nil; i++ {
		table.pages[i] = nil
	}
	table = nil
}

func doMetaCommand(input string, table *Table) MetaCommandResult {
	if input == ".exit" {
		freeTable(table)
		os.Exit(0)
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
}

func prepareStatement(input string, statement *Statement) PrepareResult {
	if len(input) >= 6 && input[:6] == "insert" {
		statement.t = STATEMENT_INSERT
		var username, email string
		n, _ := fmt.Sscanf(input, "insert %d %s %s", &statement.row.Id, &username, &email)
		copy(statement.row.Username[:], []rune(username))
		copy(statement.row.Email[:], []rune(email))
		if n < 3 {
			return PREPARE_SYNTAX_ERROR
		}
		return PREPARE_SUCCESS
	}
	if input == "select" {
		statement.t = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}
	return PREPARE_UNRECOGNIZED_STATEMENT
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	if table.numRows >= TABLE_MAX_ROWS {
		return EXECUTE_TABLE_FULL
	}

	serializeRow(&statement.row, rowSlot(table, table.numRows))
	table.numRows++
	return EXECUTE_SUCCESS
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	var row Row
	for i := 0; i < table.numRows; i++ {
		deserializeRow(rowSlot(table, i), &row)
		printRow(&row)
	}
	return EXECUTE_SUCCESS
}

func executeStatement(statement *Statement, table *Table) ExecuteResult {
	switch statement.t {
	case STATEMENT_INSERT:
		return executeInsert(statement, table)
	case STATEMENT_SELECT:
		return executeSelect(statement, table)
	}
	panic("Unknown execute statement.")
}

func printPrompt() {
	fmt.Print("db> ")
}

func main() {
	table := Table{}

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
			switch doMetaCommand(input, &table) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s'.\n", input)
				continue
			}
		}

		statement := Statement{}
		switch prepareStatement(input, &statement) {
		case PREPARE_SUCCESS:
			break
		case PREPARE_SYNTAX_ERROR:
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", input)
			continue
		}

		switch executeStatement(&statement, &table) {
		case EXECUTE_SUCCESS:
			fmt.Println("Executed.")
			break
		case EXECUTE_TABLE_FULL:
			fmt.Println("Error: Table full.")
			break
		}
	}
}
