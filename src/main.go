package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"
)

type ExecuteResult int

const (
	ExecuteSuccess = ExecuteResult(iota)
	ExecuteTableFull
)

type MetaCommandResult int

const (
	MetaCommandSuccess = MetaCommandResult(iota)
	MetaCommandUnrecognizedCommand
)

type PrepareResult int

const (
	PrepareSuccess = PrepareResult(iota)
	PrepareSyntaxError
	PrepareUnrecognizedStatement
)

type StatementType int

const (
	StatementInsert = StatementType(iota)
	StatementSelect
)

const (
	ColumnUsernameSize = 32
	ColumnEmailSize    = 255
)

type Row struct {
	ID       uint32
	Username [ColumnUsernameSize]rune
	Email    [ColumnEmailSize]rune
}

type Statement struct {
	Type        StatementType
	RowToInsert Row
}

const (
	PageSize      = 4096
	TableMaxPages = 100
	RowSize       = int(unsafe.Sizeof(Row{}))
	RowsPerPage   = PageSize / RowSize
	TableMaxRows  = RowsPerPage * TableMaxPages
)

type Table struct {
	NumRows int
	Pages   [TableMaxPages]*[RowsPerPage]*[RowSize]byte
}

func printRow(row *Row) {
	var usernameLastChar, emailLastChar int
	var r rune

	for _, r = range row.Username {
		if r != '\x00' {
			usernameLastChar++
		}
	}

	for _, r = range row.Email {
		if r != '\x00' {
			emailLastChar++
		}
	}

	fmt.Printf("(%d, %s, %s)\n", row.ID, string(row.Username[0:usernameLastChar]), string(row.Email[0:emailLastChar]))
}

func serializeRow(source *Row, destination *[RowSize]byte) {
	tmp := bytes.NewBuffer([]byte{})
	binary.Write(tmp, binary.BigEndian, source)
	copy(destination[:], tmp.Bytes())
}

func deserializeRow(source *[RowSize]byte, destination *Row) {
	binary.Read(bytes.NewBuffer(source[:]), binary.BigEndian, destination)
}

func rowSlot(table *Table, rowNum int) *[RowSize]byte {
	pageNum := rowNum / RowsPerPage
	rowNumOnPage := rowNum - pageNum*RowsPerPage

	page := table.Pages[pageNum]
	if page == nil {
		table.Pages[pageNum] = &[RowsPerPage]*[RowSize]byte{}
	}

	row := table.Pages[pageNum][rowNumOnPage]
	if row == nil {
		table.Pages[pageNum][rowNumOnPage] = &[RowSize]byte{}
	}

	return table.Pages[pageNum][rowNumOnPage]
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

		var username, email string
		argsAssigned, _ := fmt.Fscanf(bytes.NewBuffer(inputScanner.Bytes()), "insert %d %s %s",
			&statement.RowToInsert.ID, &username, &email,
		)
		if argsAssigned < 3 {
			return PrepareSyntaxError
		}

		copy(statement.RowToInsert.Username[:], []rune(username))
		copy(statement.RowToInsert.Email[:], []rune(email))

		return PrepareSuccess
	}
	if bytes.Equal(inputScanner.Bytes(), []byte("select")) {
		statement.Type = StatementSelect
		return PrepareSuccess
	}

	return PrepareUnrecognizedStatement
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	if table.NumRows >= TableMaxRows {
		return ExecuteTableFull
	}

	rowToInsert := &statement.RowToInsert

	serializeRow(rowToInsert, rowSlot(table, table.NumRows))
	table.NumRows += 1

	return ExecuteSuccess
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	var row Row
	for i := 0; i < table.NumRows; i++ {
		deserializeRow(rowSlot(table, i), &row)
		printRow(&row)
	}
	return ExecuteSuccess
}

func executeStatement(statement *Statement, table *Table) ExecuteResult {
	switch statement.Type {
	case StatementInsert:
		return executeInsert(statement, table)
	case StatementSelect:
		return executeSelect(statement, table)
	}
	panic("Unknown execute statement.")
}

func NewTable() *Table {
	table := &Table{}
	for i := 0; i < TableMaxPages; i++ {
		table.Pages[i] = nil
	}
	return table
}

func main() {
	table := NewTable()
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

		statement := &Statement{}
		switch prepareStatement(inputScanner, statement) {
		case PrepareSuccess:
			break
		case PrepareSyntaxError:
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of %q.\n", inputScanner.Bytes())
			continue
		}

		switch executeStatement(statement, table) {
		case ExecuteSuccess:
			fmt.Println("Executed.")
			break
		case ExecuteTableFull:
			fmt.Println("Error: Table full.")
			break
		}
	}
}
