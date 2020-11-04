package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
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
	PREPARE_NEGATIVE_ID
	PREPARE_STRING_TOO_LONG
	PREPARE_SYNTAX_ERROR
	PREPARE_UNRECOGNIZED_STATEMENT
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
	ROW_SIZE        = uint32(unsafe.Sizeof(Row{}))
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type Pager struct {
	fileDescriptor *os.File
	fileLength     uint32
	pages          [TABLE_MAX_PAGES]*bytes.Buffer
}

type Table struct {
	pager   *Pager
	numRows uint32
}

func pagerOpen(filename string) *Pager {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		fmt.Println("Unable to open file")
		os.Exit(8)
	}
	fileLength, _ := f.Seek(0, 2)

	pager := &Pager{}
	pager.fileDescriptor = f
	pager.fileLength = uint32(fileLength)

	for i := 0; i < TABLE_MAX_PAGES; i++ {
		pager.pages[i] = nil
	}
	return pager
}

func dbOpen(filename string) *Table {
	pager := pagerOpen(filename)
	numRows := pager.fileLength / ROW_SIZE
	table := &Table{}
	table.pager = pager
	table.numRows = numRows
	return table
}

func printRow(row *Row) {
	if *row == (Row{}) {
		return
	}
	fmt.Printf(
		"(%d, %s, %s)\n",
		row.Id,
		strings.Replace(string(row.Username[:]), "\x00", "", -1),
		strings.Replace(string(row.Email[:]), "\x00", "", -1),
	)
}

func serializeRow(source *Row, destination *bytes.Buffer) {
	binary.Write(destination, binary.BigEndian, source)
}

func deserializeRow(source *bytes.Buffer, destination *Row) {
	binary.Read(source, binary.BigEndian, destination)
}

func getPage(pager *Pager, pageNum uint32) *bytes.Buffer {
	if pageNum > TABLE_MAX_PAGES {
		_, _ = fmt.Fprintf(os.Stderr, "Tried to fetch page number out of bounds. %d > %d\n", pageNum, TABLE_MAX_PAGES)
		os.Exit(8)
	}

	if pager.pages[pageNum] == nil {
		// Cache miss. Allocate memory and load from file.
		page := bytes.NewBuffer(make([]byte, 0, PAGE_SIZE))
		numPages := pager.fileLength / PAGE_SIZE

		// We might save a partial page at the end of the file
		if pager.fileLength%PAGE_SIZE != 0 {
			numPages += 1
		}

		if pageNum <= numPages {
			pager.fileDescriptor.Seek(int64(pageNum*PAGE_SIZE), 0)
			tmp := make([]byte, PAGE_SIZE)
			n, err := pager.fileDescriptor.Read(tmp)
			if err != nil && err != io.EOF {
				_, _ = fmt.Fprintf(os.Stderr, "Error reading file: %s\n", err)
				os.Exit(8)
			}
			if n != 0 {
				page.Write(tmp)
			}
		}
		pager.pages[pageNum] = page
	}
	return pager.pages[pageNum]
}

func dbClose(table *Table) {
	pager := table.pager
	numFullPages := table.numRows / ROWS_PER_PAGE

	for i := uint32(0); i < numFullPages; i++ {
		if pager.pages[i] == nil {
			continue
		}
		pagerFlush(pager, i, PAGE_SIZE)
		pager.pages[i] = nil
	}

	numAdditionalRows := table.numRows % ROWS_PER_PAGE
	if numAdditionalRows > 0 {
		pageNum := numFullPages
		if pager.pages[pageNum] != nil {
			pagerFlush(pager, pageNum, numAdditionalRows*ROW_SIZE)
			pager.pages[pageNum] = nil
		}
	}

	err := pager.fileDescriptor.Close()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Error closing db file.")
		os.Exit(8)
	}

	for i := uint32(0); i < TABLE_MAX_PAGES; i++ {
		page := pager.pages[i]
		if page != nil {
			pager.pages[i] = nil
		}
	}
}

func pagerFlush(pager *Pager, pageNum uint32, size uint32) {
	if pager.pages[pageNum] == nil {
		_, _ = fmt.Fprintln(os.Stderr, "Tried to flush null page")
		os.Exit(8)
	}
	_, err := pager.fileDescriptor.Seek(int64(pageNum*PAGE_SIZE), 0)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error seeking: %d\n", err)
		os.Exit(8)
	}
	_, err = pager.fileDescriptor.Write(pager.pages[pageNum].Bytes())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error writing: %d\n", err)
		os.Exit(8)
	}
}

func rowSlot(table *Table, rowNum uint32) *bytes.Buffer {
	pageNum := rowNum / ROWS_PER_PAGE
	page := getPage(table.pager, pageNum)
	rowOffset := rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE
	page.Grow(int(byteOffset))
	return page
}

func doMetaCommand(input string, table *Table) MetaCommandResult {
	if input == ".exit" {
		dbClose(table)
		os.Exit(0)
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
}

func prepareInsert(input string, statement *Statement) PrepareResult {
	statement.t = STATEMENT_INSERT
	fields := strings.Fields(input)
	if len(fields) != 4 {
		return PREPARE_SYNTAX_ERROR
	}

	_, idString, username, email := fields[0], fields[1], fields[2], fields[3]
	if idString == "" || username == "" || email == "" {
		return PREPARE_SYNTAX_ERROR
	}

	id, _ := strconv.Atoi(idString)
	if id < 0 {
		return PREPARE_NEGATIVE_ID
	}
	if len(username) > COLUMN_USERNAME_SIZE {
		return PREPARE_STRING_TOO_LONG
	}

	statement.row.Id = int32(id)
	copy(statement.row.Username[:], []rune(username))
	copy(statement.row.Email[:], []rune(email))
	return EXECUTE_SUCCESS
}

func prepareStatement(input string, statement *Statement) PrepareResult {
	if len(input) >= 6 && input[:6] == "insert" {
		return prepareInsert(input, statement)
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
	for i := uint32(0); i < table.numRows; i++ {
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
	fmt.Print("db > ")
}

func main() {
	if len(os.Args) < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "Must supply a database filename.")
		os.Exit(8)
	}

	filename := os.Args[1]
	table := dbOpen(filename)

	reader := bufio.NewReader(os.Stdin)
	for {
		printPrompt()
		input, err := reader.ReadString('\n')
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Error reading input.")
			os.Exit(8)
		}
		input = strings.TrimSpace(input)
		if len(input) == 0 {
			continue
		}

		if input[0] == '.' {
			switch doMetaCommand(input, table) {
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
		case PREPARE_NEGATIVE_ID:
			fmt.Println("ID must be positive.")
			continue
		case PREPARE_STRING_TOO_LONG:
			fmt.Println("String is too long.")
			continue
		case PREPARE_SYNTAX_ERROR:
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", input)
			continue
		}

		switch executeStatement(&statement, table) {
		case EXECUTE_SUCCESS:
			fmt.Println("Executed.")
			break
		case EXECUTE_TABLE_FULL:
			fmt.Println("Error: Table full.")
			break
		}
	}
}
