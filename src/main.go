package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
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
	PrepareNegativeID
	PrepareStringTooLong
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

type Pager struct {
	FileDescriptor *os.File
	FileLength     int
	Pages          [TableMaxPages]*[RowsPerPage]*[RowSize]byte
}

type Table struct {
	NumRows int
	Pager   *Pager
}

type Cursor struct {
	Table      *Table
	RowNum     int
	EndOfTable bool
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

func getPage(pager *Pager, pageNum int) *[RowsPerPage]*[RowSize]byte {
	if pageNum > TableMaxPages {
		panic(fmt.Sprintf("Tried to fetch page number out of bounds. %d > %d\n", pageNum, TableMaxPages))
	}

	if pager.Pages[pageNum] == nil {
		// Cache miss. Allocate memory and load from file.
		page := &[RowsPerPage]*[RowSize]byte{}
		numPages := pager.FileLength / PageSize

		// We might save a partial page at the end of the file
		if pager.FileLength%PageSize != 0 {
			numPages += 1
		}

		if pageNum <= numPages {
			pager.FileDescriptor.Seek(int64(pageNum*PageSize), 0)
			tmp := make([]byte, PageSize)
			n, err := pager.FileDescriptor.Read(tmp)
			if err != nil && err != io.EOF {
				_, _ = fmt.Fprintf(os.Stderr, "Error reading file: %s\n", err)
				os.Exit(1)
			}
			if n != 0 {
				for i := 0; i < n/RowSize; i++ {
					if page[i] == nil {
						page[i] = &[RowSize]byte{}
					}
					copy(page[i][:], tmp[i*RowSize:i*RowSize+RowSize])
				}
			}
		}
		pager.Pages[pageNum] = page
	}
	return pager.Pages[pageNum]
}

func tableStart(table *Table) *Cursor {
	cursor := &Cursor{}
	cursor.Table = table
	cursor.RowNum = 0
	cursor.EndOfTable = table.NumRows == 0

	return cursor
}

func tableEnd(table *Table) *Cursor {
	cursor := &Cursor{}
	cursor.Table = table
	cursor.RowNum = table.NumRows
	cursor.EndOfTable = true

	return cursor
}

func cursorValue(cursor *Cursor) *[RowSize]byte {
	rowNum := cursor.RowNum
	pageNum := rowNum / RowsPerPage
	rowNumOnPage := rowNum - pageNum*RowsPerPage
	page := getPage(cursor.Table.Pager, pageNum)
	if page[rowNumOnPage] == nil {
		page[rowNumOnPage] = &[RowSize]byte{}
	}
	return page[rowNumOnPage]
}

func cursorAdvance(cursor *Cursor) {
	cursor.RowNum += 1
	if cursor.RowNum >= cursor.Table.NumRows {
		cursor.EndOfTable = true
	}
}

func pagerOpen(filename string) *Pager {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		fmt.Println("Unable to open file")
		os.Exit(1)
	}
	fileLength, _ := f.Seek(0, 2)

	pager := &Pager{}
	pager.FileDescriptor = f
	pager.FileLength = int(fileLength)

	for i := 0; i < TableMaxPages; i++ {
		pager.Pages[i] = nil
	}
	return pager
}

func dbOpen(filename string) *Table {
	pager := pagerOpen(filename)
	numRows := pager.FileLength / RowSize

	table := &Table{}
	table.Pager = pager
	table.NumRows = numRows

	return table
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

func pagerFlush(pager *Pager, pageNum int, size int) {
	if pager.Pages[pageNum] == nil {
		_, _ = fmt.Fprintln(os.Stderr, "Tried to flush null page")
		os.Exit(1)
	}
	_, err := pager.FileDescriptor.Seek(int64(pageNum*PageSize), 0)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error seeking: %d\n", err)
		os.Exit(1)
	}
	for _, row := range pager.Pages[pageNum] {
		if row == nil {
			continue
		}
		_, err = pager.FileDescriptor.Write(row[:])
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error writing: %d\n", err)
			os.Exit(1)
		}
	}
}

func dbClose(table *Table) {
	pager := table.Pager
	numFullPages := table.NumRows / RowsPerPage

	for i := 0; i < numFullPages; i++ {
		if pager.Pages[i] == nil {
			continue
		}
		pagerFlush(pager, i, PageSize)
		pager.Pages[i] = nil
	}

	numAdditionalRows := table.NumRows % RowsPerPage
	if numAdditionalRows > 0 {
		pageNum := numFullPages
		if pager.Pages[pageNum] != nil {
			pagerFlush(pager, pageNum, numAdditionalRows*RowSize)
			pager.Pages[pageNum] = nil
		}
	}

	err := pager.FileDescriptor.Close()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Error closing db file.")
		os.Exit(1)
	}

	for i := uint32(0); i < TableMaxPages; i++ {
		page := pager.Pages[i]
		if page != nil {
			pager.Pages[i] = nil
		}
	}
}

func doMetaCommand(inputScanner *bufio.Scanner, table *Table) MetaCommandResult {
	if bytes.Equal(inputScanner.Bytes(), []byte(".exit")) {
		dbClose(table)
		os.Exit(0)
	}
	return MetaCommandUnrecognizedCommand
}

func prepareInsert(inputScanner *bufio.Scanner, statement *Statement) PrepareResult {
	statement.Type = StatementInsert

	fields := bytes.Fields(inputScanner.Bytes())
	if len(fields) != 4 {
		return PrepareSyntaxError
	}

	_, idString, username, email := fields[0], string(fields[1]), string(fields[2]), string(fields[3])
	if len(idString) == 0 || len(username) == 0 || len(email) == 0 {
		return PrepareSyntaxError
	}

	id, err := strconv.Atoi(idString)
	if id < 0 || err != nil {
		return PrepareNegativeID
	}
	if len(username) > ColumnUsernameSize {
		return PrepareStringTooLong
	}
	if len(email) > ColumnEmailSize {
		return PrepareStringTooLong
	}

	statement.RowToInsert.ID = uint32(id)
	copy(statement.RowToInsert.Username[:], []rune(username))
	copy(statement.RowToInsert.Email[:], []rune(email))

	return PrepareSuccess
}

func prepareStatement(inputScanner *bufio.Scanner, statement *Statement) PrepareResult {
	if bytes.Equal(inputScanner.Bytes()[0:6], []byte("insert")) {
		return prepareInsert(inputScanner, statement)
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
	cursor := tableEnd(table)

	serializeRow(rowToInsert, cursorValue(cursor))
	table.NumRows += 1

	return ExecuteSuccess
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	cursor := tableStart(table)

	var row Row
	for !cursor.EndOfTable {
		deserializeRow(cursorValue(cursor), &row)
		printRow(&row)
		cursorAdvance(cursor)
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

func main() {
	if len(os.Args) < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "Must supply a database filename.")
		os.Exit(1)
	}

	filename := os.Args[1]
	table := dbOpen(filename)

	inputScanner := bufio.NewScanner(os.Stdin)
	for {
		printPrompt()
		readInput(inputScanner)

		if inputScanner.Bytes()[0] == '.' {
			switch doMetaCommand(inputScanner, table) {
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
		case PrepareNegativeID:
			fmt.Println("ID must be positive.")
			continue
		case PrepareStringTooLong:
			fmt.Println("String is too long.")
			continue
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
