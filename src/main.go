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
	ExecuteDuplicateKey
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
	RowToInsert Row // only used by insert statement
}

const (
	PageSize      = 4096
	TableMaxPages = 100
	RowSize       = uint32(unsafe.Sizeof(Row{}))
)

type Pager struct {
	FileDescriptor *os.File
	FileLength     int
	NumPages       int
	Pages          [TableMaxPages]*[PageSize]byte
}

type Table struct {
	Pager       *Pager
	RootPageNum int
}

type Cursor struct {
	Table      *Table
	PageNum    int
	CellNum    uint32
	EndOfTable bool // Indicates a position one past the last element
}

type NodeType int

const (
	NodeInternal = NodeType(iota)
	NodeLeaf
)

/*
 * Common Node Header Layout
 */
const NodeTypeSize = uint32(unsafe.Sizeof(uint16(0)))
const NodeTypeOffset = 0
const IsRootSize = uint32(unsafe.Sizeof(uint16(0)))
const IsRootOffset = NodeTypeSize
const ParentPointerSize = uint32(unsafe.Sizeof(uint32(0)))
const ParentPointerOffset = IsRootOffset + IsRootSize
const CommonNodeHeaderSize = NodeTypeSize + IsRootSize + ParentPointerSize

/*
 * Leaf Node Header Layout
 */
const LeafNodeNumCellsSize = uint32(unsafe.Sizeof(uint32(0)))
const LeafNodeNumCellsOffset = CommonNodeHeaderSize
const LeafNodeHeaderSize = CommonNodeHeaderSize + LeafNodeNumCellsSize

/*
 * Leaf Node Body Layout
 */
const LeafNodeKeySize = uint32(unsafe.Sizeof(uint32(0)))
const LeafNodeKeyOffset = 0
const LeafNodeValueSize = RowSize
const LeafNodeValueOffset = LeafNodeKeyOffset + LeafNodeKeySize
const LeafNodeCellSize = LeafNodeKeySize + LeafNodeValueSize
const LeafNodeSpaceForCells = PageSize - LeafNodeHeaderSize
const LeafNodeMaxCells = LeafNodeSpaceForCells / LeafNodeCellSize

func getNodeType(node *[PageSize]byte) NodeType {
	return NodeType(binary.BigEndian.Uint16(node[NodeTypeOffset : NodeTypeOffset+NodeTypeSize]))
}

func setNodeType(node *[PageSize]byte, typ NodeType) {
	binary.BigEndian.PutUint16(node[NodeTypeOffset:NodeTypeOffset+NodeTypeSize], uint16(typ))
}

func getLeafNodeNumCells(node *[PageSize]byte) uint32 {
	return binary.BigEndian.Uint32(node[LeafNodeNumCellsOffset : LeafNodeNumCellsOffset+LeafNodeNumCellsSize])
}

func setLeafNodeNumCells(node *[PageSize]byte, n uint32) {
	binary.BigEndian.PutUint32(node[LeafNodeNumCellsOffset:LeafNodeNumCellsOffset+LeafNodeNumCellsSize], n)
}

func getLeafNodeCell(node *[PageSize]byte, cellNum uint32) []byte {
	return node[LeafNodeHeaderSize+cellNum*LeafNodeCellSize : (LeafNodeHeaderSize+cellNum*LeafNodeCellSize)+LeafNodeCellSize]
}

func setLeafNodeCell(node *[PageSize]byte, cellNum uint32, c []byte) {
	copy(
		node[LeafNodeHeaderSize+cellNum*LeafNodeCellSize:(LeafNodeHeaderSize+cellNum*LeafNodeCellSize)+LeafNodeCellSize],
		c[:],
	)
}

func getLeafNodeKey(node *[PageSize]byte, cellNum uint32) uint32 {
	return binary.BigEndian.Uint32(getLeafNodeCell(node, cellNum)[:LeafNodeKeySize])
}

func setLeafNodeKey(node *[PageSize]byte, cellNum uint32, k uint32) {
	binary.BigEndian.PutUint32(getLeafNodeCell(node, cellNum)[:LeafNodeKeySize], k)
}

func leafNodeValue(node *[PageSize]byte, cellNum uint32) []byte {
	return getLeafNodeCell(node, cellNum)[LeafNodeKeySize:]
}

func printConstants() {
	fmt.Println("RowSize:", RowSize)
	fmt.Println("CommonNodeHeaderSize:", CommonNodeHeaderSize)
	fmt.Println("LeafNodeHeaderSize:", LeafNodeHeaderSize)
	fmt.Println("LeafNodeNumCellsSize:", LeafNodeNumCellsSize)
	fmt.Println("LeafNodeSpaceForCells:", LeafNodeSpaceForCells)
	fmt.Println("LeafNodeMaxCells:", LeafNodeMaxCells)
}

func printLeafNode(node *[PageSize]byte) {
	numCells := getLeafNodeNumCells(node)
	fmt.Printf("leaf (size %d)\n", numCells)
	for i := uint32(0); i < numCells; i++ {
		key := getLeafNodeKey(node, i)
		fmt.Printf("  - %d : %d\n", i, key)
	}
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

func serializeRow(source *Row, destination []byte) {
	tmp := bytes.NewBuffer([]byte{})
	binary.Write(tmp, binary.BigEndian, source)
	copy(destination[:], tmp.Bytes())
}

func deserializeRow(source []byte, destination *Row) {
	binary.Read(bytes.NewBuffer(source[:]), binary.BigEndian, destination)
}

func initializeLeafNode(node *[PageSize]byte) {
	setNodeType(node, NodeLeaf)
	setLeafNodeNumCells(node, 0)
}

func getPage(pager *Pager, pageNum int) *[PageSize]byte {
	if pageNum > TableMaxPages {
		panic(fmt.Sprintf("Tried to fetch page number out of bounds. %d > %d\n", pageNum, TableMaxPages))
	}

	if pager.Pages[pageNum] == nil {
		// Cache miss. Allocate memory and load from file.
		page := [PageSize]byte{}
		numPages := pager.FileLength / PageSize

		// We might save a partial page at the end of the file
		if pager.FileLength%PageSize != 0 {
			numPages += 1
		}

		if pageNum <= numPages {
			pager.FileDescriptor.Seek(int64(pageNum*PageSize), 0)
			tmp := make([]byte, PageSize)
			_, err := pager.FileDescriptor.Read(tmp)
			if err != nil && err != io.EOF {
				_, _ = fmt.Fprintf(os.Stderr, "Error reading file: %s\n", err)
				os.Exit(1)
			}
			copy(page[:], tmp)
		}

		pager.Pages[pageNum] = &page

		if pageNum >= pager.NumPages {
			pager.NumPages = pageNum + 1
		}
	}

	return pager.Pages[pageNum]
}

func tableStart(table *Table) *Cursor {
	cursor := &Cursor{}
	cursor.Table = table
	cursor.PageNum = table.RootPageNum
	cursor.CellNum = 0

	rootNode := getPage(table.Pager, table.RootPageNum)
	numCells := getLeafNodeNumCells(rootNode)
	cursor.EndOfTable = numCells == 0

	return cursor
}

/*
Return the position of the given key.
If the key is not present, return the position
where it should be inserted
*/
func tableFind(table *Table, key uint32) *Cursor {
	rootPageNum := table.RootPageNum
	rootNode := getPage(table.Pager, rootPageNum)

	if getNodeType(rootNode) == NodeLeaf {
		return leafNodeFind(table, rootPageNum, key)
	} else {
		fmt.Println("Need to implement searching an internal node")
		os.Exit(1)
	}

	return nil
}

func leafNodeFind(table *Table, pageNum int, key uint32) *Cursor {
	node := getPage(table.Pager, pageNum)
	numCells := getLeafNodeNumCells(node)

	cursor := &Cursor{}
	cursor.Table = table
	cursor.PageNum = pageNum

	// Binary search
	minIndex := uint32(0)
	onePastMaxIndex := numCells
	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		keyAtIndex := getLeafNodeKey(node, index)
		if key == keyAtIndex {
			cursor.CellNum = index
			return cursor
		}
		if key < keyAtIndex {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	cursor.CellNum = minIndex
	return cursor
}

func cursorValue(cursor *Cursor) []byte {
	pageNum := cursor.PageNum
	page := getPage(cursor.Table.Pager, pageNum)
	return leafNodeValue(page, cursor.CellNum)
}

func cursorAdvance(cursor *Cursor) {
	pageNum := cursor.PageNum
	node := getPage(cursor.Table.Pager, pageNum)

	cursor.CellNum += 1
	if cursor.CellNum >= getLeafNodeNumCells(node) {
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
	pager.NumPages = int(fileLength / PageSize)

	if fileLength%PageSize != 0 {
		fmt.Println("Db file is not a whole number of pages. Corrupt file.")
		os.Exit(1)
	}

	for i := 0; i < TableMaxPages; i++ {
		pager.Pages[i] = nil
	}
	return pager
}

func dbOpen(filename string) *Table {
	pager := pagerOpen(filename)

	table := &Table{}
	table.Pager = pager
	table.RootPageNum = 0

	if pager.NumPages == 0 {
		// New database file. Initialize page 0 as leaf node.
		rootNode := getPage(pager, 0)
		initializeLeafNode(rootNode)
	}

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

func pagerFlush(pager *Pager, pageNum int) {
	if pager.Pages[pageNum] == nil {
		_, _ = fmt.Fprintln(os.Stderr, "Tried to flush null page")
		os.Exit(1)
	}
	_, err := pager.FileDescriptor.Seek(int64(pageNum*PageSize), 0)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error seeking: %d\n", err)
		os.Exit(1)
	}

	if pager.Pages[pageNum] != nil {
		_, err = pager.FileDescriptor.Write((*pager.Pages[pageNum])[:])
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error writing: %d\n", err)
			os.Exit(1)
		}
	}
}

func dbClose(table *Table) {
	pager := table.Pager

	for i := 0; i < pager.NumPages; i++ {
		if pager.Pages[i] == nil {
			continue
		}
		pagerFlush(pager, i)
		pager.Pages[i] = nil
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
	} else if bytes.Equal(inputScanner.Bytes(), []byte(".btree")) {
		fmt.Println("Tree:")
		printLeafNode(getPage(table.Pager, 0))
		return MetaCommandSuccess
	} else if bytes.Equal(inputScanner.Bytes(), []byte(".constants")) {
		fmt.Println("Constants:")
		printConstants()
		return MetaCommandSuccess
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

func leafNodeInsert(cursor *Cursor, key uint32, value *Row) {
	node := getPage(cursor.Table.Pager, cursor.PageNum)

	numCells := getLeafNodeNumCells(node)
	if numCells >= LeafNodeMaxCells {
		// Node full
		fmt.Println("Need to implement splitting a leaf node.")
		os.Exit(1)
	}

	if cursor.CellNum < numCells {
		// Make room for new cell
		for i := numCells; i > cursor.CellNum; i-- {
			setLeafNodeCell(node, i, getLeafNodeCell(node, i-1))
		}
	}

	setLeafNodeNumCells(node, numCells+1)
	setLeafNodeKey(node, cursor.CellNum, key)
	serializeRow(value, leafNodeValue(node, cursor.CellNum))
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	node := getPage(table.Pager, table.RootPageNum)
	numCells := getLeafNodeNumCells(node)
	if numCells >= LeafNodeMaxCells {
		return ExecuteTableFull
	}

	rowToInsert := &statement.RowToInsert
	keyToInsert := rowToInsert.ID
	cursor := tableFind(table, keyToInsert)

	if cursor.CellNum < numCells {
		keyAtIndex := getLeafNodeKey(node, cursor.CellNum)
		if keyAtIndex == keyToInsert {
			return ExecuteDuplicateKey
		}
	}

	leafNodeInsert(cursor, rowToInsert.ID, rowToInsert)

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
		case ExecuteDuplicateKey:
			fmt.Println("Error: Duplicate key.")
			break
		case ExecuteTableFull:
			fmt.Println("Error: Table full.")
			break
		}
	}
}
