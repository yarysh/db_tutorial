package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

func runScript(commands []string) []string {
	cmd := exec.Command("go", "run", "main.go", "test.db")
	stdin, _ := cmd.StdinPipe()
	defer stdin.Close()

	for _, command := range commands {
		io.WriteString(stdin, command+"\n")
	}

	// Read entire output
	rawOutput, _ := cmd.CombinedOutput()
	return strings.Split(string(rawOutput), "\n")
}

func TestDB(t *testing.T) {
	t.Run("inserts and retrieves a row", func(t *testing.T) {
		result := runScript([]string{
			"insert 1 user1 person1@example.com",
			"select",
			".exit",
		})
		defer os.Remove("test.db")

		expect := []string{
			"db > Executed.",
			"db > (1, user1, person1@example.com)",
			"Executed.",
			"db > ",
		}
		if !reflect.DeepEqual(result, expect) {
			t.FailNow()
		}
	})

	t.Run("prints error message when table is full", func(t *testing.T) {
		scripts := make([]string, 302)
		for i := 0; i <= 300; i++ {
			scripts[i] = fmt.Sprintf("insert %d user%d person%d@example.com", i, i, i)
		}
		scripts[len(scripts)-1] = ".exit"

		result := runScript(scripts)
		defer os.Remove("test.db")

		if result[len(scripts)-2] != "db > Error: Table full." {
			t.FailNow()
		}
	})

	t.Run("allows inserting strings that are the maximum length", func(t *testing.T) {
		longUsername := strings.Repeat("a", 32)
		longEmail := strings.Repeat("a", 255)
		script := []string{
			fmt.Sprintf("insert 1 %s %s", longUsername, longEmail),
			"select",
			".exit",
		}

		result := runScript(script)
		defer os.Remove("test.db")

		expect := []string{
			"db > Executed.",
			fmt.Sprintf("db > (1, %s, %s)", longUsername, longEmail),
			"Executed.",
			"db > ",
		}
		if !reflect.DeepEqual(result, expect) {
			t.FailNow()
		}
	})

	t.Run("prints an error message if id is negative", func(t *testing.T) {
		script := []string{
			fmt.Sprintf("insert -1 cstack foo@bar.com"),
			"select",
			".exit",
		}

		result := runScript(script)
		defer os.Remove("test.db")

		expect := []string{
			"db > ID must be positive.",
			"db > Executed.",
			"db > ",
		}
		if !reflect.DeepEqual(result, expect) {
			t.FailNow()
		}
	})

	t.Run("keeps data after closing connection", func(t *testing.T) {
		result1 := runScript([]string{
			"insert 1 user1 person1@example.com",
			".exit",
		})
		expect1 := []string{
			"db > Executed.",
			"db > ",
		}
		if !reflect.DeepEqual(result1, expect1) {
			t.FailNow()
		}

		result2 := runScript([]string{
			"select",
			".exit",
		})
		defer os.Remove("test.db")

		expect2 := []string{
			"db > (1, user1, person1@example.com)",
			"Executed.",
			"db > ",
		}
		if !reflect.DeepEqual(result2, expect2) {
			t.FailNow()
		}
	})

	t.Run("allows printing out the structure of a one-node btree", func(t *testing.T) {
		script := []string{
			"insert 3 user3 person3@example.com",
			"insert 1 user1 person1@example.com",
			"insert 2 user2 person2@example.com",
			".btree",
			".exit",
		}

		result := runScript(script)
		defer os.Remove("test.db")

		expect := []string{
			"db > Executed.",
			"db > Executed.",
			"db > Executed.",
			"db > Tree:",
			"leaf (size 3)",
			"  - 0 : 1",
			"  - 1 : 2",
			"  - 2 : 3",
			"db > ",
		}
		if !reflect.DeepEqual(result, expect) {
			fmt.Println(result)
			t.FailNow()
		}
	})

	t.Run("prints constants", func(t *testing.T) {
		script := []string{
			".constants",
			".exit",
		}

		result := runScript(script)
		defer os.Remove("test.db")

		expect := []string{
			"db > Constants:",
			"RowSize: 1152",
			"CommonNodeHeaderSize: 8",
			"LeafNodeHeaderSize: 12",
			"LeafNodeNumCellsSize: 4",
			"LeafNodeSpaceForCells: 4084",
			"LeafNodeMaxCells: 3",
			"db > ",
		}
		if !reflect.DeepEqual(result, expect) {
			t.FailNow()
		}
	})

	t.Run("prints an error message if there is a duplicate id", func(t *testing.T) {
		script := []string{
			"insert 1 user1 person1@example.com",
			"insert 1 user1 person1@example.com",
			"select",
			".exit",
		}

		result := runScript(script)
		defer os.Remove("test.db")

		expect := []string{
			"db > Executed.",
			"db > Error: Duplicate key.",
			"db > (1, user1, person1@example.com)",
			"Executed.",
			"db > ",
		}
		if !reflect.DeepEqual(result, expect) {
			t.FailNow()
		}
	})
}
