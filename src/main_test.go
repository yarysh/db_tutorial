package main

import (
	"fmt"
	"io"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

func runScript(commands []string) []string {
	cmd := exec.Command("go", "run", "main.go")
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
		expect := []string{
			"db > ID must be positive.",
			"db > Executed.",
			"db > ",
		}
		if !reflect.DeepEqual(result, expect) {
			t.FailNow()
		}
	})
}
