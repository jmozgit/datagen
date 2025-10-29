package terminal

import (
	"fmt"

	"github.com/jmozgit/datagen/internal/progress"
)

type table struct {
	maxWidthName   int
	rows           int
	tableRow       map[int]string
	drawWithHeader bool
}

func newTable(states map[string]progress.State) *table {
	t := new(table)

	t.drawWithHeader = true
	t.tableRow = make(map[int]string)
	t.rows = len(states)
	t.maxWidthName = 10
	inactiveTable := len(states) - 1
	activeTable := 0

	for name, prg := range states {
		t.maxWidthName = max(t.maxWidthName, len(name)+1)
		if prg.ActualRows > 0 || prg.ActualSize > 0 {
			t.tableRow[activeTable] = name
			activeTable++
		} else {
			t.tableRow[inactiveTable] = name
			inactiveTable--
		}
	}

	return t
}

func (t *table) drawRow(terminal *Terminal, name string, state progress.State) {
	terminal.eraseLine()

	name = fmt.Sprintf("%-*s", t.maxWidthName, name)
	terminal.write([]byte(name))

	violationConstraints := fmt.Sprintf("%-20d", state.ViolationConstraints)
	terminal.write([]byte(violationConstraints))

	percent := float64(0)
	var rowsProgress string
	if state.TotalRows == 0 {
		rowsProgress = "-/-"
	} else {
		rowsProgress = fmt.Sprintf("%d/%d", state.ActualRows, state.TotalRows)
		percent = (float64(state.ActualRows) / float64(state.TotalRows)) * 100
	}
	rowsProgress = fmt.Sprintf("%-30s", rowsProgress)
	terminal.write([]byte(rowsProgress))

	var sizeProgress string
	if state.TotalSize == 0 {
		sizeProgress = "-/-"
	} else {
		asize := state.ActualSize.HumanReadable()
		tsize := state.TotalSize.HumanReadable()
		sizeProgress = fmt.Sprintf("%s/%s", asize, tsize)
		percent = (float64(state.ActualSize) / float64(state.TotalSize)) * 100
	}
	sizeProgress = fmt.Sprintf("%-30s", sizeProgress)
	terminal.write([]byte(sizeProgress))

	percentFormat := fmt.Sprintf("%.2f", percent)
	terminal.write([]byte(percentFormat))

	terminal.newLine()
}

func (t *table) draw(terminal *Terminal, _ progress.FlushOptions, states map[string]progress.State) {
	if t.drawWithHeader {
		header := fmt.Sprintf(
			"%-*s%-20s%-30s%-30s%-5s\n",
			t.maxWidthName, "table",
			"uniq_violations",
			"row_progress",
			"size_progres",
			"%",
		)
		terminal.write([]byte(header))
		t.drawWithHeader = false
	} else {
		terminal.moveCursorUp(t.rows)
	}

	for row := 0; row < t.rows; row++ {
		name := t.tableRow[row]
		t.drawRow(terminal, name, states[name])
	}
}
