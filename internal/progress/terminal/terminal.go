package terminal

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/jmozgit/datagen/internal/progress"
)

type Terminal struct {
	out io.Writer
	tbl *table
}

func New(out io.Writer) *Terminal {
	return &Terminal{
		out: out,
		tbl: nil,
	}
}

func (t *Terminal) Draw(ctx context.Context, opts progress.FlushOptions, states map[string]progress.State) error {
	if t.tbl == nil {
		t.tbl = newTable(states)
	}

	t.tbl.draw(t, opts, states)

	return nil
}

func (t *Terminal) eraseLine() {
	t.write([]byte("\033[K"))
}

func (t *Terminal) moveCursorUp(n int) {
	t.write([]byte(fmt.Sprintf("\033[%dA", n)))
}

func (t *Terminal) saveCursor() {
	t.write([]byte("\033[s"))
}

func (t *Terminal) returnCursorToLastSaved() {
	t.write([]byte("\033[u"))
}

func (t *Terminal) newLine() {
	t.write([]byte("\n"))
}

func (t *Terminal) moveCursorDown(n int) {
	key := []byte(fmt.Sprintf("\033[%dB", n))
	t.out.Write(key)
}

func (t *Terminal) write(s []byte) {
	if _, err := t.out.Write(s); err != nil {
		slog.Error("write to terminal", slog.Any("error", err))
	}
}
