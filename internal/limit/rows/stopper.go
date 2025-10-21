package rows

import (
	"github.com/viktorkomarov/datagen/internal/model"
)

type Stopper struct {
	rows int64
}

func NewStopper(rows int64) *Stopper {
	return &Stopper{rows: rows}
}

func (s *Stopper) ContinueAllowed(report model.SaveReport) (bool, error) {
	s.rows -= int64(report.RowsSaved)

	return s.rows > 0, nil
}
