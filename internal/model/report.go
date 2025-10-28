package model

import (
	"github.com/c2h5oh/datasize"
)

type SaveReport struct {
	RowsSaved           int
	ConstraintViolation int
}

func (s SaveReport) Add(o SaveReport) SaveReport {
	return SaveReport{
		RowsSaved:           s.RowsSaved + o.RowsSaved,
		ConstraintViolation: s.ConstraintViolation + o.ConstraintViolation,
	}
}

type ProgressState struct {
	Table                string
	RowsCollected        int64
	SizeCollected        datasize.ByteSize
	ViolationConstraints int64
}
