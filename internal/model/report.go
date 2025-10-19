package model

type SaveReport struct {
	RowsSaved           int
	BytesSaved          int
	ConstraintViolation int
}

func (s SaveReport) Add(o SaveReport) SaveReport {
	return SaveReport{
		RowsSaved:           s.RowsSaved + o.RowsSaved,
		BytesSaved:          s.BytesSaved + o.BytesSaved,
		ConstraintViolation: s.ConstraintViolation + o.ConstraintViolation,
	}
}
