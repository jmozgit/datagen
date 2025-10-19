package model

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
