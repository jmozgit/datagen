package common

import "github.com/viktorkomarov/datagen/internal/model"

type DataPartitionerMut struct {
	data     [][]any
	from, to int
}

func NewDataPartionerMut(data [][]any) DataPartitionerMut {
	return DataPartitionerMut{
		data: data,
		from: 0,
		to:   len(data),
	}
}

func (b DataPartitionerMut) Len() int {
	return b.to - b.from
}

func (b DataPartitionerMut) Split() (DataPartitionerMut, DataPartitionerMut) {
	mid := b.Len() / 2
	before := DataPartitionerMut{data: b.data, from: b.from, to: b.from + mid}
	after := DataPartitionerMut{data: b.data, from: b.from + mid, to: b.to}

	return before, after
}

func (b DataPartitionerMut) Data() [][]any {
	return b.data[b.from:b.to]
}

func (b DataPartitionerMut) MakeInvalid(idx int) {
	b.data[b.from+idx] = model.DiscardedRow
}

func (b DataPartitionerMut) AllData() [][]any {
	return b.data
}
