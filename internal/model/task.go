package model

type TaskProgress struct {
	Rows  uint64
	Bytes uint64
}

type Task struct {
	Limit  TaskProgress
	Schema DatasetSchema
}
