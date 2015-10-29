package main

type Row map[string]interface{}
type Result []Row

type DataEndPoint interface {
	Iterate(after uint64, limit int) (Result, error)
	Get(row map[string]interface{}) (Row, error)
}
