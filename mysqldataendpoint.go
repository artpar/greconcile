package main

import (
	_ "github.com/go-sql-driver/mysql"
	sql "github.com/jmoiron/sqlx"
)

type MysqlDataEndPoint struct {
	db        *sql.DB
	tableName string
}

func (this MysqlDataEndPoint) Iterate(after uint64, limit int) (Result, error) {
	result, err := this.db.Queryx("Select * from "+this.tableName+" limit ?,?", after, limit)
	if err != nil {
		log.Error("Failed to get results from [%s], %v", this.tableName, err)
		return Result{}, err
	}
	defer result.Close()
	returnList := make([]Row, 0)
	for result.Next() {
		item := make(map[string]interface{})
		result.MapScan(item)
		returnList = append(returnList, item)
	}
	return returnList, nil
}

func (this MysqlDataEndPoint) Get(row map[string]interface{}) (Row, error) {
	return Row{}, nil
}

func NewMysqlDataProvider(config map[string]interface{}) (MysqlDataEndPoint, error) {
	db, err := sql.Open("mysql", config["connectionString"].(string))
	if err != nil {
		return MysqlDataEndPoint{}, err
	}
	return MysqlDataEndPoint{db: db, tableName: config["table"].(string)}, nil
}
