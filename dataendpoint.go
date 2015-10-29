package main

import (
	sql "github.com/jmoiron/sqlx"
	_ "github.com/go-sql-driver/mysql"

)


type Row map[string]interface{}
type Result []Row


type DataEndPoint interface {
	Iterate(after uint64, limit int) Result
	Get(id string) Row
}

type MysqlDataEndPoint struct {
	db        *sql.DB
	tableName string
}

type WebEndPoint struct {

}

func (this WebEndPoint) Iterate(after uint64, limit int) Result {
	return Result{}
}

func (this WebEndPoint) Get(id string) Row {
	return Row{}
}


func (this MysqlDataEndPoint) Iterate(after uint64, limit int) Result {
	result, err := this.db.Queryx("Select * from " + this.tableName + " limit ?,?", after, limit)
	if err != nil {
		log.Error("Failed to get results from [%s], %v", this.tableName, err)
		return Result{}
	}
	defer result.Close()
	returnList := make([]Row, 0)
	for result.Next() {
		item := make(map[string]interface{})
		result.MapScan(item)
		returnList = append(returnList, item)
	}
	return returnList
}

func (this MysqlDataEndPoint) Get(id string) Row {
	return Row{}
}

func NewWebApi(config map[string]interface{}) (WebEndPoint, error) {
	return WebEndPoint{}, nil
}

func NewMysqlDataProvider(config map[string]interface{}) (MysqlDataEndPoint, error) {
	db, err := sql.Open("mysql", config["connectionString"].(string))
	if err != nil {
		return MysqlDataEndPoint{}, err
	}
	return MysqlDataEndPoint{db:db, tableName: config["table"].(string)}, nil
}