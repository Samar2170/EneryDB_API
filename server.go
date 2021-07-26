package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	//"bytes"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	// "sync"
	//"github.com/elastic/go-elasticsearch/v8"
)

var (
	ename string
	code  string
	id int
)

var Dataseries []string


type M map[string]interface{}

func datasets(w http.ResponseWriter, r *http.Request) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		print(err)
	}
	defer db.Close()

	sql := `SELECT "id" ,"Code", "Name" FROM core_seriesdetail ORDER BY "Name"`
	rows, err := db.Query(sql)
	if err != nil {
		log.Fatal(err)
	}

	var mySlice []M
	for rows.Next() {
		err := rows.Scan(&id, &code, &ename)
		if err != nil {
			panic(err)
		}


		m1:=M{"id":id,"code":code, "name":ename}
		mySlice=append(mySlice,m1)
		
	}
	json.NewEncoder(w).Encode(mySlice)
}

type D map[string]interface{}

var (
	data_id int
	period string
	data float64
)


func returnDataset(w http.ResponseWriter, r *http.Request) {
	vars:=mux.Vars(r)
	Id:=vars["id"]
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		print(err)
	}
	defer db.Close()
	var mySlice []D
	sql := `SELECT "id" ,"period", "data" FROM core_seriesdata WHERE series_id=$1`
	rows, err := db.Query(sql,Id)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		err := rows.Scan(&data_id, &period, &data)
		if err != nil {
			panic(err)
		}


		m1:=D{"id":data_id,"period":period, "value":data}
		mySlice=append(mySlice,m1)
		
	}
	fmt.Printf("datasets/%s fetched\n", Id)
	json.NewEncoder(w).Encode(mySlice)
}

func returnDatasetByCode(w http.ResponseWriter, r *http.Request) {
	vars:=mux.Vars(r)
	code:=vars["code"]
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		print(err)
	}
	defer db.Close()

	var mySlice []D
	sql1:=`SELECT "id" FROM core_seriesdetail WHERE "Code"=$1`
	row:=db.QueryRow(sql1,code)

	var id int
	row.Scan(&id)
	sql := `SELECT "id" ,"period", "data" FROM core_seriesdata WHERE series_id=$1`
	rows, err := db.Query(sql,id)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		err := rows.Scan(&data_id, &period, &data)
		if err != nil {
			panic(err)
		}


		m1:=D{"id":data_id,"period":period, "value":data}
		mySlice=append(mySlice,m1)
		
	}
	fmt.Printf("datasets/%s fetched\n", code)
	json.NewEncoder(w).Encode(mySlice)
}


func handleRequests() {
	myRouter := mux.NewRouter().StrictSlash(true)
	myRouter.HandleFunc("/datasets", datasets)
	myRouter.HandleFunc("/datasets/{id}", returnDataset)
	myRouter.HandleFunc("/datasets/code/{code}", returnDatasetByCode)
	log.Fatal(http.ListenAndServe(":10000", myRouter))
}
