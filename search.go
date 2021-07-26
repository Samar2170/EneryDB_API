package main

import (
	"github.com/elastic/go-elasticsearch/v8"
	"bytes"
	"fmt"
	"encoding/json"
	"github.com/tidwall/gjson"
	"log"
)




func searchDatasets() {
	var b bytes.Buffer
	es, err := elasticsearch.NewDefaultClient()
	if err!=nil{
		fmt.Println(err)
	}
	res,_:=es.Search(es.Search.WithIndex("eia_series_detail"),es.Search.WithTrackTotalHits(true))
	b.ReadFrom(res.Body)

	values:= gjson.GetManyBytes(b.Bytes(), "hits.total.value", "took")
	fmt.Printf(
		"[%s] %d hits; took %d ms\n",
		res.Status(),
		values[0].Int(),
		values[1].Int(),
	)
}

func searchDs3() {
	es, err := elasticsearch.NewDefaultClient()
	if err!=nil{
		fmt.Println(err)
	}
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
		  "match": map[string]interface{}{
			"name": "Natural Gas",
		  },
		},
	}
	if err:=json.NewEncoder(&buf).Encode(query); err!=nil{
		log.Fatalf("Error - %s",err)
	}

	res,_:=es.Search(
		es.Search.WithIndex("eia_series_details"),
		es.Search.WithBody(&buf),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()
	
	if res.IsError() {
		var e map[string]interface{}
		if err:=json.NewDecoder(res.Body).Decode(&e); err !=nil{
			log.Fatalf("Error %s", err)
		} else {
			log.Fatalf("[%s] %s: %s",
			res.Status(),
			e["error"].(map[string]interface{})["type"],
			e["error"].(map[string]interface{})["reason"],	
		)
		}
	}

	var r  map[string]interface{}

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	log.Printf(
		"[%s] %d hits; took: %dms",
		res.Status(),
		int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)),
		int(r["took"].(float64)),
	)
	 
	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
		// searchId:=hit.(map[string]interface{})["_id"]
		source:=hit.(map[string]interface{})["_source"]
		id:=source.(map[string]interface{})["id"]
		code:=source.(map[string]interface{})["code"]
		name:=source.(map[string]interface{})["name"]
		log.Printf(" * ID=%v, %s  %s", id,code,name)
	}
	
}