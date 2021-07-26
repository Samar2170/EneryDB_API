package main

import (
	"github.com/elastic/go-elasticsearch/v8"
	"bytes"
	"fmt"
	"encoding/json"
	"github.com/tidwall/gjson"
	//"log"
)

func searchDs() {
	es, err := elasticsearch.NewDefaultClient()
	if err!=nil{
		fmt.Println(err)
	}
	var r map[string]interface{}

	res,_:=es.Search(es.Search.WithTrackTotalHits(true))
	json.NewDecoder(res.Body).Decode(&r)
	fmt.Printf(
		"[%s] %d hits; took: %dms\n",
		res.Status(),
		int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)),
		int(r["took"].(float64)),
	)

}

func searchDs2() {
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
