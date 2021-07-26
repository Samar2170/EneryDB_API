package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

// parsing issues,
func saveToDb() {
	files, err := ioutil.ReadDir("./Assets/Eia_Json")
	if err != nil {
		log.Println(err)
	}
	for _, f := range files {
		jsonFile, err := os.Open("./Assets/Eia_Json/" + f.Name())
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Sucessfully opened file for %s", f)
		defer jsonFile.Close()
		bytevalue, _ := ioutil.ReadAll(jsonFile)
		var result map[string]interface{}
		json.Unmarshal([]byte(bytevalue), &result)
		fmt.Println(result)
	}

}
