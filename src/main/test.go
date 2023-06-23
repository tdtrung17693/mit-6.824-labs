package main

import (
	"encoding/json"
	"log"
	"os"
)

type KeyValue struct {
	Key   string
	Value string
}

func loadJson(fileName string) []KeyValue {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open file. Details: %v", err)
	}
	decoder := json.NewDecoder(file)
	var intermediate []KeyValue
	if err := decoder.Decode(&intermediate); err != nil {
		log.Fatalf("cannot decode file. Details: %v", err)
	}
	return intermediate
}

func main() {
  a :=loadJson("mr-4-0")
  log.Println("%v",a)
}
