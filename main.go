package main

import (
	"encoding/csv"
	"github.com/influxdata/influxdb-client-go"
	"io"
	"main/config"
	"os"
	"time"
)

var client influxdb2.InfluxDBClient
var writeApi influxdb2.WriteApi
var csvFile *os.File
var filePath = "sample/sample.csv"
var measurement = "maas"

func init() {
	initClient()
	loadCsv(filePath)
}

func main() {
	var isHeader = true
	var header []string
	reader := csv.NewReader(csvFile)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		if isHeader {
			header = record
		} else {
			if len(header) == len(record) {
				tag := makeTag(record)
				filed := makeField(header, record)
				p := influxdb2.NewPoint(measurement, tag, filed, time.Now())
				writeApi.WritePoint(p)
			}
		}
		isHeader = false
	}
	writeApi.Flush()
	client.Close()
}

func initClient() {
	option := influxdb2.DefaultOptions().SetBatchSize(20)
	client = influxdb2.NewClientWithOptions(config.Url, config.Token, option)
	writeApi = client.WriteApi(config.Org, config.Bucket)
}

func loadCsv(file string) {
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	csvFile = f
}

func makeTag(record []string) map[string]string {
	tag := map[string]string{
		"tagKey": record[1],
	}
	return tag
}

func makeField(header []string, record []string) map[string]interface{} {
	field := map[string]interface{}{}
	for i := range header {
		field[header[i]] = record[i]
	}
	return field
}
