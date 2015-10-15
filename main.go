package main

import (
	"encoding/csv"
	"flag"
	"os"
	"strings"
	"time"

	"gopkg.in/olivere/elastic.v2"

	"github.com/NeowayLabs/logger"
)

const defaultElasticSearch = "http://127.0.0.1:9200"

var (
	host      string
	index     string
	indexType string
	fieldlist string
	fields    []string
	output    string
)

func main() {
	flag.StringVar(&host, "host", defaultElasticSearch, "elastic search host to get data from")
	flag.StringVar(&index, "index", "", "name of index to export")
	flag.StringVar(&indexType, "type", "", "name of type inside of <index> to export [optional]")
	flag.StringVar(&fieldlist, "fieldlist", "", "list of fields to export")
	flag.StringVar(&output, "output", "", "name of file to output")

	flag.Parse()

	if host == "" || index == "" || indexType == "" || fieldlist == "" || output == "" {
		logger.Error("Missing some parameters")

		flag.Usage()
		os.Exit(1)
	}

	// Connect to client
	client, err := getESClient(host)
	if err != nil {
		logger.Fatal("Error connecting to `%s`: %+v", host, err.Error())
	}

	// Verify if index exists
	exists, err := client.IndexExists(index).Do()
	if err != nil {
		logger.Fatal("Error verifying if index <%s> exists: %+v", index, err.Error())
	}
	if !exists {
		logger.Fatal("The index <%s> doesn't exists, we need a valid index or alias", index)
	}

	// Verify if type exists
	exists, err = client.TypeExists().Index(index).Type(indexType).Do()
	if err != nil {
		logger.Fatal("Error verifying if type <%s/%s> exists: %+v", index, indexType, err.Error())
	}
	if !exists {
		logger.Fatal("The type <%s/%s> doesn't exists, we need a valid type", index, indexType)
	}

	fields = strings.Split(fieldlist, ",")
	if len(fields) == 0 || fields[0] == "" {
		logger.Fatal("Fields informed is invalid")
	}

	file, err := os.Create(output)
	if err != nil {
		logger.Fatal("Cannot create output file[%s]: %s", output, err.Error())
	}

	csvWriter := csv.NewWriter(file)
	csvWriter.Comma = ';'

	// Export index/type to output
	exporter := NewExporter(client, index).Size(10).BulkSize(1000)
	if indexType != "" {
		exporter = exporter.Type(indexType)
	}
	exporter.Fields(fields...)
	exporter.Writer(csvWriter)
	exporter.Progress(showExportProgress)

	// Implement HERE your search quey
	query := elastic.NewMatchAllQuery()
	exporter.Query(query)

	logger.Info("Starting exporting to <%s>...", output)
	exportStart = time.Now()

	resp, err := exporter.Do()
	if err != nil {
		logger.Fatal("Error trying exporting: %+v", err.Error())
	}

	logger.Info("Exported was completed in <%s>, %d documents successed and %d failed", time.Since(exportStart), resp.Success, resp.Failed)

	if len(resp.Errors) > 0 {
		logger.Warn("We get errors in some documents...")

		for _, respItem := range resp.Errors {
			logger.Error("Index[%s] Type[%s] Id[%s]: %s", respItem.Index, respItem.Type, respItem.Id, respItem.Error)
		}
	}
}

func getESClient(esURL string) (*elastic.Client, error) {
	esClient, err := elastic.NewClient(
		elastic.SetURL(esURL),
		elastic.SetSniff(false),
		elastic.SetErrorLog(logger.DefaultLogger.Handlers[0].(*logger.DefaultHandler).ErrorLogger),
		elastic.SetInfoLog(logger.DefaultLogger.Handlers[0].(*logger.DefaultHandler).DebugLogger),
		elastic.SetTraceLog(logger.DefaultLogger.Handlers[0].(*logger.DefaultHandler).DebugLogger),
	)

	if err != nil {
		return esClient, err
	}

	esVersion, err := esClient.ElasticsearchVersion(esURL)
	if err != nil {
		logger.Fatal("Error getting ES version: %+v", err.Error())
	}
	logger.Info("Connected in Elasticsearch <%s>, version %s", esURL, esVersion)

	return esClient, err
}

var (
	exportProgress = -1
	exportStart    time.Time
)

func showExportProgress(current, total int64) {
	percent := (float64(current) / float64(total)) * 100
	if int(percent) > exportProgress {
		exportProgress = int(percent)
		logger.Info("Exporting... %d%% [Time elapsed: %s]", exportProgress, time.Since(exportStart).String())
	}
}
