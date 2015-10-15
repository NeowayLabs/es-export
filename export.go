package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"strings"

	"github.com/NeowayLabs/logger"

	"gopkg.in/olivere/elastic.v2"
)

// Exporter ...
type Exporter struct {
	client   *elastic.Client
	index    string
	types    []string
	query    elastic.Query
	fields   []string
	bulkSize int
	size     int
	scroll   string
	progress ExporterProgressFunc
	writer   *csv.Writer
}

var bulkSize = 1000

// ExporterProgressFunc is a callback that can be used with Exporter
// to report progress while reindexing data.
type ExporterProgressFunc func(current, total int64)

// ExporterResponse ...
type ExporterResponse struct {
	Success int64
	Failed  int64
	Errors  []*elastic.BulkResponseItem
}

// NewExporter returns a new Exporter.
func NewExporter(client *elastic.Client, index string) *Exporter {
	return &Exporter{
		client: client,
		index:  index,
	}
}

// Type restricts the scan to the given type.
func (ex *Exporter) Type(typ string) *Exporter {
	if ex.types == nil {
		ex.types = make([]string, 0)
	}
	ex.types = append(ex.types, typ)
	return ex
}

// Types allows to restrict the scan to a list of types.
func (ex *Exporter) Types(types ...string) *Exporter {
	if ex.types == nil {
		ex.types = make([]string, 0)
	}
	ex.types = append(ex.types, types...)
	return ex
}

// Fields specifies the fields the scan query should load.
// The default fields are _source, _parent, _routing.
func (ex *Exporter) Fields(fields ...string) *Exporter {
	ex.fields = fields
	return ex
}

// BulkSize returns the number of documents to send to Elasticsearch per chunk.
// The default is 500.
func (ex *Exporter) BulkSize(bulkSize int) *Exporter {
	ex.bulkSize = bulkSize
	return ex
}

// Query sets the query to perform
func (ex *Exporter) Query(query elastic.Query) *Exporter {
	ex.query = query
	return ex
}

// Writer will be used to write documents from elastic search to file, for example
func (ex *Exporter) Writer(writer *csv.Writer) *Exporter {
	ex.writer = writer
	return ex
}

// Size is the number of results to return per shard, not per request.
// So a size of 10 which hits 5 shards will return a maximum of 50 results
// per scan request.
func (ex *Exporter) Size(size int) *Exporter {
	ex.size = size
	return ex
}

// Scroll specifies for how long the scroll operation on the source index
// should be maintained. The default is 5m.
func (ex *Exporter) Scroll(timeout string) *Exporter {
	ex.scroll = timeout
	return ex
}

// Progress indicates a callback that will be called while indexing.
func (ex *Exporter) Progress(f ExporterProgressFunc) *Exporter {
	ex.progress = f
	return ex
}

// Do starts the exporting process.
func (ex *Exporter) Do() (*ExporterResponse, error) {
	if ex.client == nil {
		return nil, errors.New("no source client")
	}
	if ex.index == "" {
		return nil, errors.New("no source index")
	}
	if ex.writer == nil {
		return nil, errors.New("no destination writer")
	}
	if ex.fields == nil {
		return nil, errors.New("no fields")
	}
	if ex.bulkSize <= 0 {
		ex.bulkSize = 500
	}
	if ex.scroll == "" {
		ex.scroll = "5m"
	}

	// Count total to report progress (if necessary)
	var err error
	var current, total int64
	if ex.progress != nil {
		total, err = ex.count()
		if err != nil {
			return nil, err
		}
	}

	// Prepare scan and scroll to iterate through the source index
	scanner := ex.client.Scan(ex.index).Scroll(ex.scroll).Fields(ex.fields...)
	if len(ex.types) > 0 {
		scanner = scanner.Types(ex.types...)
	}
	if ex.query != nil {
		scanner = scanner.Query(ex.query)
	}
	if ex.size > 0 {
		scanner = scanner.Size(ex.size)
	}

	cursor, err := scanner.Do()

	bulk := 0

	ret := &ExporterResponse{
		Errors: make([]*elastic.BulkResponseItem, 0),
	}

	if err := ex.writer.Write(ex.fields); err != nil {
		return nil, err
	}

	ex.writer.Flush()
	err = ex.writer.Error()
	if err != nil {
		logger.Fatal("Error flushing to file: %s", err.Error())
	}

	// Main loop iterates through the source index and bulk indexes into target.
	for {
		docs, err := cursor.Next()
		if err == elastic.EOS {
			break
		}
		if err != nil {
			return ret, err
		}

		if docs.TotalHits() > 0 {
			for _, hit := range docs.Hits.Hits {
				if ex.progress != nil {
					current++
					ex.progress(current, total)
				}

				var values []string
				for _, field := range ex.fields {
					if hit.Fields[field] == nil {
						values = append(values, "")
						continue
					}

					value := hit.Fields[field].([]interface{})
					items := make([]string, len(value))

					for i, item := range value {
						switch t := item.(type) {
						case string:
							items[i] = item.(string)
						case bool:
							items[i] = fmt.Sprintf("%t", item)
						case float64:
							items[i] = fmt.Sprintf("%f", item)
						default:
							logger.Error("unexpected type %T\n", t)
						}
					}

					values = append(values, strings.Join(items, "\n"))
				}

				if err := ex.writer.Write(values); err != nil {
					logger.Warn("Error writing to file: %s", err.Error())
					continue
				}

				bulk++
				if bulk >= bulkSize {
					bulk = 0

					ex.writer.Flush()
					err = ex.writer.Error()
					if err != nil {
						logger.Fatal("Error flushing to file: %s", err.Error())
					}
				}
			}
		}
	}

	if bulk >= 0 {
		ex.writer.Flush()
		err = ex.writer.Error()
		if err != nil {
			logger.Fatal("Error flushing to file: %s", err.Error())
		}
	}

	return ret, nil
}

// count returns the number of documents in the source index.
// The query is taken into account, if specified.
func (ex *Exporter) count() (int64, error) {
	service := ex.client.Count(ex.index)
	if len(ex.types) > 0 {
		service = service.Types(ex.types...)
	}
	if ex.query != nil {
		service = service.Query(ex.query)
	}
	return service.Do()
}
