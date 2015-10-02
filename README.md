# ES-Export

This tool will export your data from [Elastic Search](https://www.elastic.co/) to CSV file. You can choose if you need
export a whole index or only a type.

## Usage

```
./es-export \
    -host=<source-elastic-search> \
    -index=<index-name> \
    -type=<type-name> \
    -output=<output-file>
```

* **-host:** source of data to export **<required>**

* **-index:** name of index to export **<required>**

* **-type:** name of type inside of <index> to export **<optional>**

* **-output:** name of csv file to write **<required>**
