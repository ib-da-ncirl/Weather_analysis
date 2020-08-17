# weather

1. [Installation](#installation)
1. [Usage](#usage)
1. [Configuration](#configuration)
1. [Sample command lines](#sample-command-lines)
    1. [Save files](#save-files)
    1. [Analyse stations](#analyse-stations)
    1. [Upload data to hbase](#upload-data-to-hbase)

### Installation
Install dependencies via

    pip3 install -r requirements.txt

### Usage
    Usage: etl.py
     -h        |--help           : Display usage
     -c <value>|--config <value> : Specify path to configuration script
     -n <value>|--nrows <value>  : Number of rows of file to read
     -t <value>|--thrift <value> : Address of Thrift host; default localhost
     -p <value>|--port <value>   : Host port; default 9090
     -d <value>|--delay <value>  : Inter-request delay (sec); default 10
     -f <value>|--folder <value> : Folder to save files to; default ./
     -i <value>|--info <value>   : Path to save station data info to; default ./info.csv
     -u <value>|--uri <value>    : Uri for data; default https://cli.fusio.net/cli/climate_data/webdata/
     -b <value>|--begin <value>  : Minimum date for readings; yyyy-mm-dd
     -e <value>|--end <value>    : Maximum date for readings; yyyy-mm-dd
     -s        |--save           : Save files
     -a        |--analyse        : Analyse files
     -l        |--load           : Upload data to hbase
     -v        |--verbose        : Verbose mode

### Configuration

See [sample_config.yaml](sample_config.yaml) for details.  

### Sample command lines
#### Save files

Save the data for the specified stations 

    python3 etl.py -s -f ../data -v

Station data files will be downloaded and saved in the `../data` folder

#### Analyse stations

Analyse the data for the specified stations

    python3 etl.py -a -i ./results/info.csv -v

Data for station(s) will be analysed and saved in the `./results/info.csv` file

#### Upload data to hbase

Upload station data is to hbase. 

    python3 etl.py -l -v

Data for station(s) will be uploaded to the hbase server specified in the configuration file.

