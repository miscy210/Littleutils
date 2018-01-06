# Littleutils

## 1. pip_upgrader.py
#### This scripts is to upgrade the all packages installed in the python environment.

usage:

    # activate your python environment
    
    python pip_upgrade.py
    
    
## 2. elasticDataHelper.py

#### This .py script is like the elasticdump tool which is built as a pure python script. 
#### Since the elasticdump is built and installed via node, it narrowed the available scenes.
#### Also it adds an option to specify the fields you queried, so to export a much smaller data used to update.
#### And it holds a considerable speed in both data downloading and uploading.

usage:

    (1) simply dump the data in or out the elasticsearch engine
   
    python elasticDataHelper.py -i output.mapping -o http://127.0.0.1:9200/newindex/newtype -t mapping
    
    python elasticDataHelper.py -i output.json -o http://127.0.0.1:9200/newindex/newtype -t data
    
    
    # or 
    
    python elasticDataHelper.py --input output.mapping --output http://127.0.0.1:9200/newindex/newtype --type mapping
    
    python elasticDataHelper.py --input output.json --output http://127.0.0.1:9200/newindex/newtype --type data
    
    
    (2) specify the fields so to update data from source to target
    
    python elasticDataHelper.py -i http://127.0.0.1:9200/newindex/newtype -o http://127.0.0.1:9200/newindex1/newtype1 -t data -f field1 field2 filed3
    
    python elasticDataHelper.py --input http://127.0.0.1:9200/newindex/newtype --output http://127.0.0.1:9200/newindex1/newtype1 --type data --field field1 field2 filed3
