# PySpark-Assignment
PySpark excersise with loading, filtering, joining and saving 2 csv files. That includes logging, schema validation, exception handling and tests.

### Assignment background:
A very small company called KommatiPara that deals with bitcoin trading has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their financial details to starting reaching out to them for a new marketing push.

### Examples
```
python main.py --src-clients-path raw_data/dataset_one.csv --src-details-path raw_data/dataset_two.csv --countries "France" "United States"
```

```
python main.py --src-clients-path raw_data/dataset_one.csv --src-details-path raw_data/dataset_two.csv --countries "Netherlands" "United Kingdom"
```

### Input data schemas:
- Client data schema (specified with: --src-clients-path, default path: raw_data/dataset_one.csv):
  
  |id|first_name|last_name|email|country|
  |--|----------|---------|-----|-------|

- Financial client details(specified with: --src-details-path, default path: raw_data/dataset_two.csv):
  
  |id|btc_a|cc_t|cc_n|
  |--|-----|----|----|

- Country names surrounded by quotation marks and seperated by spaces (--countries): 
  
### Output data schema (output path: client_data/output.csv):
|client_identifier|email|country|bitcoin_address|credit_card_type|
|-----------------|-----|-------|---------------|----------------|


### Example logs after one run
```
30-11-2023 14:43:27 - INFO - =======================================================================================
30-11-2023 14:43:32 - INFO - Loaded: raw_data/dataset_one.csv with 1000 records
30-11-2023 14:43:32 - INFO - Loaded: raw_data/dataset_two.csv with 1000 records
30-11-2023 14:43:32 - INFO - Rows: ['Netherlands', 'United Kingdom'] in column: country removed, resulting in row's change: 1000 -> 100
30-11-2023 14:43:32 - INFO - Columns dropped: first_name, last_name
30-11-2023 14:43:32 - INFO - Columns dropped: cc_n
30-11-2023 14:43:32 - INFO - DataFrames joined on id, using inner join
30-11-2023 14:43:32 - INFO - Renamed columns: id -> client_identifier, btc_a -> bitcoin_address, cc_t -> credit_card_type
30-11-2023 14:43:33 - INFO - DataFrame saved to: C:\Users\kmoszczy\Desktop\Python\PySpark-Assignment\client_data/output.csv
```
