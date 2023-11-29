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

### Result:
