# wasbOrcToCsv

WASB ORC to CSV is a data engineering utility for converting Windows Azure Storage Blob-based Apache ORC files (typically from Hive) into CSV.

The process is the following:
1) Authenticate in Azure
2) Download ORC file from Blob store
3) Write contents of ORC file a tempfile
4) Using ORC C++ library to deserialize ORC
5) Print ORC columns as CSV using custom "Flat class"

The utility is currently limited to ```struct``` ORC types, but others should be straightforward to add.

