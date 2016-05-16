# wasbOrcToCsv

WASB ORC to CSV is a data engineering utility for converting Windows Azure Storage Blob-based Apache ORC files (typically from Hive) into CSV.

The process is the following:
- Authenticate in Azure
- Download ORC file from Blob store
- Write contents of ORC file a tempfile
- Using ORC C++ library to deserialize ORC
- Print ORC columns as CSV using custom "Flat class"

The utility is currently limited to ```struct``` ORC types, but others should be straightforward to add.

