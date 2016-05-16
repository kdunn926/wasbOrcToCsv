// Windows Azure Storage bits
#include "was/storage_account.h"
#include "was/blob.h"

// Apache ORC bits
#include "orc/ColumnPrinter.hh"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <memory>
#include <iostream>

int usage() {
  std::cerr << "\n wasb2orc takes three arguments: " ;
  std::cerr << "\n -a <AZURE ACCOUNT CONNECTION STRING>" ;
  std::cerr << "\n\te.g. \"DefaultEndpointsProtocol=https\\;AccountName=someOrgInsightData\\;AccountKey=...\"" ;
  std::cerr << "\n -c <AZURE STORAGE CONTAINER NAME>" ;
  std::cerr << "\n -b <AZURE BLOB FILENAME>" ;
  std::cerr << "\n" ;

  return -1;
}

namespace orc {
  void writeString(std::string& file, const char *ptr) ;

  // Custom column printer to print CSV rather than JSON
  class FlatStructColumnPrinter: public ColumnPrinter {
    private:
        std::vector<ColumnPrinter*> fieldPrinter;
        std::vector<std::string> fieldNames;
    public:
        FlatStructColumnPrinter(std::string&, const Type& type);
        virtual ~FlatStructColumnPrinter();
        void printRow(uint64_t rowId) override;
        void reset(const ColumnVectorBatch& batch) override;

  };

  FlatStructColumnPrinter::FlatStructColumnPrinter(std::string& buffer,
                                           const Type& type
                                           ): ColumnPrinter(buffer) {
    for(unsigned int i=0; i < type.getSubtypeCount(); ++i) {
      fieldNames.push_back(type.getFieldName(i));
      fieldPrinter.push_back(createColumnPrinter(buffer,
                                                 type.getSubtype(i)).release());
    }
  }

  void FlatStructColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
        writeString(buffer, "null");
    } 
    else {
        //writeChar(buffer, '{');
        for(unsigned int i=0; i < fieldPrinter.size(); ++i) {
            if (i != 0) {
                writeString(buffer, ",");
            }
            //writeChar(buffer, '"');
            //writeString(buffer, fieldNames[i].c_str());
            //writeString(buffer, "\": ");
            fieldPrinter[i]->printRow(rowId);
        }
        //writeChar(buffer, '}');
    }
  }


  FlatStructColumnPrinter::~FlatStructColumnPrinter() {
    for (size_t i = 0; i < fieldPrinter.size(); i++) {
        delete fieldPrinter[i];
    }
  }

  void FlatStructColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const StructVectorBatch& structBatch =
            dynamic_cast<const StructVectorBatch&>(batch);
    for(size_t i=0; i < fieldPrinter.size(); ++i) {
        fieldPrinter[i]->reset(*(structBatch.fields[i]));
    }
  }

  // Custom column printer object referencing FlatStruct impl above
  std::unique_ptr<ColumnPrinter> createCustomColumnPrinter(std::string& buffer,
                                                     const Type* type) {
    ColumnPrinter *result = NULL;
    if (type == NULL) {
        throw std::logic_error("Not yet implemented");
    } 
    else {
        switch(static_cast<int64_t>(type->getKind())) {
            // Only hanlding Struct types for now, others are likely
            // achievable as well
            case STRUCT:
                result = new orc::FlatStructColumnPrinter(buffer, *type);
                break;

            default:
                throw std::logic_error("Not yet implemented");
        }
    }
    return std::unique_ptr<ColumnPrinter>(result);
  }
}

int main(int argc, char** argv) {

  char* storageAccount = NULL;
  char* storageContainer = NULL;
  char* storageBlob = NULL;

  int index;
  int c;

  if (argc != 7) {
    return usage();
  }

  while ((c = getopt (argc, argv, "a:c:b:h")) != -1) {
    switch (c)
      {
      case 'a':
        storageAccount = optarg;
        break;
      case 'c':
        storageContainer = optarg;
        break;
      case 'b':
        storageBlob = optarg;
        break;
      case '?':
        if (optopt == 'c')
          fprintf (stderr, "Option -%c requires an argument.\n", optopt);
        else if (isprint (optopt))
          fprintf (stderr, "Unknown option `-%c'.\n", optopt);
        else
          fprintf (stderr,
                   "Unknown option character `\\x%x'.\n",
                   optopt);
        return -1;
      case 'h':
      default:
        return usage();
      }
  }

  if ( storageAccount == NULL ) {
      std::cerr << "\nStorage account not provided\n" ;
      return usage();
  }
  else if ( storageContainer == NULL ) {
      std::cerr << "\nStorage container not provided\n" ;
      return usage();
  }
  else if (  storageBlob == NULL ) {
      std::cerr << "\nStorage blob not provided\n" ;
      return usage();
  }

  std::cerr << "Container = " << storageContainer << " Blob = " << storageBlob << std::endl;

  for (index = optind; index < argc; index++) {
      printf ("Non-option argument %s\n", argv[index]);
  }

  // Define the connection-string with your values.
  const utility::string_t storage_connection_string(U(storageAccount));

  // Retrieve storage account from connection string.
  azure::storage::cloud_storage_account storage_account = azure::storage::cloud_storage_account::parse(storage_connection_string);

  // Create the blob client.
  azure::storage::cloud_blob_client blob_client = storage_account.create_cloud_blob_client();

  // Retrieve a reference to a previously created container.
  azure::storage::cloud_blob_container container = blob_client.get_container_reference(U(storageContainer));

  // Retrieve reference to a blob within the above container.
  azure::storage::cloud_block_blob blockBlob = container.get_block_blob_reference(U(storageBlob));

  // Save blob contents to a file.
  concurrency::streams::container_buffer<std::vector<uint8_t>> buffer;
  concurrency::streams::ostream output_stream(buffer);

  std::cerr << "Starting download" << std::endl; 
  std::flush(std::cerr);

  blockBlob.download_to_stream(output_stream);

  std::cerr << "Finished download" << std::endl;
  std::flush(std::cerr);

  // Open the temp file for download
  char tmpFile[] = "/mnt/resource/w2oTemp.XXXXXX";
  int outfile = mkstemp(tmpFile);
  std::vector<unsigned char>& data = buffer.collection();

  std::cerr << "Writing data to tempfile" << std::endl;
  std::flush(std::cerr);
  write(outfile, (char *)&data[0], buffer.size());
  close(outfile);

  std::cerr << "Finished writing data to tempfile" << std::endl;
  std::flush(std::cerr);

  // Open the tempfile for conversion
  orc::ReaderOptions opts;
  std::unique_ptr<orc::Reader> reader;
  try {
    std::cerr << "Creating ORC reader" << std::endl;
    std::flush(std::cerr);
    reader = orc::createReader(orc::readLocalFile(std::string(tmpFile)), opts);
  } 
  catch (std::exception& ex) {
    std::cerr << "Caught exception: " << ex.what() << "\n";
    return 1;
  }
  std::cerr << "Created ORC reader" << std::endl;
  std::flush(std::cerr);

  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1000);
  std::string line;
  std::unique_ptr<orc::ColumnPrinter> printer =
          orc::createCustomColumnPrinter(line, &reader->getSelectedType());
  unsigned long rows = 0;
  unsigned long batches = 0;
  while (reader->next(*batch)) {
    batches += 1;
    rows += batch->numElements;

    printer->reset(*batch);
    for(unsigned long i=0; i < batch->numElements; ++i) {
      line.clear();
      printer->printRow(i);
      line += "\n";
      const char* str = line.c_str();
      fwrite(str, 1, strlen(str), stdout);
    }
  }

  return 0;
}
