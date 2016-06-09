// Windows Azure Storage bits
#include "was/storage_account.h"
#include "was/blob.h"

#include <stdio.h>
#include <iostream>
#include <streambuf>

int usage() {
  std::cerr << "\n wasb2orc takes three arguments: " ;
  std::cerr << "\n -a <AZURE ACCOUNT CONNECTION STRING>" ;
  std::cerr << "\n\te.g. \"DefaultEndpointsProtocol=https\\;AccountName=someOrgInsightData\\;AccountKey=...\"" ;
  std::cerr << "\n -c <AZURE STORAGE CONTAINER NAME>" ;
  std::cerr << "\n -b <AZURE BLOB FILENAME>" ;
  std::cerr << "\n" ;

  return -1;
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

  for (index = optind; index < argc; index++)
    printf ("Non-option argument %s\n", argv[index]);

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
  //concurrency::streams::container_buffer<std::vector<uint8_t>> buffer;
  //concurrency::streams::stringstreambuf buffer;
  //concurrency::streams::ostream output_stream(buffer);

  std::cerr << "Starting download" << std::endl; 
  std::flush(std::cerr);

  //blockBlob.download_to_stream(output_stream);
  blockBlob.download_to_file(U("/dev/stdout"));

  std::cerr << "Finished download" << std::endl;
  std::flush(std::cerr);

  //std::vector<unsigned char>& data = buffer.collection();

  std::cerr << "Writing data to stdout" << std::endl;
  std::flush(std::cerr);
  //write(fileno(stdout), (char *)&data[0], buffer.size());
  //std::cout << buffer.collection() << std::endl;

  std::cerr << "Finished writing data to stdout" << std::endl;
  std::flush(std::cerr);
 
  return 0;
}
