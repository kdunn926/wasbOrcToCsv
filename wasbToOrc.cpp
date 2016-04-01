// Windows Azure Storage bits
#include "was/storage_account.h"
#include "was/blob.h"

// Apache ORC bits
#include "orc/ColumnPrinter.hh"
#include "Exceptions.hh"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <memory>
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {

  // Define the connection-string with your values.
  const utility::string_t storage_connection_string(U("DefaultEndpointsProtocol=https;AccountName=;AccountKey="));

  // Retrieve storage account from connection string.
  azure::storage::cloud_storage_account storage_account = azure::storage::cloud_storage_account::parse(storage_connection_string);

  // Create the blob client.
  azure::storage::cloud_blob_client blob_client = storage_account.create_cloud_blob_client();

  // Retrieve a reference to a previously created container.
  azure::storage::cloud_blob_container container = blob_client.get_container_reference(U("kdunn-test"));

  // Retrieve reference to a blob named "my-blob-1".
  azure::storage::cloud_block_blob blockBlob = container.get_block_blob_reference(U("Orders/000001_0"));

  // Save blob contents to a file.
  concurrency::streams::container_buffer<std::vector<uint8_t>> buffer;
  concurrency::streams::ostream output_stream(buffer);

  /* \brief Downloads a range of bytes in a blob to a stream.
   *
   * \param target The target stream.
   * \param offset The offset at which to begin downloading the blob, in bytes.
   * \param length The length of the data to download from the blob, in bytes.
   * \param condition An "azure::storage::access_condition" object that represents the access condition for the operation.
   * \param options An "azure::storage::blob_request_options" object that specifies additional options for the request.
   * \param context An "azure::storage::operation_context" object that represents the context for the current operation.
   */
  //blockBlob.download_range_to_stream(output_stream, 0 /*offset*/, blockBlob.properties().size() /*length*/,
  //                                   azure::storage::access_condition(),
  //                                   azure::storage::blob_request_options(),
  //                                   azure::storage::operation_context());

  /* \brief Intitiates an asynchronous operation to download a range of bytes in a blob to a stream.
   *
   * \param target: The target stream.
   * \param offset: The offset at which to begin downloading the blob, in bytes.
   * \param length: The length of the data to download from the blob, in bytes.
   * \returns: object that represents the current operation.
   */
  //blockBlob.download_range_to_stream_async(concurrency::streams::ostream target,
  //                                         int64_t offset,
  //                                         int64_t length);


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
  try{
    std::cerr << "Creating ORC reader" << std::endl;
    std::flush(std::cerr);
    reader = orc::createReader(orc::readLocalFile(std::string(tmpFile)), opts);
  } catch (std::exception& ex) {
    std::cerr << "Caught exception: " << ex.what() << "\n";
    return 1;
  }
  std::cerr << "Created ORC reader" << std::endl;
  std::flush(std::cerr);

  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1000);
  std::string line;
  std::unique_ptr<orc::ColumnPrinter> printer =
  createColumnPrinter(line, &reader->getSelectedType());
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
  //std::cout << "Rows: " << rows << std::endl;
  //std::cout << "Batches: " << batches << std::endl;
  return 0;
}
