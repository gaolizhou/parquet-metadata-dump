#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <cstring>
#include <memory>
#include <boost/shared_ptr.hpp>
#include <thrift/TApplicationException.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <gen-cpp/parquet_types.h>

static constexpr int64_t DEFAULT_FOOTER_READ_SIZE = 64 * 1024;
static constexpr uint32_t FOOTER_SIZE = 8;
static constexpr uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

template <class T>
inline void DeserializeThriftMsg(const uint8_t* buf, uint32_t* len, T* deserialized_msg) {
  // Deserialize msg bytes into c++ thrift msg using memory transport.
  std::shared_ptr<apache::thrift::transport::TMemoryBuffer> tmem_transport(
      new apache::thrift::transport::TMemoryBuffer(const_cast<uint8_t*>(buf), *len));
  apache::thrift::protocol::TCompactProtocolFactoryT<
      apache::thrift::transport::TMemoryBuffer>
      tproto_factory;
  std::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
      tproto_factory.getProtocol(tmem_transport);
  try {
    deserialized_msg->read(tproto.get());
  } catch (std::exception& e) {
    std::stringstream ss;
    ss << "Couldn't deserialize thrift: " << e.what() << "\n";
    throw std::runtime_error(ss.str());
  }
  uint32_t bytes_left = tmem_transport->available_read();
  *len = *len - bytes_left;
}


int main(int argc, char *argv[]) {
try {
  std::ifstream ifs("1.parquet", std::ios::in|std::ifstream::binary);
  ifs.seekg (0, std::ios::end);
  int64_t file_size = ifs.tellg();
  ifs.seekg (0, std::ios::beg);

  if (file_size < FOOTER_SIZE) {
    throw std::runtime_error("Corrupted file, smaller than file footer");
  }

  uint8_t footer_buffer[DEFAULT_FOOTER_READ_SIZE];
  int64_t footer_read_size = std::min(file_size, DEFAULT_FOOTER_READ_SIZE);
  ifs.seekg(file_size - footer_read_size, std::ios::beg);
  ifs.read(reinterpret_cast<char*>(footer_buffer), footer_read_size);
  if (memcmp(footer_buffer + footer_read_size - 4, PARQUET_MAGIC, 4) != 0) {
    throw std::runtime_error("Invalid parquet file. Corrupt footer.");
  }

  uint32_t metadata_len =
      *reinterpret_cast<uint32_t*>(footer_buffer + footer_read_size - FOOTER_SIZE);
  int64_t metadata_start = file_size - FOOTER_SIZE - metadata_len;
  if (FOOTER_SIZE + metadata_len > file_size) {
    throw std::runtime_error(
        "Invalid parquet file. File is less than "
            "file metadata size.");
  }

  std::shared_ptr<char> metadata_buffer = std::shared_ptr<char>(new char[metadata_len], std::default_delete<char[]>());
  if (footer_read_size >= (metadata_len + FOOTER_SIZE)) {
    memcpy(metadata_buffer.get(),
           footer_buffer + (footer_read_size - metadata_len - FOOTER_SIZE),
           metadata_len);
  } else {
    ifs.seekg(metadata_start, std::ios::beg);
    ifs.read(metadata_buffer.get(), metadata_len);
  }
  std::unique_ptr<parquet::format::FileMetaData> meta_thrift(new parquet::format::FileMetaData);
  DeserializeThriftMsg(reinterpret_cast<uint8_t *>(metadata_buffer.get()), &metadata_len, meta_thrift.get());
  meta_thrift->printTo(std::cout);
} catch (std::exception &e) {
  std::cout << e.what() << std::endl;
}


  return 0;
}