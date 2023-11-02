module;

#include "faiss/impl/FaissException.h"
#include "faiss/impl/io.h"
#include "faiss/index_io.h"

import stl;
import infinity_exception;
import infinity_assert;
import file_system;
import third_party;

module faiss_index_file_worker;

namespace infinity {

FaissIndexFileWorker::~FaissIndexFileWorker() {
    if (data_ != nullptr) {
        FreeInMemory();
        data_ = nullptr;
    }
}

struct FSIOReader : faiss::IOReader {
    FSIOReader(FileHandler *file_handler) : file_handler_(file_handler) {}
    size_t operator()(void *ptr, size_t size, size_t nitems) override {
        file_handler_->Read(ptr, size * nitems);
        return nitems;
    }
    FileHandler *file_handler_;
};

struct FSIOWriter : faiss::IOWriter {
    FSIOWriter(FileHandler *file_handler) : file_handler_(file_handler) {}
    size_t operator()(const void *ptr, size_t size, size_t nitems) override {
        file_handler_->Write(ptr, size * nitems);
        return nitems;
    }
    FileHandler *file_handler_;
};

void FaissIndexFileWorker::AllocateInMemory() {
    if (data_) {
        Error<StorageException>("Data is already allocated.", __FILE_NAME__, __LINE__);
    }
    auto faiss_index_ptr = new FaissIndexPtr(nullptr, nullptr);
    data_ = static_cast<void *>(faiss_index_ptr);
}

void FaissIndexFileWorker::FreeInMemory() {
    if (!data_) {
        Error<StorageException>("Data is not allocated.", __FILE_NAME__, __LINE__);
    }
    auto faiss_index_ptr = static_cast<FaissIndexPtr *>(data_);
    delete faiss_index_ptr->index_;
    delete faiss_index_ptr->quantizer_;
    delete faiss_index_ptr;
    data_ = nullptr;
}

void FaissIndexFileWorker::WriteToFileImpl(bool &prepare_success) {
    try {
        auto faiss_index_ptr = static_cast<FaissIndexPtr *>(data_);
        FSIOWriter writer(file_handler_.get());
        faiss::write_index(faiss_index_ptr->index_, &writer);
        prepare_success = true; // Not run defer_fn
    } catch (faiss::FaissException &xcp) {
        Error<StorageException>(Format("Faiss exception: {}", xcp.what()), __FILE_NAME__, __LINE__);
    }
}

void FaissIndexFileWorker::ReadFromFileImpl() {
    FSIOReader reader(file_handler_.get());
    auto index = faiss::read_index(&reader);
    data_ = static_cast<void *>(new FaissIndexPtr(index, nullptr));
}
} // namespace infinity