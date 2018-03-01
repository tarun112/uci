#include <cstdio>
#include <iostream>
#include <fstream>
#include <cerrno>
#include <cstring>
#include <cmath>
#include <sys/stat.h>
#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    struct stat stFileInfo;
    if(stat(fileName.c_str(), &stFileInfo) == 0) 
        return -1;


    FILE* fp = fopen(fileName.c_str(), "a+b");
    if(fp == NULL)
        return -1;
    fclose(fp);

    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if(remove(fileName.c_str()) != 0)
        return -1;    
    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return fileHandle.openFile(fileName);
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return fileHandle.closeFile();
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    _fp = NULL;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if(NULL  == _fp)
        return -1;
    size_t pgoffset = (pageNum + DATA_PAGE_OFFSET)*PAGE_SIZE;
    rewind(_fp);
    if( fseek(_fp, pgoffset, SEEK_SET) ) return -1;
    if (PAGE_SIZE != fread(data, 1, PAGE_SIZE, _fp)) return -1;
    readPageCounter++;

    int stat = updataHeaderPage();
    if (stat) return stat;

    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if(NULL  == _fp)
        return -1;
//    if(pageNum > appendPageCounter)
//        return -1;
    if (pageNum >= getNumberOfPages())
        return -1;
    size_t pgoffset = (pageNum + DATA_PAGE_OFFSET)*PAGE_SIZE;
    rewind(_fp);
    if( fseek(_fp, pgoffset, SEEK_SET) ) return -1;
    if (PAGE_SIZE != fwrite(data, 1, PAGE_SIZE, _fp)) return -1;
    fflush(_fp);
    writePageCounter++;

    int stat = updataHeaderPage();
    if (stat) return stat;

    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    if(NULL  == _fp)
        return -1;
    if( fseek(_fp, 0, SEEK_END) ) return -1;
    if (PAGE_SIZE != fwrite(data, 1, PAGE_SIZE, _fp)) return -1;
    fflush(_fp);
    appendPageCounter++;

    int stat = updataHeaderPage();
    if (stat) return stat;

    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
//    if (NULL == _fp)
//        return -1;
//    if (fseek(_fp, 0, SEEK_END)) return -1;
////    fseek(fp, 0L, SEEK_END);
//    long sz = ftell(_fp);
//    return ceil(sz /(double) PAGE_SIZE);
    int stat = 0;
    stat = readCountersFromFile();
    if (stat) return stat;
    
    return appendPageCounter;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    int stat = readCountersFromFile();
    if (stat) return stat;

    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;

    return 0;
}


int FileHandle::openFile(const string &fname)
{
    struct stat stFileInfo;
    if(stat(fname.c_str(), &stFileInfo) != 0) {
        _fp = fopen(fname.c_str(), "wb");
        if(_fp == NULL)
            return -1;
        fclose(_fp);
     }

    _fp = fopen(fname.c_str(), "r+b");
    if(_fp == NULL)
        return -1;

    int stat = 0;
    if (0 == getTotalNumberOfPages()) {
        stat = initFile();
        if (stat) return stat;
    }
    else {
        stat = readCountersFromFile();
        if (stat) return stat;
    }

    return 0;
}

int FileHandle::closeFile()
{
    int stat = updataHeaderPage();
    if (stat) return stat;

    fclose(_fp);
    _fp = NULL;

    return 0;
}

int FileHandle::getTotalNumberOfPages()
{
    if (NULL == _fp)
        return -1;
    if (fseek(_fp, 0, SEEK_END)) return -1;
    //    fseek(fp, 0L, SEEK_END);
    long sz = ftell(_fp);
    return ceil(sz / (double)PAGE_SIZE);
}

int FileHandle::updataHeaderPage()
{
    if (NULL == _fp)
        return -1;

    void* data = malloc(PAGE_SIZE);
    int offset = 0;
    memcpy((char*)data + offset, (char*)&readPageCounter, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)data + offset, (char*)&writePageCounter, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)data + offset, (char*)&appendPageCounter, sizeof(int));

    rewind(_fp);
    if (fseek(_fp, HEADER_PAGE_OFFSET, SEEK_SET)) return -1;
    if (PAGE_SIZE != fwrite(data, 1, PAGE_SIZE, _fp)) return -1;
    fflush(_fp);

    free(data);

    return 0;

}

int FileHandle::readCountersFromFile()
{
    if (NULL == _fp)
        return -1;

    void* data = malloc(PAGE_SIZE);

    rewind(_fp);
    if (fseek(_fp, HEADER_PAGE_OFFSET, SEEK_SET)) return -1;
    if (PAGE_SIZE != fread(data, 1, PAGE_SIZE, _fp)) return -1;

    int offset = 0;
    memcpy((char*)&readPageCounter, (char*)data + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)&writePageCounter, (char*)data + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)&appendPageCounter, (char*)data + offset, sizeof(int));

    free(data);

    return 0;

}

int FileHandle::initFile()
{
    if (NULL == _fp)
        return -1;

    return updataHeaderPage();

}
