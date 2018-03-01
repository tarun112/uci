#include <cstdio>
#include <cstring>
#include <iostream>
#include <cmath>
#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance()->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}

int getDataSize(const vector<Attribute> &recordDescriptor, const void* data) 
{
    int nfields = recordDescriptor.size();
    int nnullbytes = ceil(nfields/8.0);
    int offset = nnullbytes;
    unsigned char* nullbytes = new unsigned char[nnullbytes];
    memcpy(nullbytes, data, nnullbytes*sizeof(char));
    for(int i = 0; i< nfields; ++i) {
        if((nullbytes[i/8] >> (7-i%8)) & 1)
            continue;
        const Attribute& atrb = recordDescriptor[i];
        if(atrb.type == TypeVarChar) {
            int length;
            memcpy(&length, (char*)data+offset, sizeof(int));
            offset += length;
        }
        offset += 4;        
    }
    delete[] nullbytes;
    return offset;
}

int getNumSlots(const void* data)
{
    int nslots=0;
    int nspos = PAGE_SIZE-sizeof(int);
    memcpy((char*)&nslots, (char*)data + nspos, sizeof(int));
    return nslots;    
}

int getSlotInfo(const void* data, int slotNum, int &slotoffset, int &slotlen) 
{
    int nslots = getNumSlots(data);
    int slotnum = slotNum+1;
    memcpy(&slotoffset, (char*)data+(PAGE_SIZE-sizeof(int)-2*slotnum*sizeof(int)), sizeof(int));
    memcpy(&slotlen, (char*)data+(PAGE_SIZE-2*slotnum*sizeof(int)), sizeof(int));
    return 0;
}

int getLastSlotInfo(const void* data, int &slotoffset, int &slotlen)
{
    int nslots = getNumSlots(data);
    slotoffset = -1;
    slotlen = -1;
    int i = 0;
    while (slotoffset == -1 || slotlen == -1) {
        getSlotInfo(data, nslots - 1 -i, slotoffset, slotlen);
        ++i;
    }
    return 0;
}


int getNextFreeSlot(void* data)
{
    int nslots = getNumSlots(data);
    for(int i=0; i<nslots; ++i) {
        int slotoffset=-2, slotlen=-2;
        getSlotInfo(data, i, slotoffset, slotlen);
        if(slotoffset == -1 && slotlen == -1 ){
            return i;
        }
    }

    return nslots;
}

int getFreeSpaceOnPage(void* data)
{
    int nslots = getNumSlots(data);
    int lsoff, lslen;
    getLastSlotInfo(data, lsoff, lslen);
    return (PAGE_SIZE - (lsoff+lslen) - 2*sizeof(int)*nslots-sizeof(int));
}

int checkPageFreeSlot(FileHandle &fileHandle, int pageNum, int datasize, RID &rid, int &offset) 
{
    void* data = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum,data);
    int nslots = getNumSlots(data);
    // int nspos = PAGE_SIZE-sizeof(int);
    // memcpy(&nslots, (char*)data + nspos, sizeof(int));
    // int lsopos = nspos - nslots*2*sizeof(int);
    // int lslpos = lsopos + sizeof(int);
    // memcpy(&lastslotoffset, (char*)data + lsopos, sizeof(int));
    // memcpy(&lastslotlength, (char*)data + lslpos, sizeof(int));
    int freespace = getFreeSpaceOnPage(data);
    if(datasize + 2*sizeof(int) <= freespace) {
        int slotNum = getNextFreeSlot(data);
        rid.pageNum = pageNum;
        rid.slotNum = slotNum;
        if(slotNum == 0)
            offset = 0;
        else if (slotNum == nslots) {
            int lastslotoffset, lastslotlen;
            getLastSlotInfo(data, lastslotoffset, lastslotlen);
            offset = lastslotoffset + lastslotlen;
        } else {
            int i = 0;
            int lastslotoffset = -1, lastslotlen = -1;
            while (lastslotoffset == -1 || lastslotlen == -1) {
                getSlotInfo(data, slotNum - 1-i, lastslotoffset, lastslotlen);
                ++i;
            }
            offset = lastslotoffset+lastslotlen;
        }
        free(data);
        return 0;
    } else {
        free(data);
        return -1;
    }
}

int getNextSlot(FileHandle &fileHandle, int datasize, RID &rid, int &offset) {
    int npages = fileHandle.getNumberOfPages();
    if(0 != npages) {
        if( 0 == checkPageFreeSlot(fileHandle, npages-1, datasize, rid, offset) )
            return 0;
        else {
            for(int i=0; i<npages-1; ++i) {
                if( 0 == checkPageFreeSlot(fileHandle, i, datasize, rid, offset) )
                    return 0;                
            }
        }
    }

    void* data = malloc(PAGE_SIZE);
    int nslots = 0;
    memcpy((char*)data+(PAGE_SIZE-sizeof(int)), (char*)&nslots, sizeof(int));
    fileHandle.appendPage(data);
    rid.pageNum = fileHandle.getNumberOfPages()-1;
    rid.slotNum = 0;
    offset=0;
    free(data);
    return 0;
}

int insertBlock(void* data, int blockoffset, int blocksize)
{
    int nslots = getNumSlots(data);
    int lastslotoffset=-1, lastslotlength=-1;
    getLastSlotInfo(data, lastslotoffset, lastslotlength);   
    int movesize = lastslotoffset + lastslotlength - blockoffset;
    memmove((char*)data+blockoffset+blocksize, (char*)data+blockoffset, movesize);
    return 0;
}

int updateAllSlotsOffset(void* data,int slotoffset, int slotlen, int slotNum)
{
    int nslots = getNumSlots(data);
    for(int i=slotNum; i<nslots; ++i ) {
        int slotnum = i+1;
        int soffset;
        memcpy(&soffset, (char*)data+(PAGE_SIZE-sizeof(int)-2*slotnum*sizeof(int)), sizeof(int));
        if (soffset != -1) {
            soffset -= slotlen;
            memcpy((char*)data + (PAGE_SIZE - sizeof(int) - 2 * slotnum * sizeof(int)), (char*)&soffset, sizeof(int));
        }
    }   
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    int datasize = getDataSize(recordDescriptor, data);
//    cout << "debug: datasize:" << datasize << endl;
    int offset = 0;
    getNextSlot(fileHandle, datasize, rid, offset);
    void* buffer = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, buffer);
    unsigned nslots = getNumSlots(buffer);
    if((rid.slotNum == 0 && nslots > 0) ||(rid.slotNum != 0 && rid.slotNum < nslots)) {
        insertBlock(buffer, offset, datasize);       
        updateAllSlotsOffset(buffer, offset, -datasize, rid.slotNum+1);
    } else if (rid.slotNum == nslots) {
        nslots++;
    }
    
    memcpy((char*)buffer+offset, data, datasize);
    int slotoffset = offset;
    int slotlen = datasize;
    int slotnum = rid.slotNum+1;
    memcpy((char*)buffer+(PAGE_SIZE-sizeof(int)), (char*)&nslots, sizeof(int) );
    memcpy((char*)buffer+(PAGE_SIZE-2*sizeof(int)*slotnum-sizeof(int)), (char*)&slotoffset, sizeof(int) );
    memcpy((char*)buffer+(PAGE_SIZE-2*sizeof(int)*slotnum), (char*)&slotlen, sizeof(int) );

    fileHandle.writePage(rid.pageNum, buffer);
 //   cout << "rid:" << rid.pageNum << "," << rid.slotNum << endl;
    free(buffer);
    return 0;
}

bool isTombstone(const void* buffer, int offset, RID &trid) {
    int tombstone = 0;
    memcpy(&tombstone, (char*)buffer + offset, sizeof(int));
    if (tombstone == -1) {
        memcpy((char*)&trid.pageNum, (char*)buffer + offset+sizeof(int), sizeof(unsigned));
        memcpy((char*)&trid.slotNum, (char*)buffer + offset+2*sizeof(int), sizeof(unsigned));
        return true;
    }
    return false;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    void* buffer = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, buffer);
    int slotoffset, slotlen;
    getSlotInfo(buffer, rid.slotNum, slotoffset, slotlen);
    if(slotoffset == -1 || slotlen == -1) {
        free(buffer);
        return -1;
    }
    RC ret = 0;
    RID trid;
    if(isTombstone(buffer, slotoffset, trid))
        ret = readRecord(fileHandle, recordDescriptor, trid, data);
    else 
        memcpy((char*)data, (char*)buffer + slotoffset, slotlen);
    
    free(buffer);
    return ret;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    int nfields = recordDescriptor.size();
    int nnullbytes = ceil(nfields/8.0);
    int offset = nnullbytes;
    unsigned char* nullbytes = new unsigned char[nnullbytes];
    memcpy(nullbytes, data, nnullbytes*sizeof(char));
    for(int i = 0; i< nfields; ++i) {
        const Attribute& atrb = recordDescriptor[i];
        if((nullbytes[i/8] >> (7-i%8)) & 1) {
            cout << atrb.name << ":NULL ";
            continue;
        }
        if(atrb.type == TypeVarChar) {
            int length;
            memcpy(&length, (char*)data+offset, sizeof(int));
//            cout << "length:" << length << endl;
            offset += 4;
            char* val = new char[length+1];
            memcpy(val, (char*)data+offset, length);
            val[length] = '\0';
            offset += length;
            cout << atrb.name << ":" << val << " ";
            delete[] val;
        } else if(atrb.type == TypeInt) {
            int val;
            memcpy(&val, (char*)data+offset, sizeof(int));
            offset += sizeof(int);
            cout << atrb.name << ":" << val << " ";
        } else if(atrb.type == TypeReal) {
            float val;
            memcpy(&val, (char*)data+offset, sizeof(float));
            offset += sizeof(float);
            cout << atrb.name << ":" << val << " ";
        }
    }
    cout << endl;
   
    delete[] nullbytes;
    return 0;
}

int removeBlock(void* data, int blockoffset, int blocksize)
{
    int nslots=getNumSlots(data);
    int lastslotoffset = -1, lastslotlength = -1;
    getLastSlotInfo(data, lastslotoffset, lastslotlength);
    int movesize = lastslotoffset + lastslotlength - blockoffset - blocksize;
    memmove((char*)data+blockoffset, (char*)data+blockoffset+blocksize, movesize);
    return 0;
}


RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    void* buffer = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, buffer);
    int slotoffset, slotlen;
    getSlotInfo(buffer, rid.slotNum, slotoffset, slotlen);
    if (slotoffset == -1 || slotlen == -1) {
        free(buffer);
        return -1;
    }

    RID trid;
    if (isTombstone(buffer, slotoffset, trid)) {
        RC stat = deleteRecord(fileHandle, recordDescriptor, trid);
        if (stat) return stat;
    }

    //   cout << "debug:" << slotoffset << " " << slotlen << endl;
    removeBlock(buffer, slotoffset, slotlen);
    int soffset = -1;
    int slen=-1;
    int slotnum = rid.slotNum+1;
    memcpy((char*)buffer+(PAGE_SIZE-sizeof(int)-2*slotnum*sizeof(int)),(char*)&soffset, sizeof(int));
    memcpy((char*)buffer+(PAGE_SIZE-2*slotnum*sizeof(int)), (char*)&slen, sizeof(int));
    updateAllSlotsOffset(buffer, slotoffset, slotlen, rid.slotNum+1);
    // slotoffset = -1;
    // slotlen=-1;
    // memcpy((char*)buffer+(PAGE_SIZE-sizeof(int)-2*slotnum*sizeof(int)),(char*)&slotoffset, sizeof(int));
    // memcpy((char*)buffer+(PAGE_SIZE-2*slotnum*sizeof(int)), (char*)&slotlen, sizeof(int));
    
    fileHandle.writePage(rid.pageNum, buffer);
    free(buffer);
    return 0;
}

int updateBlock(void* data, int blockoffset, int blocksize, int newblocksize)
{
    int nslots = getNumSlots(data);
    int lastslotoffset, lastslotlength;
    getLastSlotInfo(data, lastslotoffset, lastslotlength);   
    int movesize = lastslotoffset + lastslotlength - (blockoffset + blocksize);
    memmove((char*)data+blockoffset+newblocksize, (char*)data+blockoffset+blocksize, movesize);
    return 0;    
}

//int createTombStoneDescriptor(vector<Attribute> &tsDesc) {
//    Attribute att1;
//    att1.name = "PageNo";
//    att1.type = AttrType::TypeInt;
//    att1.length = sizeof(int);
//    tsDesc.push_back(att1);
//    
//    att1.name = "PageOffset";
//    att1.type = AttrType::TypeInt;
//    att1.length = sizeof(int);
//    tsDesc.push_back(att1);
//    return 0;
//}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    void* buffer = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, buffer);
    int soffset, slen;
    getSlotInfo(buffer, rid.slotNum, soffset, slen);
    if(soffset == -1 || slen == -1) {
        free(buffer);
        return -1;
    }

    RID trid;
    if (isTombstone(buffer, soffset, trid)) {
        RC stat = updateRecord(fileHandle, recordDescriptor, data, trid);
        if (stat) return stat;
    }

    int datasize = getDataSize(recordDescriptor, data);
    int freespace = getFreeSpaceOnPage(buffer);
    if(datasize+2*sizeof(int) <= freespace) {
        updateBlock(buffer, soffset, slen, datasize);
        memcpy((char*)buffer+soffset, data, datasize);
        int slotnum = rid.slotNum+1;
        memcpy((char*)buffer+(PAGE_SIZE-2*sizeof(int)*slotnum), (char*)&datasize, sizeof(int) );
        updateAllSlotsOffset(buffer, soffset, -(datasize-slen), rid.slotNum+1);
        fileHandle.writePage(rid.pageNum, buffer);
        free(buffer);
        return 0;
    } else {
        RID trid;
        insertRecord(fileHandle, recordDescriptor, data, trid) ;
        int tombstonesize = sizeof(int) + 2 * sizeof(unsigned) ;
        updateBlock(buffer, soffset, slen, tombstonesize);
        int tombstone = -1;
        memcpy((char*)buffer + soffset, (char*)&tombstone, sizeof(int));
        memcpy((char*)buffer + soffset + sizeof(int), (char*)&trid.pageNum, sizeof(unsigned));
        memcpy((char*)buffer + soffset+ 2*sizeof(int), (char*)&trid.slotNum, sizeof(unsigned));
        int slotnum = rid.slotNum + 1;
        memcpy((char*)buffer + (PAGE_SIZE - 2 * sizeof(int)*slotnum), (char*)&tombstonesize, sizeof(int));
        updateAllSlotsOffset(buffer, soffset, -(tombstonesize - slen), rid.slotNum + 1);
        fileHandle.writePage(rid.pageNum, buffer);
        free(buffer);
        return 0;
    }
    
    free(buffer);
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{

    void* buffer = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, buffer);
    int slotoffset, slotlen;
    getSlotInfo(buffer, rid.slotNum, slotoffset, slotlen);
    if(slotoffset == -1 || slotlen == -1) {
        free(buffer);
        return -1;
    }

    RID trid;
    if (isTombstone(buffer, slotoffset, trid)) {
        return readAttribute(fileHandle, recordDescriptor, trid, attributeName, data);
    }

    char atbnullbytes;
    memset(&atbnullbytes, 0, 1);

    void* rbuff = malloc(slotlen+1);
    memcpy((char*)rbuff, (char*)buffer+slotoffset, slotlen);
    free(buffer);

    int nfields = recordDescriptor.size();
    int nnullbytes = ceil(nfields/8.0);
    int offset = nnullbytes;
    unsigned char* nullbytes = new unsigned char[nnullbytes];
    memcpy(nullbytes, rbuff, nnullbytes*sizeof(char));
    for(int i = 0; i< nfields; ++i) {
        const Attribute& atrb = recordDescriptor[i];
        if(attributeName == atrb.name){
            if((nullbytes[i/8] >> (7-i%8)) & 1) {
                atbnullbytes = 1 << 7 ;
                memcpy((char*)data, &atbnullbytes, sizeof(char));
                return 0;
            }
            if(atrb.type == TypeVarChar) {
                int length;
                memcpy(&length, (char*)rbuff+offset, sizeof(int));
                offset += 4;
                memcpy((char*)data + 1, (char*)&length, sizeof(int));
                memcpy((char*)data + 5, (char*)rbuff+offset, length);
            } else if(atrb.type == TypeInt) {
                memcpy((char*)data+1, (char*)rbuff+offset, sizeof(int));
            } else if(atrb.type == TypeReal) {
                memcpy((char*)data+1, (char*)rbuff+offset, sizeof(float));
            }
            memcpy((char*)data, &atbnullbytes, sizeof(char));
            return 0;
        }
        if((nullbytes[i/8] >> (7-i%8)) & 1) {
            continue;
        }
        if(atrb.type == TypeVarChar) {
            int length;
            memcpy(&length, (char*)rbuff+offset, sizeof(int));
            offset += 4;
            offset += length;
        } else if(atrb.type == TypeInt) {
            offset += sizeof(int);
        } else if(atrb.type == TypeReal) {
            offset += sizeof(float);
        }
    }
  
    free(rbuff);
    delete[] nullbytes;
    return 0;
}

// Scan returns an iterator to allow the caller to go through the results one by one. 
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator) 
{
    rbfm_ScanIterator.rbfm = this;
    rbfm_ScanIterator.fileHandle = fileHandle;
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.conditionAttribute = conditionAttribute;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = (void*)value;
    rbfm_ScanIterator.attributeNames = attributeNames;
    rbfm_ScanIterator.curPage = malloc(PAGE_SIZE);
    rbfm_ScanIterator.curRid.pageNum = -1;
    rbfm_ScanIterator.curRid.slotNum = -1;
    rbfm_ScanIterator.curPageTotalSlots = -1;

    return 0;
}

int RBFM_ScanIterator::prepareOutputRecord(const void* rbuff, void* data) {

    int natb = attributeNames.size();
    int natbnullbytes = ceil(natb / 8.0);
    unsigned char* atbnullbytes = new unsigned char[natbnullbytes];
    memset(atbnullbytes, 0, natbnullbytes);
    int atboffset = natbnullbytes;

    int nfields = recordDescriptor.size();
    int nnullbytes = ceil(nfields / 8.0);
    int offset = nnullbytes;
    unsigned char* nullbytes = new unsigned char[nnullbytes];
    memcpy(nullbytes, rbuff, nnullbytes * sizeof(char));
    int j = 0;
    for (int i = 0; i< nfields; ++i) {
        const Attribute& atrb = recordDescriptor[i];
        string atbname = attributeNames[j];
        if (atbname == atrb.name) {
            if ((nullbytes[i / 8] >> (7 - i % 8)) & 1) {
                atbnullbytes[j / 8] |= 1 << (7 - j % 8);
                j++;
                continue;
            }
            if (atrb.type == TypeVarChar) {
                int length;
                memcpy(&length, (char*)rbuff + offset, sizeof(int));
                memcpy((char*)data+atboffset, (char*)&length, sizeof(int));
                offset += sizeof(int);
                atboffset += sizeof(int);
                memcpy((char*)data+atboffset, (char*)rbuff + offset, length);
                offset += length;
                atboffset += length;
            }
            else if (atrb.type == TypeInt) {
                memcpy((char*)data+atboffset, (char*)rbuff + offset, sizeof(int));
                offset += sizeof(int);
                atboffset += sizeof(int);
            }
            else if (atrb.type == TypeReal) {
                memcpy((char*)data + atboffset, (char*)rbuff + offset, sizeof(float));
                offset += sizeof(int);
                atboffset += sizeof(int);
            }
            j++;
            if (j >= natb)
                break;
            continue;;
        }
        if ((nullbytes[i / 8] >> (7 - i % 8)) & 1) {
            continue;
        }
        if (atrb.type == TypeVarChar) {
            int length;
            memcpy(&length, (char*)rbuff + offset, sizeof(int));
            offset += 4;
            offset += length;
        }
        else if (atrb.type == TypeInt) {
            offset += sizeof(int);
        }
        else if (atrb.type == TypeReal) {
            offset += sizeof(float);
        }
    }

    memcpy((char*)data, (char*)atbnullbytes, natbnullbytes *sizeof(char));

    delete[] nullbytes;
    delete[] atbnullbytes;
    return 0;

}

bool RBFM_ScanIterator::checkCondition(const void* rbuff) {

    CompOp compRes = CompOp::NO_OP;

    int nfields = recordDescriptor.size();
    int nnullbytes = ceil(nfields / 8.0);
    int offset = nnullbytes;
    unsigned char* nullbytes = new unsigned char[nnullbytes];
    memcpy(nullbytes, rbuff, nnullbytes * sizeof(char));
    for (int i = 0; i< nfields; ++i) {
        const Attribute& atrb = recordDescriptor[i];
        if (conditionAttribute == atrb.name) {
            if ((nullbytes[i / 8] >> (7 - i % 8)) & 1) {
                return false;
            }
            if (atrb.type == TypeVarChar) {
                int length;
                memcpy(&length, (char*)rbuff + offset, sizeof(int));
                offset += 4;
                char* buff = new char[length + 1];
                memcpy((char*)buff, (char*)rbuff + offset, length);
                buff[length] = '\0';
                int clen;
                memcpy(&clen, (char*)value, sizeof(int));
                char* cbuff = new char[clen + 1];
                memcpy((char*)cbuff, (char*)value + sizeof(int), clen);
                cbuff[clen] = '\0';
                int res = strcmp(buff, cbuff);
                if (res < 0)
                    compRes = LT_OP;
                else if (res == 0)
                    compRes = EQ_OP;
                else 
                    compRes = GT_OP;
                delete[] buff;
                delete[] cbuff;
            }
            else if (atrb.type == TypeInt) {
                int val;
                memcpy((char*)&val, (char*)rbuff + offset, sizeof(int));
                int cval;
                memcpy((char*)&cval, (char*)value, sizeof(int));
                if (val < cval)
                    compRes = LT_OP;
                else if (val == cval)
                    compRes = EQ_OP;
                else
                    compRes = GT_OP;
            }
            else if (atrb.type == TypeReal) {
                float val;
                memcpy((char*)&val, (char*)rbuff + offset, sizeof(float));
                int cval;
                memcpy((char*)&cval, (char*)value, sizeof(float));
                if (val < cval)
                    compRes = LT_OP;
                else if (val == cval)
                    compRes = EQ_OP;
                else
                    compRes = GT_OP;
            }

            if (compRes == EQ_OP && (compOp == EQ_OP || compOp == LE_OP || compOp == GE_OP))
                return true;
            else if (compRes == LT_OP && (compOp == LT_OP || compOp == LE_OP))
                return true;
            else if (compRes == GT_OP && (compOp == GT_OP || compOp == GE_OP))
                return true;
            return false;
        }
        if ((nullbytes[i / 8] >> (7 - i % 8)) & 1) {
            continue;
        }
        if (atrb.type == TypeVarChar) {
            int length;
            memcpy(&length, (char*)rbuff + offset, sizeof(int));
            offset += 4;
            offset += length;
        }
        else if (atrb.type == TypeInt) {
            offset += sizeof(int);
        }
        else if (atrb.type == TypeReal) {
            offset += sizeof(float);
        }
    }

    delete[] nullbytes;
    return false;
}

//
//RC RBFM_ScanIterator::getNextRecord3(RID &rid, void *data)
//{
//    RC stat = 0;
//    if (curPageTotalSlots < 0) {
//        curRid.pageNum = 0;
//        fileHandle.readPage(curRid.pageNum, curPage);
//        curRid.slotNum = 0;
//        curPageTotalSlots = getNumSlots(curPage);
//    }
//    if (curRid.pageNum >= fileHandle.getNumberOfPages()) {
//        return RBFM_EOF;
//    }
//
//    if (curRid.slotNum >= curPageTotalSlots) {
//        curRid.pageNum++;
//        if (curRid.pageNum >= fileHandle.getNumberOfPages()) {
//            return RBFM_EOF;
//        }
//        curRid.slotNum = 0;
//        stat = fileHandle.readPage(curRid.pageNum, curPage);
//        if (stat) return RBFM_EOF;
//        curPageTotalSlots = getNumSlots(curPage);
//        return getNextRecord(rid, data);
//    }
//
//    int soff, slen;
//    getSlotInfo(curPage, curRid.slotNum, soff, slen);
//    if (soff == -1 || slen == -1) {
//        curRid.slotNum++;
//        return getNextRecord(rid, data);
//    }
//
//
//    RID trid;
//    if (isTombstone(curPage, soff, trid)) {
//        curRid.slotNum++;
//        return getNextRecord(rid, data);
//    }
//
//    void* rbuff = malloc(slen);
//    memcpy((char*)rbuff, (char*)curPage + soff, slen);
//    if (compOp == CompOp::NO_OP || checkCondition(rbuff)) {
//        prepareOutputRecord(rbuff, data);
//        rid = curRid;
//        free(rbuff);
//        curRid.slotNum++;
//        return 0;
//    }
//    else {
//        curRid.slotNum++;
//        free(rbuff);
//        return getNextRecord(rid, data);
//    }
//
//
//    return 0;
//}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
    RC stat = 0;
    bool found = false;
    while (stat != RBFM_EOF) {

        if (curPageTotalSlots < 0) {
            curRid.pageNum = 0;
            fileHandle.readPage(curRid.pageNum, curPage);
            curRid.slotNum = 0;
            curPageTotalSlots = getNumSlots(curPage);
        }

        if (curRid.pageNum >= fileHandle.getNumberOfPages()) {
            stat = RBFM_EOF;
            break;
            //            return RBFM_EOF;
        }

        if (curRid.slotNum >= curPageTotalSlots) {
            curRid.pageNum++;
            if (curRid.pageNum >= fileHandle.getNumberOfPages()) {
                stat = RBFM_EOF;
                break;
                //                return RBFM_EOF;
            }
            curRid.slotNum = 0;
            stat = fileHandle.readPage(curRid.pageNum, curPage);
            if (stat) {
                stat = RBFM_EOF;
                break;
                //                return RBFM_EOF;
            }
            curPageTotalSlots = getNumSlots(curPage);
            //            return getNextRecord(rid, data);
            continue;
        }

        int soff, slen;
        getSlotInfo(curPage, curRid.slotNum, soff, slen);
        if (soff == -1 || slen == -1) {
            curRid.slotNum++;
            //            return getNextRecord(rid, data);
            continue;
        }


        RID trid;
        if (isTombstone(curPage, soff, trid)) {
            curRid.slotNum++;
            //            return getNextRecord(rid, data);
            continue;
        }

        void* rbuff = malloc(slen);
        memcpy((char*)rbuff, (char*)curPage + soff, slen);
        if (compOp == CompOp::NO_OP || checkCondition(rbuff)) {
            prepareOutputRecord(rbuff, data);
            rid = curRid;
            free(rbuff);
            curRid.slotNum++;
            stat = 0;
            //            found = true;
            //            return 0;
            break;
        }
        else {
            curRid.slotNum++;
            free(rbuff);
            //            return getNextRecord(rid, data);
            continue;
        }
    }

    return stat;
}

RC RBFM_ScanIterator::getNextRecord2(RID &rid, void *data) 
{
    RC stat = 0;
    if (curPageTotalSlots < 0) {
        curRid.pageNum = 0;
        fileHandle.readPage(curRid.pageNum, curPage);
        curRid.slotNum = 0;
        curPageTotalSlots = getNumSlots(curPage);
    }
    if (curRid.pageNum >= fileHandle.getNumberOfPages()) {
        return RBFM_EOF;
    }

    int soff, slen;
    getSlotInfo(curPage, curRid.slotNum, soff, slen);
    RID trid;
    void* rbuff = malloc(300);

    if (curRid.slotNum >= curPageTotalSlots ) {
        curRid.pageNum++;
        if (curRid.pageNum >= fileHandle.getNumberOfPages()) {
            return RBFM_EOF;
        }
        curRid.slotNum = 0;
        stat = fileHandle.readPage(curRid.pageNum, curPage);
        if (stat) return RBFM_EOF;
        curPageTotalSlots = getNumSlots(curPage);
//        return getNextRecord(rid, data);
    } else if (soff == -1 || slen == -1) {
            curRid.slotNum++;
//            return getNextRecord(rid, data);
    }  else if (isTombstone(curPage, soff, trid)) {
            curRid.slotNum++;
//            return getNextRecord(rid, data);
    } else if (compOp == CompOp::NO_OP || checkCondition(rbuff)) {
        prepareOutputRecord(rbuff, data);
        rid = curRid;
        free(rbuff);
        curRid.slotNum++;
        return 0;
    } else {
        curRid.slotNum++;
//            return getNextRecord(rid, data);
    }
    free(rbuff);
    return getNextRecord(rid, data);

//    return 0; 
}

RC RBFM_ScanIterator::close() 
{
    if (curPage) {
        free(curPage);
        curPage = nullptr;
    }
    return 0; 
}
