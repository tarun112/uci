#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <cstring>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

class KeyChain {
public:
    string strkey;
    int intkey = -1;
    float realkey = -1.0;
    AttrType keytype;

    int compare(const KeyChain& rkchain) const {
        if (TypeVarChar == keytype) {
            return strkey.compare(rkchain.strkey);
        }
        else if (TypeInt == keytype) {
            if (intkey < rkchain.intkey)
                return -1;
            else if (intkey == rkchain.intkey)
                return 0;
            else return 1;
        }
        else if (TypeReal == keytype) {
            if (realkey < rkchain.realkey)
                return -1;
            else if (realkey == rkchain.realkey)
                return 0;
            else return 1;
        }
        return -2;
    }

    int length() const {
        if (TypeVarChar == keytype) {
            return strkey.length()+sizeof(int);
        }
        else if (TypeInt == keytype) {
            return sizeof(int);
        }
        else if (TypeReal == keytype) {
            return sizeof(float);
        }
        return -1;
    }

    int getKey(void* key) const {
        if (TypeVarChar == keytype) {
            int len = strkey.length();
            memcpy((char*)key, (char*)&len, sizeof(int));
            memcpy((char*)key+sizeof(int), (char*)strkey.c_str(), len);
            return 0;
        }
        else if (TypeInt == keytype) {
            memcpy((char*)key, (char*)&intkey, sizeof(int));
            return 0;
        }
        else if (TypeReal == keytype) {
            memcpy((char*)key, (char*)&realkey, sizeof(float));
            return 0;
        }
        return -1;
    }
};

int getKeyChain(const void* key, Attribute attribute, KeyChain &keystore);

enum class NodeType {
    LeafNode = 0,
    KeyNode = 1,
    None = 2
};

struct NodeInfo {
    NodeType _type;
    int _numKeys=0;
    int _size=0;
    PageNum _nextPage=0;
    PageNum _prevPage=0;
    PageNum _overflowPage=0;

    NodeInfo(const NodeType& type, int nkeys, int size, PageNum prevpage, PageNum nextpage, PageNum ovrfpage)
        :_type(type), _numKeys(nkeys), _size(size), _prevPage(prevpage), _nextPage(nextpage), _overflowPage(ovrfpage)
    {}

    NodeInfo()
        :_type(NodeType::None), _numKeys(0), _size(0), _prevPage(0), _nextPage(0)
    {}

};

class NodeKeyRecord {
public:
//    const void* _key = nullptr;
    KeyChain _kchain;
    PageNum _leftchild = 0;
    PageNum _rightchild = 0;

    //NodeKeyRecord(const void* key, PageNum leftc, PageNum rightc)
    //    :_leftchild(leftc), _rightchild(rightc) 
    //{
    //    getKeyChain(key, a)
    //}

    NodeKeyRecord(KeyChain ikchain, PageNum leftc, PageNum rightc)
        :_kchain(ikchain), _leftchild(leftc), _rightchild(rightc)
    {}

    NodeKeyRecord()
    {}

    int getKey(void* key) const {
        return _kchain.getKey(key);
    }

    int length() const {
        return _kchain.length() + 2 * sizeof(int);
    }
};

bool areRIDequall(const RID &rid1, const RID &rid2); 
class LeafKeyRecord {
public:
    //    const void* _key = nullptr;
    KeyChain _kchain;
    RID _rid ;

    //NodeKeyRecord(const void* key, PageNum leftc, PageNum rightc)
    //    :_leftchild(leftc), _rightchild(rightc) 
    //{
    //    getKeyChain(key, a)
    //}

    LeafKeyRecord(KeyChain ikchain, RID rid)
        :_kchain(ikchain), _rid(rid)
    {}

    LeafKeyRecord()
    {}

    int getKey(void* key) const {
        return _kchain.getKey(key);
    }

    int length() const {
        return _kchain.length() + 2 * sizeof(int);
    }

    bool areEquall(const LeafKeyRecord &rhs) const {
        if (_kchain.compare(rhs._kchain) != 0)
            return false;
        return areRIDequall(_rid, rhs._rid);
    }
};

struct NodeKeyInfo {
    int _index = -1; //index or index of next key greater than this or -2 if this is greatest
    int _offset = -1; //offset or offset of the next key greater that this or size
    bool _present = false;
    PageNum _curPage = 0;

};

struct NodeKeyChildInfo {
    enum ChildDir {
        unset = -1,
        left = 0,
        right = 1,
    } _childdir = unset;

    PageNum _curPage = 0;

    PageNum _child=0;

    NodeKeyInfo _nkinfo;
    NodeKeyRecord _nkrec;

    NodeKeyChildInfo(PageNum pagenum, ChildDir dir= ChildDir::unset)
        :_child(pagenum), _childdir(dir)
    {}

    NodeKeyChildInfo()
    {}

};

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};


class IXFileHandle {
    
public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    unsigned ixRootPageNum;
    unsigned ixFirstPageNum=0;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    int openFile(const string &fname);
    int closeFile();

    RC readPage(PageNum pageNum, void *data);                             // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
    RC appendPage(const void *data);                                      // Append a specific page
    unsigned getNumberOfPages();                                          // Get the number of pages in the file

    bool isOpen() {
        if (!_fp)
            return false;
        return true;
    }

    PageNum getRootPageNum() {
        readCountersFromFile();
        return ixRootPageNum;
    }

    PageNum setRootPageNum(PageNum rootpage) {
        ixRootPageNum = rootpage;
        updataHeaderPage();
        return ixRootPageNum;
    }

    PageNum getFirstPageNum() {
        readCountersFromFile();
        return ixFirstPageNum;
    }

    PageNum setFirstPageNum(PageNum rootpage) {
        ixFirstPageNum = rootpage;
        updataHeaderPage();
        return ixFirstPageNum;
    }

private:

    int HEADER_PAGE_OFFSET = 0;
    int DATA_PAGE_OFFSET = 0; //PAGE_SIZE,  page number start from 1


    int getTotalNumberOfPages();
    int initFile();
    int updataHeaderPage();
    int readCountersFromFile();

private:
    FILE* _fp;
};



class IX_ScanIterator {
public:

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();

    IXFileHandle _ixfileHandle;
    Attribute _attribute;
    void* _lowKey = nullptr;
    void* _highKey = nullptr;
    bool _lowKeyInclusive = false;
    bool _highKeyInclusive = false;
    void* _curPageData = nullptr;
    PageNum _curPageNum = 0;
    int _curKeyIndex = -1;
    int _curKeyOffset = -1;

    bool _dealloc_lowkey = false;
    bool _dealloc_highkey = false;

    KeyChain _lowKeyChain;
    KeyChain _highKeyChain;

};



#endif
