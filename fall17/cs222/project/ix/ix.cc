#include <cstdio>
#include <iostream>
#include <string>
#include <cassert>
#include <tuple>
#include <sys/stat.h>
#include <cmath>
#include <cstring>
#include "ix.h"

using namespace std;

//each node on 1 page
//internal nodes are keys and pointers to child
//leaf node is keys and rids

struct NodeKey {
    PageNum _prevChild;
    PageNum _nextChild;
    short int _keySize;
    void* _keyValue;
};

bool areRIDequall(const RID &rid1, const RID &rid2) {
    return ((rid1.pageNum == rid2.pageNum) && (rid1.slotNum == rid2.slotNum));
}


IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return PagedFileManager::instance()->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return ixfileHandle.openFile(fileName);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return ixfileHandle.closeFile();
}

int getNextNodeKey(AttrType type, const void* data, int &offset, void *rbuff)
{
    return 0;
}


int compareKeys(const Attribute& attrib, const void* key, const void* rbuff)
{
    return 0;
}

int getNodeInfo(const void* data, NodeInfo &nodeinfo)
{
    int offset = sizeof(int);
    int val = -1;
    memcpy((char*)&val, (char*)data + PAGE_SIZE - offset, sizeof(int));
    offset += sizeof(int);
    nodeinfo._type = (NodeType)val;
    memcpy((char*)&val, (char*)data + PAGE_SIZE - offset, sizeof(int));
    offset += sizeof(int);
    nodeinfo._numKeys = val;
    memcpy((char*)&val, (char*)data + PAGE_SIZE - offset, sizeof(int));
    offset += sizeof(int);
    nodeinfo._size = val;
    memcpy((char*)&val, (char*)data + PAGE_SIZE - offset, sizeof(int));
    offset += sizeof(int);
    nodeinfo._nextPage = val;
    memcpy((char*)&val, (char*)data + PAGE_SIZE - offset, sizeof(int));
    offset += sizeof(int);
    nodeinfo._prevPage = val;
    memcpy((char*)&val, (char*)data + PAGE_SIZE - offset, sizeof(int));
    offset += sizeof(int);
    nodeinfo._overflowPage = val;
    return 0;
}

int writeNodeInfo(void* data, const NodeInfo &nodeinfo)
{
    int offset = sizeof(int);
    int val = -1;
    memcpy((char*)data + PAGE_SIZE - offset, (char*)&nodeinfo._type, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)data + PAGE_SIZE - offset, (char*)&nodeinfo._numKeys, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)data + PAGE_SIZE - offset, (char*)&nodeinfo._size, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)data + PAGE_SIZE - offset, (char*)&nodeinfo._nextPage, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)data + PAGE_SIZE - offset, (char*)&nodeinfo._prevPage, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)data + PAGE_SIZE - offset, (char*)&nodeinfo._overflowPage, sizeof(int));
    offset += sizeof(int);
    return 0;
}

int getKeyChain(const void* key, Attribute attribute, KeyChain &keystore) {
    if (key == nullptr) {
        return 0;
    }

    if (TypeVarChar == attribute.type) {
        int len = 0;
        memcpy((char*)&len, (char*)key, sizeof(int));
        char fkey[PAGE_SIZE];
        memcpy(fkey, (char*)key + sizeof(int), len);
        fkey[len] = '\0';
        keystore.keytype = attribute.type;
        keystore.strkey = fkey;
    }
    else if (TypeInt == attribute.type) {
        int val = 0;
        memcpy((char*)&val, (char*)key, sizeof(int));
        keystore.keytype = attribute.type;
        keystore.intkey = val;
    }
    else if (TypeReal == attribute.type) {
        float val = 0;
        memcpy((char*)&val, (char*)key, sizeof(float));
        keystore.keytype = attribute.type;
        keystore.realkey = val;
    }
    return 0;
}

KeyChain getKeyChain(const void* key, Attribute attribute) {
    KeyChain kchain;
    getKeyChain(key, attribute, kchain);
    return kchain;
}

int getSizeOfNodeInfo()
{
    return 6 * sizeof(int);
}

int removeBlock(void* data, int blockoffset, int blocksize, int tsize) {
    int movesize = tsize - blockoffset - blocksize;
    memmove((char*)data + blockoffset, (char*)data + blockoffset + blocksize, movesize);
    return 0;
}

int addBlock(void* data, int blockoffset, int blocksize, int tsize) {
    int movesize = tsize - blockoffset;
    memmove((char*)data + blockoffset + blocksize, (char*)data + blockoffset, movesize);
    return 0;
}

int getLeafKeyRecord(const void* data, Attribute attrib, LeafKeyRecord &lkrec)
{
    int ofst = 0;
    char key[PAGE_SIZE];
    if (TypeVarChar == attrib.type) {
        int len = -1;
        memcpy((char*)&len, (char*)data + ofst, sizeof(int));
        memcpy(key, (char*)data + ofst, sizeof(int) + len);
//        if (key != nullptr)
//            memcpy((char*)key, (char*)data + ofst, sizeof(int) + len);
        ofst += sizeof(int) + len;
    }
    else if (TypeInt == attrib.type) {
//        if (key != nullptr)
//            memcpy((char*)key, (char*)data + ofst, sizeof(int));
        memcpy((char*)key, (char*)data + ofst, sizeof(int));
        ofst += sizeof(int);
    }
    else if (TypeReal == attrib.type) {
//        if (key != nullptr)
//            memcpy((char*)key, (char*)data + ofst, sizeof(float));
        memcpy((char*)key, (char*)data + ofst, sizeof(float));
        ofst += sizeof(float);
    }
    memcpy((char*)&lkrec._rid.pageNum, (char*)data + ofst, sizeof(int));
    ofst += sizeof(int);
    memcpy((char*)&lkrec._rid.slotNum, (char*)data + ofst, sizeof(int));
    ofst += sizeof(int);

    getKeyChain(key, attrib, lkrec._kchain);

    return 0;
}

int getNodeKeyRecord(const void* data, Attribute attrib, NodeKeyRecord &nkrec)
{
    int ofst = 0;
    memcpy((char*)&nkrec._leftchild, (char*)data + ofst, sizeof(int));
    ofst += sizeof(int);
    char key[PAGE_SIZE];
    if (TypeVarChar == attrib.type) {
        int len = -1;
        memcpy((char*)&len, (char*)data + ofst, sizeof(int));
        //if (nkrec._key != nullptr)
        //    memcpy((char*)nkrec._key, (char*)data + ofst, sizeof(int) + len);
        memcpy(key, (char*)data + ofst, sizeof(int) + len);
        ofst += sizeof(int) + len;
    }
    else if (TypeInt == attrib.type) {
        //if (nkrec._key != nullptr)
        //    memcpy((char*)nkrec._key, (char*)data + ofst, sizeof(int));
        memcpy(key, (char*)data + ofst, sizeof(int));
        ofst += sizeof(int);
    }
    else if (TypeReal == attrib.type) {
        //if (nkrec._key != nullptr)
        //    memcpy((char*)nkrec._key, (char*)data + ofst, sizeof(float));
        memcpy(key, (char*)data + ofst, sizeof(float));
        ofst += sizeof(float);
    }
    memcpy((char*)&nkrec._rightchild, (char*)data + ofst, sizeof(int));
    ofst += sizeof(int);

    getKeyChain(key, attrib, nkrec._kchain);
    return 0;
}

//int searchNodeKey(const void* data, const NodeInfo &nodeinfo, Attribute attrib, KeyChain ikchain, PageNum& pagenum)
int searchNodeKey(const void* data, const NodeInfo &nodeinfo, Attribute attrib, KeyChain ikchain, NodeKeyChildInfo& nkcinfo)
{
    if (nodeinfo._numKeys == 0) {
        nkcinfo._child = 0;
        nkcinfo._childdir = NodeKeyChildInfo::ChildDir::unset;
        return 0;
    }

    int offset = 0, loffset = 0;
    int i = 0;
    bool found = false;
//    PageNum leftc = 0;
//    PageNum rightc = 0;
//    KeyChain kchain;

    while (i < nodeinfo._numKeys) {
//        getNodeKeyRecordChain((char*)data + offset, attrib, leftc, rightc, kchain);
        NodeKeyRecord nkrec;
        getNodeKeyRecord((char*)data+offset, attrib, nkrec);
        if (ikchain.compare(nkrec._kchain) < 0) {
//            pagenum = leftc;
            nkcinfo._child = nkrec._leftchild;
            nkcinfo._childdir = NodeKeyChildInfo::ChildDir::left;
            nkcinfo._nkinfo._index = i;
            nkcinfo._nkinfo._offset = offset;
            nkcinfo._nkinfo._present = false;
            nkcinfo._nkrec = nkrec;
            found = true;
            break;
        }
        else if (ikchain.compare(nkrec._kchain) == 0) {
            nkcinfo._child = nkrec._rightchild;
            nkcinfo._childdir = NodeKeyChildInfo::ChildDir::right;
            nkcinfo._nkinfo._index = i;
            nkcinfo._nkinfo._offset = offset;
            nkcinfo._nkinfo._present = true;
            nkcinfo._nkrec = nkrec;
            //            pagenum = rightc;
            found = true;
            break;
        }

        if (i == nodeinfo._numKeys - 1) {
            nkcinfo._child = nkrec._rightchild;
            nkcinfo._childdir = NodeKeyChildInfo::ChildDir::right;
            nkcinfo._nkinfo._index = -2;
            nkcinfo._nkinfo._offset = nodeinfo._size;
            nkcinfo._nkinfo._present = false;
            nkcinfo._nkrec = nkrec;
            //            pagenum = rightc;
            found = true;
            break;
        }
        offset += nkrec.length();
        ++i;
    }

    if (!found)
        return -1;

    return 0;
}

int searchNodeKeyInfo(const void* data, const NodeInfo &nodeinfo, Attribute attrib, KeyChain ikchain, NodeKeyInfo &nkinfo, NodeKeyRecord &nkrec)
{
    int offset = 0, loffset = 0;
    int i = 0;
    bool found = false;
    NodeKeyRecord fnkrec;
    while (i < nodeinfo._numKeys) {
//        getNodeKeyRecordChain((char*)data + offset, attrib, nkrec);
        getNodeKeyRecord((char*)data + offset, attrib, fnkrec);
        if (ikchain.compare(fnkrec._kchain) == 0) {
            nkinfo._index = i;
            nkinfo._offset = offset;
            nkinfo._present = true;
            nkrec = fnkrec;
//            nkrec._kchain = kchain;
            found = true;
            break;
        }
        if (ikchain.compare(fnkrec._kchain) < 0) {
            nkinfo._index = i;
            nkinfo._offset = offset;
            nkinfo._present = false;
            nkrec = fnkrec;
            found = true;
            break;
        }
        offset += fnkrec.length();
        ++i;
    }

    if (!found) {
        nkinfo._index = -2;
        nkinfo._offset = nodeinfo._size;
        nkinfo._present = false;
    }

    return 0;
}


//int searchKeyPage(IXFileHandle &ixfileHandle, PageNum curPage, const Attribute& attribute, const void *key, PageNum& pagenum, vector<PageNum> &parents)
int searchKeyPage(IXFileHandle &ixfileHandle, PageNum curPage, const Attribute& attribute, const void *key, NodeKeyChildInfo &nkcinfo, vector<NodeKeyChildInfo> &parents)
{
    void *data = malloc(PAGE_SIZE);
    RC stat = 0;
    stat = ixfileHandle.readPage(curPage, data);
    if (stat) return stat;

    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);

    if (nodeinfo._type == NodeType::LeafNode) {
        return -1;
    }

    KeyChain ikchain;
    getKeyChain(key, attribute, ikchain);

    bool found = false;
    PageNum pagenum = curPage;
    while (nodeinfo._type != NodeType::LeafNode) {
        NodeKeyChildInfo fnkcinfo;
//        searchNodeKey(data, nodeinfo, attribute, ikchain, nkcinfo);
        searchNodeKey(data, nodeinfo, attribute, ikchain, fnkcinfo);
        fnkcinfo._curPage = pagenum;
        parents.push_back(fnkcinfo);
        pagenum = fnkcinfo._child;
        if (0 == pagenum) {
            nkcinfo = fnkcinfo;
            free(data);
            return 0;
        }
        stat = ixfileHandle.readPage(pagenum, data);
        if (stat) {
            free(data);
            return stat;
        }

        getNodeInfo(data, nodeinfo);
        if (nodeinfo._type == NodeType::LeafNode) {
            nkcinfo = fnkcinfo;
            found = true;
            break;
        }
//        parents.push_back(pagenum);
//        parents.push_back(nkcinfo);
    }
    free(data);
 
    if (!found)
        return -1;
    
    return 0;
}


int getNextPageNum(IXFileHandle &ixfileHandle, PageNum &pagenum ) {
    pagenum = ixfileHandle.getNumberOfPages();
    return 0;
}

int getKeySize(const Attribute &attribute, const void *key)
{
    if (TypeInt == attribute.type) {
        return sizeof(int);
    } 
    else if( TypeReal == attribute.type) {
        return sizeof(float);
    }
    else if (TypeVarChar == attribute.type) {
        int length;
        memcpy(&length, (char*)key, sizeof(int));
        return length + sizeof(int);
    }
    return -1;
}

int writeNodeKeyRecord(void* data, NodeKeyInfo &nkinfo, NodeKeyRecord &nkrec) {
    int ofst = nkinfo._offset;
    memcpy((char*)data + ofst, (char*)&nkrec._leftchild, sizeof(int));
    ofst += sizeof(int);
    char key[PAGE_SIZE];
    nkrec.getKey(key);
    int keysize = nkrec._kchain.length();
    memcpy((char*)data + ofst, (char*)key, keysize);
    ofst += keysize;
    memcpy((char*)data + ofst, (char*)&nkrec._rightchild, sizeof(int));
    ofst += sizeof(int);

    return 0;
}

int getNodePageIndexKeyRecord(const void* data, const Attribute &attrib, NodeInfo inodeinfo, int index, NodeKeyRecord& ilkrec)
{
    int i = 0;
    int ofst = 0;
    while (i < inodeinfo._numKeys) {
        NodeKeyRecord flkrec;
        getNodeKeyRecord((char*)data + ofst, attrib, flkrec);
        if (i == index) {
            ilkrec = flkrec;
            return 0;
        }
        ofst += flkrec.length();
        ++i;
    }
    return -1;
}

int getNodePageOffsetKeyRecord(const void* data, const Attribute &attrib, NodeInfo inodeinfo, int offset, NodeKeyRecord& ilkrec)
{
    if (offset >= inodeinfo._size)
        return -1;

    getNodeKeyRecord((char*)data + offset, attrib, ilkrec);

    return 0;
}

int updateNodeKeyRecord(void* data, const Attribute &attrib, NodeInfo nodeinfo,
    NodeKeyInfo &nkinfo, NodeKeyRecord &nkrec) 
{
    writeNodeKeyRecord(data, nkinfo, nkrec);

    int idx = nkinfo._index;
    if (idx == -2) {
        NodeKeyRecord lnkrec;
        getNodePageIndexKeyRecord(data, attrib, nodeinfo, nodeinfo._numKeys-2, lnkrec);

        NodeKeyInfo lnkinfo(nkinfo);
        --lnkinfo._index;
        lnkinfo._offset -= lnkrec.length();
        lnkrec._rightchild = nkrec._leftchild;

        writeNodeKeyRecord(data, lnkinfo, lnkrec);
        return 0;
    }

    if (idx > 0 || idx == -2) {
        NodeKeyRecord lnkrec;
        getNodePageIndexKeyRecord(data, attrib, nodeinfo, idx - 1, lnkrec);

        NodeKeyInfo lnkinfo(nkinfo);
        --lnkinfo._index;
        lnkinfo._offset -= lnkrec.length();
        lnkrec._rightchild = nkrec._leftchild;

        writeNodeKeyRecord(data, lnkinfo, lnkrec);
    }
    if (idx >= 0 && idx < nodeinfo._numKeys-1) {
        NodeKeyRecord lnkrec;
        getNodePageIndexKeyRecord(data, attrib, nodeinfo, idx+1, lnkrec);

        NodeKeyInfo lnkinfo(nkinfo);
        ++lnkinfo._index;
        lnkinfo._offset += nkrec.length();
        lnkrec._leftchild = nkrec._rightchild;

        writeNodeKeyRecord(data, lnkinfo, lnkrec);
    }

    return 0;
}

bool isNodePageHasFreeSpace(IXFileHandle &ixfileHandle, PageNum pagenum, NodeKeyRecord lkrec) {
    void* data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pagenum, data);
    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);
    if (lkrec.length() <= PAGE_SIZE - getSizeOfNodeInfo() - nodeinfo._size)
        return true;
    return false;
}

int nodePageInsertRecord(void* data, int offset, NodeInfo &nodeinfo, NodeKeyRecord ilkrec) {
    int blocksize = ilkrec.length();
    int sz = nodeinfo._size;
    addBlock(data, offset, blocksize, sz);

    char key[PAGE_SIZE];
    ilkrec._kchain.getKey(key);
    int keysize = ilkrec._kchain.length();

    int ofst = offset;
    memcpy((char*)data + ofst, (char*)&ilkrec._leftchild, sizeof(int));
    ofst += sizeof(int);
    memcpy((char*)data + ofst, (char*)key, keysize);
    ofst += keysize;
    memcpy((char*)data + ofst, (char*)&ilkrec._rightchild, sizeof(int));
    ofst += sizeof(int);

    nodeinfo._numKeys += 1;
    nodeinfo._size += blocksize;
    //    writeNodeInfo(data, nodeinfo);

    return 0;
}

int appendNodePage(IXFileHandle &ixfileHandle, PageNum &pagenum, NodeInfo &nodeinfo)
{
    void* data = malloc(PAGE_SIZE);
    ixfileHandle.appendPage(data);
    getNextPageNum(ixfileHandle, pagenum);

    writeNodeInfo(data, nodeinfo);
    ixfileHandle.writePage(pagenum, data);
    free(data);

    return 0;
}


int nodePageSplitInsertKey(IXFileHandle &ixfileHandle, PageNum pagenum, const Attribute &attribute,
    vector<NodeKeyChildInfo> parents, NodeKeyRecord ilkrec);

int nodePageInsertKey(IXFileHandle &ixfileHandle, PageNum pagenum,
    const Attribute &attribute, vector<NodeKeyChildInfo> &parents, NodeKeyRecord nkrec)
{

    //    getKeyChain(nkrec._key, attribute, nkrec._kchain);
    int keysize = nkrec._kchain.length();
    //    int keysize = getKeySize(attribute, key);

    void* data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pagenum, data);

    NodeInfo nodeinfo(NodeType::KeyNode, 1, keysize, 0, 0, 0);
    getNodeInfo(data, nodeinfo);

    //    int leftchild = 0;
    //    int rightchild = 0;
    char key[PAGE_SIZE];
    if (0 == nodeinfo._numKeys) {
        nodePageInsertRecord(data, nodeinfo._size, nodeinfo, nkrec);
        writeNodeInfo(data, nodeinfo);
        ixfileHandle.writePage(pagenum, data);
        free(data);
        return 0;
    }

    NodeKeyInfo fnkinfo;
    NodeKeyRecord fnkrec;
    searchNodeKeyInfo(data, nodeinfo, attribute, nkrec._kchain, fnkinfo, fnkrec);
    if (fnkinfo._present) {
        updateNodeKeyRecord((char*)data, attribute, nodeinfo, fnkinfo, nkrec);
        ixfileHandle.writePage(pagenum, data);
        free(data);
        return 0;
    }
    else {
        if (isNodePageHasFreeSpace(ixfileHandle, pagenum, nkrec)) {
            int sz = nodeinfo._size;
            int blockofst = fnkinfo._offset;
            int blocksize = nkrec.length();
            addBlock(data, blockofst, blocksize, sz);

            nodeinfo._numKeys += 1;
            nodeinfo._size += nkrec.length();
            writeNodeInfo(data, nodeinfo);

            updateNodeKeyRecord((char*)data, attribute, nodeinfo, fnkinfo, nkrec);

            ixfileHandle.writePage(pagenum, data);
            free(data);
            return 0;

        }
        else {
            nodePageSplitInsertKey(ixfileHandle, pagenum, attribute, parents, nkrec);
        }
    }

    //int ofst = 0;
    //memcpy((char*)data + ofst, (char*)&leftchild, sizeof(int));
    //ofst += sizeof(int);
    //memcpy((char*)data + ofst, (char*)key, keysize);
    //ofst += keysize;
    //memcpy((char*)data + ofst, (char*)&rightchild, sizeof(int));
    //ofst += sizeof(int);

    //nodeinfo._numKeys += 1;
    //nodeinfo._size += keysize+2*sizeof(int);
    //writeNodeInfo(data, nodeinfo);

    //ixfileHandle.writePage(pagenum, data);

    free(data);

    return 0;
}

int nodePageSplitInsertKey(IXFileHandle &ixfileHandle, PageNum pagenum, const Attribute &attribute,
    vector<NodeKeyChildInfo> parents, NodeKeyRecord ilkrec) 
{
    void* data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pagenum, data);

    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);
    int sz = nodeinfo._size;

    int pagesize = 2 * PAGE_SIZE;
    void* data2 = malloc(pagesize);
    memcpy((char*)data2, (char*)data, PAGE_SIZE);

    if (ilkrec.length() > 2 * PAGE_SIZE - getSizeOfNodeInfo() - sz)
        return -1;

    {
        if (sz == 0) {
            //                leafPageInsertRecord(data, 0, nodeinfo, ilkrec);
            //                return -1;  //error
        }

        int i = 0;
        int ofst = 0;
        NodeKeyRecord flkrec;
        while (i < nodeinfo._numKeys) {
            getNodeKeyRecord((char*)data2 + ofst, attribute, flkrec);
            if (ilkrec._kchain.compare(flkrec._kchain) <= 0) {
                nodePageInsertRecord(data2, ofst, nodeinfo, ilkrec);
                break;
            }
            ofst += flkrec.length();
            if (i == nodeinfo._numKeys - 1) {
                nodePageInsertRecord(data2, nodeinfo._size, nodeinfo, ilkrec);
                break;
            }
            ++i;
        }

        //            splitPage(data2, data0, data1, nodeinfo)
        {
            int sz2 = nodeinfo._size;
            assert(sz2 >= PAGE_SIZE - getSizeOfNodeInfo());
            void* data11 = malloc(PAGE_SIZE);
            void* data12 = malloc(PAGE_SIZE);
            int i = 0;
            int ofst = 0;
            int halfofst = 0;
            int halfidx = 0;
            while (i < nodeinfo._numKeys) {
                NodeKeyRecord flkrec;
                getNodeKeyRecord((char*)data2 + ofst, attribute, flkrec);
                ofst += flkrec.length();
                ++i;
                if (halfofst == 0 && ofst >= sz2 / 2) {
                    halfofst = ofst;
                    halfidx = i;
                    break;
                }

            }

            
            PageNum rootpage = ixfileHandle.getRootPageNum();

            NodeKeyChildInfo mkcinfo;
            if (rootpage == pagenum || parents.empty()) {
                assert(rootpage == pagenum);
                NodeInfo nodeinfo;
                nodeinfo._type = NodeType::KeyNode;
                appendNodePage(ixfileHandle, rootpage, nodeinfo);
                ixfileHandle.setRootPageNum(rootpage);

//                NodeKeyRecord nkrec(getKeyChain(key, attribute), 0, 0);
//                vector<NodeKeyChildInfo> parents;
//                nodePageInsertKey(ixfileHandle, rootpage, attribute, parents, nkrec);

                mkcinfo._curPage = rootpage;
                mkcinfo._childdir = NodeKeyChildInfo::ChildDir::left;
                mkcinfo._child = pagenum;
            }
            else {
                mkcinfo = parents.back();
                parents.pop_back();
            }

            if (mkcinfo._childdir == NodeKeyChildInfo::ChildDir::left) {
                memcpy((char*)data11, (char*)data2, halfofst);
                ixfileHandle.appendPage(data11);
                PageNum nodepage = 0;
                getNextPageNum(ixfileHandle, nodepage);
                NodeInfo nodeinfo1(nodeinfo);
                nodeinfo1._size = halfofst;
                nodeinfo1._numKeys = halfidx;// ?
                nodeinfo1._nextPage = pagenum;// ?
                writeNodeInfo(data11, nodeinfo1);
                ixfileHandle.writePage(nodepage, data11);

                NodeKeyRecord remnkrec;
                getNodePageIndexKeyRecord(data2, attribute, nodeinfo, halfidx, remnkrec);
                int remofst = remnkrec.length();
                memcpy((char*)data12, (char*)data2 + halfofst+remofst, sz2 - halfofst-remofst);
                NodeInfo nodeinfo2(nodeinfo);
                nodeinfo2._size = sz2 - halfofst - remofst;
                nodeinfo2._numKeys = nodeinfo._numKeys - halfidx-1;// ?
                nodeinfo2._prevPage = nodepage;// ?
                writeNodeInfo(data12, nodeinfo2);
                ixfileHandle.writePage(pagenum, data12);

                //NodeKeyRecord flkrec;
                //getNodeKeyRecord(data12, attribute, flkrec);
                //NodeKeyRecord fnkrec(flkrec._kchain, nodepage, mkcinfo._child);
                NodeKeyRecord fnkrec(remnkrec._kchain, nodepage, mkcinfo._child);

                nodePageInsertKey(ixfileHandle, mkcinfo._curPage, attribute, parents, fnkrec);

            }
            else if (mkcinfo._childdir == NodeKeyChildInfo::ChildDir::right) {
                NodeKeyRecord remnkrec;
                getNodePageIndexKeyRecord(data2, attribute, nodeinfo, halfidx, remnkrec);
                int remofst = remnkrec.length();

                memcpy((char*)data12, (char*)data2 + halfofst + remofst, sz2 - halfofst - remofst);
                ixfileHandle.appendPage(data12);
                PageNum nodepage = 0;
                getNextPageNum(ixfileHandle, nodepage);
                NodeInfo nodeinfo2(nodeinfo);
                nodeinfo2._size = sz2 - halfofst - remofst;
                nodeinfo2._numKeys = nodeinfo._numKeys - halfidx-1;// ?
                nodeinfo2._prevPage = pagenum;// ?
                writeNodeInfo(data12, nodeinfo2);
                ixfileHandle.writePage(nodepage, data12);


                //NodeKeyRecord flkrec;
                //getNodeKeyRecord(data12, attribute, flkrec);
                NodeKeyRecord fnkrec(remnkrec._kchain, mkcinfo._child, nodepage);

                nodePageInsertKey(ixfileHandle, mkcinfo._curPage, attribute, parents, fnkrec);

                memcpy((char*)data11, (char*)data2, halfofst);
                NodeInfo nodeinfo1(nodeinfo);
                nodeinfo1._size = halfofst;
                nodeinfo1._numKeys = halfidx;// ?
                nodeinfo1._nextPage = nodepage;// ?
                writeNodeInfo(data11, nodeinfo1);
                ixfileHandle.writePage(pagenum, data11);
            }
        }
    }
            return 0;
}


//int nodePageInsertKey

int nodePageUpdateKey(IXFileHandle &ixfileHandle, NodeKeyChildInfo nkcinfo, const Attribute &attribute, PageNum child)
{
    
    void* data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(nkcinfo._curPage, data);

    //NodeInfo nodeinfo;
    //getNodeInfo(data, nodeinfo);
    //if (nodeinfo._numKeys == 0) {
    //    PageNum pagenum = nkcinfo._curPage;
    //    NodeKeyRecord fnkrec(nkcinfo._nkrec._kchain, 0, child);
    //    return nodePageInsertKey(ixfileHandle, pagenum, attribute, fnkrec);
    //}

    int offset = nkcinfo._nkinfo._offset;
    if (nkcinfo._childdir == NodeKeyChildInfo::ChildDir::left) {
        memcpy((char*)data + offset, (char*)&child, sizeof(int));
    }
    else if (nkcinfo._childdir == NodeKeyChildInfo::ChildDir::right) {
        int len = nkcinfo._nkrec._kchain.length();
        memcpy((char*)data + offset + sizeof(int) + len, (char*)&child, sizeof(int));
    }

    ixfileHandle.writePage(nkcinfo._curPage, data);

    free(data);
    return 0;
}


int leafPageInsertRecord(void* data, int offset, NodeInfo &nodeinfo, LeafKeyRecord ilkrec) {
    int blocksize = ilkrec.length();
    int sz = nodeinfo._size;
    addBlock(data, offset, blocksize, sz);

    char key[PAGE_SIZE];
    ilkrec._kchain.getKey(key);
    int keysize = ilkrec._kchain.length();

    int ofst = offset;
    memcpy((char*)data + ofst, (char*)key, keysize);
    ofst += keysize;
    memcpy((char*)data + ofst, (char*)&ilkrec._rid.pageNum, sizeof(int));
    ofst += sizeof(int);
    memcpy((char*)data + ofst, (char*)&ilkrec._rid.slotNum, sizeof(int));
    ofst += sizeof(int);

    nodeinfo._numKeys += 1;
    nodeinfo._size += blocksize;
//    writeNodeInfo(data, nodeinfo);

    return 0;
}

//int sizePageInsertKey(void* data, int pagesize, NodeInfo ndeinfo,
//    LeafKeyRecord lkrec) {
//
//    int sz = nodeinfo._size;
//    if (keysize + 2 * sizeof(int) < PAGE_SIZE - getSizeOfNodeInfo() - sz) {
//        if (sz == 0) {
//            leafPageInsertRecord(data, 0, nodeinfo, ilkrec);
//        }
//        else {
//            int i = 0;
//            int ofst = 0;
//            LeafKeyRecord flkrec;
//            while (i < nodeinfo._numKeys) {
//                getLeafKeyRecord((char*)data + ofst, attribute, flkrec);
//                if (ilkrec._kchain.compare(flkrec._kchain) <= 0) {
//                    leafPageInsertRecord(data, ofst, nodeinfo, ilkrec);
//                    break;
//                }
//                ofst += flkrec.length();
//                if (i == nodeinfo._numKeys - 1) {
//                    leafPageInsertRecord(data, nodeinfo._size, nodeinfo, ilkrec);
//                    break;
//                }
//                ++i;
//            }
//        }
//        ixfileHandle.writePage(pagenum, data);
//    }
//
//}

int leafPageInsertKey(IXFileHandle &ixfileHandle, PageNum pagenum, 
    const Attribute &attribute, const void *key, const RID &rid)
{
    KeyChain ikchain;
    getKeyChain(key, attribute, ikchain);
    LeafKeyRecord ilkrec(ikchain, rid);
    int recsize = ilkrec.length();

    void* data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pagenum, data);

    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);

    //int pagesize = PAGE_SIZE;
    //sizePageInsertKey(data, pagesize, nodeinfo, ilkrec);

    int sz = nodeinfo._size;
    if (recsize > PAGE_SIZE - getSizeOfNodeInfo() - sz)
        return -1;
    if (sz == 0) {
        leafPageInsertRecord(data, 0, nodeinfo, ilkrec);
    }
    else {
        int i = 0;
        int ofst = 0;
        LeafKeyRecord flkrec;
        while (i < nodeinfo._numKeys) {
            getLeafKeyRecord((char*)data + ofst, attribute, flkrec);
            if (ilkrec._kchain.compare(flkrec._kchain) <= 0) {
                leafPageInsertRecord(data, ofst, nodeinfo, ilkrec);
                break;
            }
            ofst += flkrec.length();
            if (i == nodeinfo._numKeys - 1) {
                leafPageInsertRecord(data, nodeinfo._size, nodeinfo, ilkrec);
                break;
            }
            ++i;
        }
    }
    writeNodeInfo(data, nodeinfo);
    ixfileHandle.writePage(pagenum, data);
    free(data);
    return 0;
}

int leafPageSplitInsertKey(IXFileHandle &ixfileHandle, PageNum pagenum, const Attribute &attribute, 
    vector<NodeKeyChildInfo> parents, LeafKeyRecord ilkrec) {
//    NodeKeyChildInfo mkcinfo, LeafKeyRecord ilkrec) {
    
    void* data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pagenum, data);

    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);
    int sz = nodeinfo._size;

    int pagesize = 2 * PAGE_SIZE;
    void* data2 = malloc(pagesize);
    memcpy((char*)data2, (char*)data, PAGE_SIZE);
    
    if (ilkrec.length() > 2 * PAGE_SIZE - getSizeOfNodeInfo() - sz)
        return -1;
    
    {
        if (sz == 0) {
//                leafPageInsertRecord(data, 0, nodeinfo, ilkrec);
//                return -1;  //error
        }
            
        int i = 0;
        int ofst = 0;
        LeafKeyRecord flkrec;
        while (i < nodeinfo._numKeys) {
            getLeafKeyRecord((char*)data2 + ofst, attribute, flkrec);
            if (ilkrec._kchain.compare(flkrec._kchain) <= 0) {
                leafPageInsertRecord(data2, ofst, nodeinfo, ilkrec);
                break;
            }
            ofst += flkrec.length();
            if (i == nodeinfo._numKeys - 1) {
                leafPageInsertRecord(data2, nodeinfo._size, nodeinfo, ilkrec);
                break;
            }
            ++i;
        }

//            splitPage(data2, data0, data1, nodeinfo)
        {
            int sz2 = nodeinfo._size;
            assert(sz2 >= PAGE_SIZE - getSizeOfNodeInfo());
            void* data11 = malloc(PAGE_SIZE);
            void* data12 = malloc(PAGE_SIZE);
            int i = 0;
            int ofst = 0;
            LeafKeyRecord pkrec;
            vector<tuple<int, int, int, int>> eqKeysRec;
            int eqKeysOfst = 0;
            int eqKeysSz = 0;
            int eqKeyIdx = 0;
            int neqkeys = 1;
            bool eqkey = false;
            int halfofst = 0;
            int halfidx = 0;
            int prevofst = 0;

            while (i < nodeinfo._numKeys) {
                LeafKeyRecord flkrec;
                getLeafKeyRecord((char*)data2 + ofst, attribute, flkrec);
                if (i > 0 ) {
                    if (pkrec._kchain.compare(flkrec._kchain) == 0) {
//                        eqKeysOfst = ofst;
//                        eqKeysSz += flkrec.length();
//                        eqKeyIdx = i;
//                        eqKeysOfst = ofst;
                        eqKeysSz += flkrec.length();
                        ++neqkeys;
                        eqkey = true;
                    }
                    else if (eqkey) {
                        eqKeysRec.push_back(make_tuple(eqKeysOfst, eqKeysSz, eqKeyIdx, neqkeys));
                        eqKeysOfst = ofst;
                        eqKeysSz = flkrec.length();
                        eqKeyIdx = i;
                        neqkeys = 1;
                        eqkey = false;
                    }
                    else {
                        eqKeysSz = flkrec.length();
                        eqKeysOfst = ofst;
                        eqKeyIdx = i;
                    }
                }
                else {
                    eqKeysSz = flkrec.length();
                    eqKeysOfst = ofst;
                    eqKeyIdx = i;
                }
                if (  i == nodeinfo._numKeys - 1 && eqkey) {
                    eqKeysRec.push_back(make_tuple(eqKeysOfst, eqKeysSz, eqKeyIdx, neqkeys));
                    eqKeysOfst = ofst;
                    eqKeysSz = flkrec.length();
                    eqKeyIdx = i;
                    neqkeys = 1;
                    eqkey = false;
                }
                pkrec = flkrec;
                prevofst = ofst;
                ofst += flkrec.length();
                ++i;
                if (halfofst == 0 && ofst >= sz2 / 2) {
                    halfofst = ofst;
                    halfidx = i;
                }
            }

            for (int i = 0; i < eqKeysRec.size(); ++i) {
                int eqkofst = get<0>(eqKeysRec[i]);
                int eqidx = get<2>(eqKeysRec[i]);
                if (eqkofst >= sz2 / 2) {
                    halfofst = eqkofst;
                    halfidx = eqidx;
                    break;
                }

                int eqksz = get<1>(eqKeysRec[i]);
                if (eqkofst+eqksz >= sz2 / 2) {
                    halfofst = eqkofst + eqksz;
                    halfidx = eqidx + get<3>(eqKeysRec[i]);
                    break;
                }
            }
            if (halfofst > PAGE_SIZE - getSizeOfNodeInfo())
            {
                //memcpy((char*)data12, (char*)data2 + halfofst, sz2 - halfofst);
                //ixfileHandle.appendPage(data12);
                //PageNum leafpage = 0;
                //getNextPageNum(ixfileHandle, leafpage);
                //NodeInfo nodeinfo2(nodeinfo);
                //nodeinfo2._size = sz2 - halfofst;
                //nodeinfo2._numKeys = nodeinfo._numKeys - halfidx;// ?
                //writeNodeInfo(data12, nodeinfo2);
                //ixfileHandle.writePage(leafpage, data12);

                //memcpy((char*)data11, (char*)data2, halfofst);
                //NodeInfo nodeinfo1(nodeinfo);
                //nodeinfo1._size = halfofst;
                //nodeinfo1._numKeys = halfidx;// ?
                //nodeinfo1._overflowPage = leafpage;// ?
                //writeNodeInfo(data11, nodeinfo1);
                //ixfileHandle.writePage(pagenum, data11);
                return -1;
            }
            else {
                const NodeKeyChildInfo &mkcinfo = parents.back();
                if (mkcinfo._childdir == NodeKeyChildInfo::ChildDir::left) {
                    memcpy((char*)data11, (char*)data2, halfofst);
                    ixfileHandle.appendPage(data11);
                    PageNum leafpage = 0;
                    getNextPageNum(ixfileHandle, leafpage);
                    NodeInfo nodeinfo1(nodeinfo);
                    nodeinfo1._size = halfofst;
                    nodeinfo1._numKeys = halfidx;// ?
                    nodeinfo1._nextPage = pagenum;// ?
                    writeNodeInfo(data11, nodeinfo1);
                    ixfileHandle.writePage(leafpage, data11);

                    if(nodeinfo1._prevPage)
                    {
                        void* data2 = malloc(PAGE_SIZE);
                        ixfileHandle.readPage(nodeinfo1._prevPage, data2);
                        NodeInfo ninfo;
                        getNodeInfo(data2, ninfo);
                        ninfo._nextPage = leafpage;
                        writeNodeInfo(data2, ninfo);
                        ixfileHandle.writePage(nodeinfo1._prevPage, data2);
                        free(data2);
                    }

                    memcpy((char*)data12, (char*)data2 + halfofst, sz2 - halfofst);
                    NodeInfo nodeinfo2(nodeinfo);
                    nodeinfo2._size = sz2 - halfofst;
                    nodeinfo2._numKeys = nodeinfo._numKeys - halfidx;// ?
                    nodeinfo2._prevPage = leafpage;// ?
                    writeNodeInfo(data12, nodeinfo2);
                    ixfileHandle.writePage(pagenum, data12);

                    LeafKeyRecord flkrec;
                    getLeafKeyRecord(data12, attribute, flkrec);
                    NodeKeyRecord fnkrec(flkrec._kchain, leafpage, mkcinfo._child);
                    assert(mkcinfo._child == pagenum);

                    parents.pop_back();
                    nodePageInsertKey(ixfileHandle, mkcinfo._curPage, attribute, parents, fnkrec);

                }
                else if (mkcinfo._childdir == NodeKeyChildInfo::ChildDir::right) {
                    memcpy((char*)data12, (char*)data2 + halfofst, sz2 - halfofst);
                    ixfileHandle.appendPage(data12);
                    PageNum leafpage = 0;
                    getNextPageNum(ixfileHandle, leafpage);
                    NodeInfo nodeinfo2(nodeinfo);
                    nodeinfo2._size = sz2 - halfofst;
                    nodeinfo2._numKeys = nodeinfo._numKeys - halfidx;// ?
                    nodeinfo2._prevPage = pagenum;// ?
                    writeNodeInfo(data12, nodeinfo2);
                    ixfileHandle.writePage(leafpage, data12);

                    if (nodeinfo2._nextPage)
                    {
                        void* data2 = malloc(PAGE_SIZE);
                        ixfileHandle.readPage(nodeinfo2._nextPage, data2);
                        NodeInfo ninfo;
                        getNodeInfo(data2, ninfo);
                        ninfo._prevPage = leafpage;
                        writeNodeInfo(data2, ninfo);
                        ixfileHandle.writePage(nodeinfo2._nextPage, data2);
                        free(data2);
                    }

                    memcpy((char*)data11, (char*)data2, halfofst);
                    NodeInfo nodeinfo1(nodeinfo);
                    nodeinfo1._size = halfofst;
                    nodeinfo1._numKeys = halfidx;// ?
                    nodeinfo1._nextPage = leafpage;// ?
                    writeNodeInfo(data11, nodeinfo1);
                    ixfileHandle.writePage(pagenum, data11);

                    LeafKeyRecord flkrec;
                    getLeafKeyRecord(data12, attribute, flkrec);
                    NodeKeyRecord fnkrec(flkrec._kchain, mkcinfo._child, leafpage);
                    assert(mkcinfo._child == pagenum);
                    //                    if (mkcinfo._childdir == NodeKeyChildInfo::ChildDir::left)
                    //                        fnkrec = NodeKeyRecord;

                    parents.pop_back();
                    nodePageInsertKey(ixfileHandle, mkcinfo._curPage, attribute, parents, fnkrec);
                }

            }
        }
    }

//        sizePageInsertKey(data, pagesize, nodeinfo, ilkrec);
//        splitLeafPage()
    

    free(data);
    return 0;
}

//int getNodePageIndexKeyRecord(const void* data, const Attribute &attrib, NodeInfo inodeinfo, int index,  NodeKeyRecord& nkrec)
//{
//    int i = 0;
//    int ofst = 0;
//    while (i < inodeinfo._numKeys) {
//        NodeKeyRecord fnkrec;
//        getNodeKeyRecord((char*)data+ofst, attrib, fnkrec);
//        if (i == index) {
//            nkrec = fnkrec;
//            return 0;
//        }
//        ofst += fnkrec.length();
//        ++i;
//    }
//    return -1;
//}

int getLeafPageIndexKeyRecord(const void* data, const Attribute &attrib, NodeInfo inodeinfo, int index, LeafKeyRecord& ilkrec)
{
    int i = 0;
    int ofst = 0;
    while (i < inodeinfo._numKeys) {
        LeafKeyRecord flkrec;
        getLeafKeyRecord((char*)data + ofst, attrib, flkrec);
        if (i == index) {
            ilkrec = flkrec;
            return 0;
        }
        ofst += flkrec.length();
        ++i;
    }
    return -1;
}

int getLeafPageOffsetKeyRecord(const void* data, const Attribute &attrib, NodeInfo inodeinfo, int offset, LeafKeyRecord& ilkrec)
{
    if (offset >= inodeinfo._size)
        return -1;

    getLeafKeyRecord((char*)data + offset, attrib, ilkrec);

    return 0;
}

//int getPrevNextLeafPage(IXFileHandle &ixfileHandle, vector<NodeKeyChildInfo> &parents,
//    const Attribute &attrib, PageNum &prevp, PageNum &nextp) {
//    
//    prevp = 0;
//    nextp = 0;
//    
//    NodeKeyChildInfo inkcinfo = parents.back();
//    PageNum pagenum = inkcinfo._curPage;
//
//    void* data = malloc(PAGE_SIZE);
//    ixfileHandle.readPage(pagenum, data);
//
//    NodeInfo nodeinfo;
//    getNodeInfo(data, nodeinfo);
//
//    if (nodeinfo._numKeys == 0) //get 1 level up and check prev and next keys
//        return 0;
//
//    int offset = inkcinfo._nkinfo._offset;
//    int index = inkcinfo._nkinfo._index;
//
//    //int idx2 = index;
//    //if (idx2 == 0) {
//    //    int i = 0;
//    //    int sz = parents.size();
//    //    while (idx2 == 0 & i < sz) {
//    //        NodeKeyChildInfo tnkcinfo = parents[sz - i - 1];
//    //        idx2 = tnkcinfo._nkinfo._index;
//    //        NodeKeyRecord tnkrec = tnkcinfo._nkrec;
//    //        if(inkcinfo._childdir == NodeKeyChildInfo::ChildDir::right)
//    //            nprevp = tnkrec._leftchild
//
//    //        += i;
//    //    }
//    //}
//
//
//    if (index == 0) {
//        if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::left) {
//            prevp = 0;  //get a level up
//            NodeKeyRecord nkrec = inkcinfo._nkrec;
////            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec);
//            nextp = nkrec._rightchild;
//            if (nextp) {
//                void* data2 = malloc(PAGE_SIZE);
//                ixfileHandle.readPage(nextp, data2);
//                NodeInfo nodeinfo2;
//                getNodeInfo(data2, nodeinfo2);
//                prevp = nodeinfo2._prevPage;
//                free(data2);
//            }
//        }
//        else if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::right) {
//            NodeKeyRecord nkrec = inkcinfo._nkrec;
////            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec);
//            prevp = nkrec._leftchild;  //get a level up
//            NodeKeyRecord nkrec2;
////            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index + 1, nkrec2);
//            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec2);
//            nextp = nkrec2._rightchild;
//        }
//    }
//    else if (index == nodeinfo._numKeys - 1) {
//        if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::left) {
//            NodeKeyRecord nkrec;
//            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index-1, nkrec);
//            prevp = nkrec._leftchild;  //get a level up
//            NodeKeyRecord nkrec2;
//            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec2);
//            nextp = nkrec2._rightchild;
//        }
//        else if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::right) {
//            nextp = 0;  //get a level up
//            NodeKeyRecord nkrec;
//            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec);
//            prevp = nkrec._leftchild;
//            if (prevp) {
//                void* data2 = malloc(PAGE_SIZE);
//                ixfileHandle.readPage(prevp, data2);
//                NodeInfo nodeinfo2;
//                getNodeInfo(data2, nodeinfo2);
//                nextp = nodeinfo2._nextPage;
//                free(data2);
//            }
//        }
//    }
//    else {
//        if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::left) {
//            NodeKeyRecord nkrec;
//            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index - 1, nkrec);
//            prevp = nkrec._leftchild;  
//            NodeKeyRecord nkrec2;
//            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec2);
//            nextp = nkrec2._rightchild;
//        }
//        else if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::right) {
//            NodeKeyRecord nkrec;
//            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec);
//            prevp = nkrec._leftchild;  //get a level up
//            NodeKeyRecord nkrec2;
//            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index + 1, nkrec2);
//            nextp = nkrec2._rightchild;
//        }
//    }
//
//    return 0;
//}

int getPrevNextLeafPage(IXFileHandle &ixfileHandle, vector<NodeKeyChildInfo> &parents,
    const Attribute &attrib, PageNum &prevp, PageNum &nextp) {

    prevp = 0;
    nextp = 0;

    NodeKeyChildInfo inkcinfo = parents.back();
    PageNum pagenum = inkcinfo._curPage;

    void* data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pagenum, data);

    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);

    if (nodeinfo._numKeys == 0) //get 1 level up and check prev and next keys
        return 0;

    int offset = inkcinfo._nkinfo._offset;
    int index = inkcinfo._nkinfo._index;

    //    NodeKeyRecord nkrec = inkcinfo._nkrec;

    NodeKeyRecord nkrec;
    getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec);

    if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::left) {
        nextp = nkrec._rightchild;
        if (nextp) {
            void* data2 = malloc(PAGE_SIZE);
            ixfileHandle.readPage(nextp, data2);
            NodeInfo nodeinfo2;
            getNodeInfo(data2, nodeinfo2);
            prevp = nodeinfo2._prevPage;
            free(data2);
        }
    }
    else if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::right) {
        prevp = nkrec._leftchild;
        if (prevp) {
            void* data2 = malloc(PAGE_SIZE);
            ixfileHandle.readPage(prevp, data2);
            NodeInfo nodeinfo2;
            getNodeInfo(data2, nodeinfo2);
            nextp = nodeinfo2._nextPage;
            free(data2);
        }
    }
    return 0;

    //int idx2 = index;
    //if (idx2 == 0) {
    //    int i = 0;
    //    int sz = parents.size();
    //    while (idx2 == 0 & i < sz) {
    //        NodeKeyChildInfo tnkcinfo = parents[sz - i - 1];
    //        idx2 = tnkcinfo._nkinfo._index;
    //        NodeKeyRecord tnkrec = tnkcinfo._nkrec;
    //        if(inkcinfo._childdir == NodeKeyChildInfo::ChildDir::right)
    //            nprevp = tnkrec._leftchild

    //        += i;
    //    }
    //}


    //if (index == 0) {
    //    if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::left) {
    //        prevp = 0;  //get a level up
    //        NodeKeyRecord nkrec = inkcinfo._nkrec;
    //        //            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec);
    //        nextp = nkrec._rightchild;
    //        if (nextp) {
    //            void* data2 = malloc(PAGE_SIZE);
    //            ixfileHandle.readPage(nextp, data2);
    //            NodeInfo nodeinfo2;
    //            getNodeInfo(data2, nodeinfo2);
    //            prevp = nodeinfo2._prevPage;
    //            free(data2);
    //        }
    //    }
    //    else if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::right) {
    //        NodeKeyRecord nkrec = inkcinfo._nkrec;
    //        //            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec);
    //        prevp = nkrec._leftchild;  //get a level up
    //        NodeKeyRecord nkrec2;
    //        //            getNodePageIndexKeyRecord(data, attrib, nodeinfo, index + 1, nkrec2);
    //        getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec2);
    //        nextp = nkrec2._rightchild;
    //    }
    //}
    //else if (index == nodeinfo._numKeys - 1) {
    //    if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::left) {
    //        NodeKeyRecord nkrec;
    //        getNodePageIndexKeyRecord(data, attrib, nodeinfo, index - 1, nkrec);
    //        prevp = nkrec._leftchild;  //get a level up
    //        NodeKeyRecord nkrec2;
    //        getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec2);
    //        nextp = nkrec2._rightchild;
    //    }
    //    else if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::right) {
    //        nextp = 0;  //get a level up
    //        NodeKeyRecord nkrec;
    //        getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec);
    //        prevp = nkrec._leftchild;
    //        if (prevp) {
    //            void* data2 = malloc(PAGE_SIZE);
    //            ixfileHandle.readPage(prevp, data2);
    //            NodeInfo nodeinfo2;
    //            getNodeInfo(data2, nodeinfo2);
    //            nextp = nodeinfo2._nextPage;
    //            free(data2);
    //        }
    //    }
    //}
    //else {
    //    if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::left) {
    //        NodeKeyRecord nkrec;
    //        getNodePageIndexKeyRecord(data, attrib, nodeinfo, index - 1, nkrec);
    //        prevp = nkrec._leftchild;
    //        NodeKeyRecord nkrec2;
    //        getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec2);
    //        nextp = nkrec2._rightchild;
    //    }
    //    else if (inkcinfo._childdir == NodeKeyChildInfo::ChildDir::right) {
    //        NodeKeyRecord nkrec;
    //        getNodePageIndexKeyRecord(data, attrib, nodeinfo, index, nkrec);
    //        prevp = nkrec._leftchild;  //get a level up
    //        NodeKeyRecord nkrec2;
    //        getNodePageIndexKeyRecord(data, attrib, nodeinfo, index + 1, nkrec2);
    //        nextp = nkrec2._rightchild;
    //    }
    //}

    //return 0;
}

int leafPageUpdatePrevNext(IXFileHandle &ixfileHandle, PageNum pagenum, PageNum prevp, PageNum nextp) {

    void* data = malloc(PAGE_SIZE);

    if (pagenum == 0)
        return -1;

    {
        ixfileHandle.readPage(pagenum, data);

        NodeInfo nodeinfo;
        getNodeInfo(data, nodeinfo);
        nodeinfo._prevPage = prevp;
        nodeinfo._nextPage = nextp;
        writeNodeInfo(data, nodeinfo);

        ixfileHandle.writePage(pagenum, data);
    }

    if(prevp != 0)
    {
        ixfileHandle.readPage(prevp, data);

        NodeInfo nodeinfo;
        getNodeInfo(data, nodeinfo);
        nodeinfo._nextPage = pagenum;
        writeNodeInfo(data, nodeinfo);

        ixfileHandle.writePage(prevp, data);
    }

    if (nextp != 0)
    {
        ixfileHandle.readPage(nextp, data);

        NodeInfo nodeinfo;
        getNodeInfo(data, nodeinfo);
        nodeinfo._prevPage = pagenum;
        writeNodeInfo(data, nodeinfo);

        ixfileHandle.writePage(nextp, data);
    }

    free(data);
    return 0;
}

int leafPageJoinPrevNext(IXFileHandle &ixfileHandle, PageNum prevp, PageNum nextp) {

    void* data = malloc(PAGE_SIZE);

    //if (pagenum == 0)
    //    return -1;

    //{
    //    ixfileHandle.readPage(pagenum, data);

    //    NodeInfo nodeinfo;
    //    getNodeInfo(data, nodeinfo);
    //    nodeinfo._prevPage = prevp;
    //    nodeinfo._nextPage = nextp;
    //    writeNodeInfo(data, nodeinfo);

    //    ixfileHandle.writePage(pagenum, data);
    //}

    if (prevp != 0)
    {
        ixfileHandle.readPage(prevp, data);

        NodeInfo nodeinfo;
        getNodeInfo(data, nodeinfo);
        nodeinfo._nextPage = nextp;
        writeNodeInfo(data, nodeinfo);

        ixfileHandle.writePage(prevp, data);
    }

    if (nextp != 0)
    {
        ixfileHandle.readPage(nextp, data);

        NodeInfo nodeinfo;
        getNodeInfo(data, nodeinfo);
        nodeinfo._prevPage = prevp;
        writeNodeInfo(data, nodeinfo);

        ixfileHandle.writePage(nextp, data);
    }

    free(data);
    return 0;
}


bool isLeafPageHasFreeSpace(IXFileHandle &ixfileHandle, PageNum pagenum, LeafKeyRecord lkrec) {
    void* data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pagenum, data);
    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);
    if (lkrec.length() <= PAGE_SIZE - getSizeOfNodeInfo() - nodeinfo._size)
        return true;
    return false;
}

int setFirstPage(IXFileHandle &ixfileHandle, PageNum pagenum) {
    void *data = malloc(PAGE_SIZE);

    ixfileHandle.readPage(pagenum, data);
    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);

    while (nodeinfo._prevPage != 0) {
        pagenum = nodeinfo._prevPage;
        ixfileHandle.readPage(pagenum, data);
        getNodeInfo(data, nodeinfo);
    }

    while (nodeinfo._numKeys == 0) {
        pagenum = nodeinfo._nextPage;
        ixfileHandle.readPage(pagenum, data);
        getNodeInfo(data, nodeinfo);
    }

    ixfileHandle.setFirstPageNum(pagenum);
    return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{

    PageNum rootpage = 0;
    int numpages = ixfileHandle.getNumberOfPages();
    if (numpages == 0) {  //append the root page
        NodeInfo nodeinfo;
        nodeinfo._type = NodeType::KeyNode;
        appendNodePage(ixfileHandle, rootpage, nodeinfo);
        ixfileHandle.setRootPageNum(rootpage);

        NodeKeyRecord nkrec(getKeyChain(key, attribute), 0, 0);
        vector<NodeKeyChildInfo> parents;
        nodePageInsertKey(ixfileHandle, rootpage, attribute, parents, nkrec);
    }

    rootpage = ixfileHandle.getRootPageNum();
    NodeKeyChildInfo nkcinfo;
    vector<NodeKeyChildInfo> parents;
    searchKeyPage(ixfileHandle, rootpage, attribute, key, nkcinfo, parents);
    PageNum pagenum = nkcinfo._child;
    if (pagenum == 0) {     //append leaf page
        NodeInfo nodeinfo;
        PageNum leafpage = 0;
        nodeinfo._type = NodeType::LeafNode;
        appendNodePage(ixfileHandle, leafpage, nodeinfo);

        leafPageInsertKey(ixfileHandle, leafpage, attribute, key, rid);
        nodePageUpdateKey(ixfileHandle, parents.back(), attribute, leafpage);
        PageNum prevp = 0, nextp = 0;
        getPrevNextLeafPage(ixfileHandle, parents, attribute, prevp, nextp);
        leafPageUpdatePrevNext(ixfileHandle, leafpage, prevp, nextp);

        setFirstPage(ixfileHandle, leafpage);

        return 0;
    }
    else {      //insert key in leaf page
        LeafKeyRecord inkrec(getKeyChain(key, attribute), rid);
        if (isLeafPageHasFreeSpace(ixfileHandle, pagenum, inkrec)) {
            leafPageInsertKey(ixfileHandle, pagenum, attribute, key, rid);
        }
        else {
            LeafKeyRecord flkrec(getKeyChain(key, attribute), rid);
            int res = leafPageSplitInsertKey(ixfileHandle, pagenum, attribute, parents, flkrec);
            if (res != 0) 
                return res;
            setFirstPage(ixfileHandle, pagenum);
        }
        return 0;
    }

    return -1;
}


RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    PageNum rootpage = ixfileHandle.getRootPageNum();
    RC stat = -1;
    vector<NodeKeyChildInfo> parents;
    NodeKeyChildInfo nkcinfo;
    stat = searchKeyPage(ixfileHandle, rootpage, attribute, key, nkcinfo, parents);
    if (stat) return stat;

    PageNum pagenum = nkcinfo._child;

    void* data = malloc(PAGE_SIZE);
    stat = ixfileHandle.readPage(pagenum, data);
    if (stat) {
        free(data);
        return stat;
    }

    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);
    if (nodeinfo._type != NodeType::LeafNode) {
        free(data);
        return -1;
    }

    KeyChain ikchain;
    getKeyChain(key, attribute, ikchain);
    LeafKeyRecord ilkrec(ikchain, rid);

    int i = 0;
    int ofst = 0;

    if ((nodeinfo._numKeys == 0) || (nodeinfo._size == 0))
        return -1;

    int totalkeys = 0;

    bool found = false;
    while (true) {
        NodeInfo nodeinfo2(nodeinfo);
        while (i < nodeinfo2._numKeys) {
            LeafKeyRecord flkrec;
            getLeafKeyRecord((char*)data + ofst, attribute, flkrec);
            if (ilkrec.areEquall(flkrec)) {
                int blocksize = ilkrec.length();
                removeBlock(data, ofst, blocksize, nodeinfo._size);
                --nodeinfo2._numKeys;
                nodeinfo2._size -= blocksize;
                writeNodeInfo(data, nodeinfo2);
                ixfileHandle.writePage(pagenum, data);
                found = true;
                break;
            }
            ofst += flkrec.length();
            ++i;
        }
        totalkeys += nodeinfo2._numKeys;
        PageNum ovrfpage = nodeinfo2._overflowPage;
        if (0 == ovrfpage)
            break;

        ixfileHandle.readPage(pagenum, data);
        getNodeInfo(data, nodeinfo2);
    }

    if (!found)
        return -1;

    ixfileHandle.readPage(pagenum, data);
    getNodeInfo(data, nodeinfo);

    if ((nodeinfo._numKeys == 0) || (nodeinfo._size == 0)) {
//    if (totalkeys == 0) {
        nodePageUpdateKey(ixfileHandle, parents.back(), attribute, 0);
        PageNum prevp = 0, nextp = 0;
        getPrevNextLeafPage(ixfileHandle, parents, attribute, prevp, nextp);
        void* data = malloc(PAGE_SIZE);
//        leafPageUpdatePrevNext(ixfileHandle, leafpage, prevp, nextp);
        leafPageJoinPrevNext(ixfileHandle, prevp, nextp);
        if (prevp)
            setFirstPage(ixfileHandle, prevp);
        else if (nextp)
            setFirstPage(ixfileHandle, nextp);
    }

    return 0;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    if (!ixfileHandle.isOpen())
        return - 1;
    ix_ScanIterator._ixfileHandle = ixfileHandle;
    ix_ScanIterator._attribute = attribute;
    ix_ScanIterator._lowKey = (void*)lowKey;
    ix_ScanIterator._highKey = (void*)highKey;
    ix_ScanIterator._lowKeyInclusive = lowKeyInclusive;
    ix_ScanIterator._highKeyInclusive = highKeyInclusive;
    ix_ScanIterator._curPageData = malloc(PAGE_SIZE);
    ix_ScanIterator._curPageNum = 0;

    return 0;
}

int printBtreeJsonLeaf(const void* data, const Attribute &attribute, PageNum curpage, int indent)
{
    RC stat = 0;

    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);

    int offset = 0;
    cout << string(indent, ' ') << "{\"keys\":";
    cout << " [";
    int i = 0;
    while (i < nodeinfo._numKeys) {
        AttrType type = attribute.type;
        if (type == TypeVarChar) {
            int len = 0;
            memcpy((char*)&len, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            char rkey[PAGE_SIZE];
            memcpy(rkey, (char*)data + offset, len * sizeof(char));
            offset += len;
            rkey[len] = '\0';
            int ridpagenum=0;
            memcpy((char*)&ridpagenum, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            int ridslotnum = 0;
            memcpy((char*)&ridslotnum, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            if (0 == i)
                cout << "\"" << rkey << ":[(" << ridpagenum << "," << ridslotnum << ")]\"";
            else
                cout << ",\"" << rkey << ":[(" << ridpagenum << "," << ridslotnum << ")]\"";
        }
        else if (type == TypeInt) {
            int val = 0;
            memcpy((char*)&val, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            int ridpagenum = 0;
            memcpy((char*)&ridpagenum, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            int ridslotnum = 0;
            memcpy((char*)&ridslotnum, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            if (0 == i)
                cout << "\"" << val << ":[(" << ridpagenum << "," << ridslotnum << ")]\"";
            else
                cout << ",\"" << val << ":[(" << ridpagenum << "," << ridslotnum << ")]\"";
        }
        else if (type == TypeReal) {
            float val = 0;
            memcpy((char*)&val, (char*)data + offset, sizeof(int));
            offset += sizeof(float);
            int ridpagenum = 0;
            memcpy((char*)&ridpagenum, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            int ridslotnum = 0;
            memcpy((char*)&ridslotnum, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            if (0 == i)
                cout << "\"" << val << ":[(" << ridpagenum << "," << ridslotnum << ")]\"";
            else
                cout << ",\"" << val << ":[(" << ridpagenum << "," << ridslotnum << ")]\"";
        }
        ++i;
    }
//    cout << "]}" << endl;
    cout << "]}";
    return 0;

}

int printBtreeJson(IXFileHandle &ixfileHandle, const Attribute &attribute, PageNum curpage, int indent) 
{    
    if (curpage <= 0)
        return 0;
    RC stat = 0;
    void* data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(curpage, data);

    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);

    if (nodeinfo._type == NodeType::LeafNode) {
        return printBtreeJsonLeaf(data, attribute, curpage, indent+4);
    }

    int offset = 0;
    cout << string(indent, ' ') << "{\"keys\":";
    cout << "[";
    int i = 0;
    vector<PageNum> childrens;
    while (i < nodeinfo._numKeys) {
        AttrType type = attribute.type;
        if (0 == i) {
            PageNum leftchild = 0;
            memcpy((char*)&leftchild, (char*)data + offset, sizeof(int));
            if(leftchild > 0)
                childrens.push_back(leftchild);
        }
        offset += sizeof(int);
        if (type == TypeVarChar) {
            int len = 0;
            memcpy((char*)&len, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            char rkey[PAGE_SIZE];
            memcpy(rkey, (char*)data + offset, len * sizeof(char));
            offset += len;
            rkey[len] = '\0';
            if(0==i)
                cout << "\"" << rkey << "\"";
            else 
                cout << ",\"" << rkey << "\"";
        }
        else if (type == TypeInt) {
            int val = 0;
            memcpy((char*)&val, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            if (0 == i)
                cout << val ;
            else
                cout << "," << val;
        }
        else if (type == TypeReal) {
            float val = 0;
            memcpy((char*)&val, (char*)data + offset, sizeof(int));
            offset += sizeof(float);
            if (0 == i)
                cout << val;
            else
                cout << "," << val;
        }
        PageNum rightchild = 0;
        memcpy((char*)&rightchild, (char*)data + offset, sizeof(int));
        offset += sizeof(int);
        if (rightchild > 0)
            childrens.push_back(rightchild);

        ++i;
    }
    cout << "]," << endl;;
    cout << string(indent, ' ') << "\"children\":[" << endl;
    for (int i = 0; i < childrens.size(); ++i) {
        PageNum pagenum = childrens[i];
        printBtreeJson(ixfileHandle, attribute, pagenum, indent+4);
        
        if (i != childrens.size() - 1)
            cout << "," << endl;
        else
            cout << endl;
    }
    cout << string(indent, ' ') << "]" << endl;

    cout << string(indent, ' ') << "}" ;
    free(data);
    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    PageNum rootpage = ixfileHandle.getRootPageNum();
    int indent = 0;
    printBtreeJson(ixfileHandle, attribute, rootpage, indent);
    cout << endl;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}


int getLowestKey(IXFileHandle& ixfileHandle, PageNum rootPage, Attribute attrib, void* lowestKey)
{
    void* data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(rootPage, data);

    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);

    int i = 0;
    int ofst = 0;
    while (i < nodeinfo._numKeys) {
        NodeKeyRecord nkrec;
        getNodeKeyRecord((char*)data+ofst, attrib, nkrec);

        PageNum child = nkrec._leftchild;
        if (nkrec._leftchild == 0) {
            child = nkrec._rightchild;
        }
        if (child == 0) {
            ofst += nkrec.length();
            ++i;
            continue;
        }
        //if (child) {
        //    NodeInfo nodeinfo2(nodeinfo);
        //    ixfileHandle.readPage(child, data);
        //    getNodeInfo(data, nodeinfo2);

        //    if (nodeinfo2._numKeys == 0) {
        //        ofst += nkrec.length();
        //        ++i;
        //        continue;
        //    }
        //}

        while (nodeinfo._type != NodeType::LeafNode) {
            NodeKeyRecord nkrec;
            getNodeKeyRecord(data, attrib, nkrec);

            PageNum child = nkrec._leftchild;
            if (nkrec._leftchild == 0) {
                child = nkrec._rightchild;
            }
            if (child == 0) {
                break;
            }

            ixfileHandle.readPage(child, data);
            getNodeInfo(data, nodeinfo);

            if (nodeinfo._numKeys == 0)
                break;

            if (nodeinfo._type == NodeType::LeafNode) {
                LeafKeyRecord flkrec;
                getLeafKeyRecord(data, attrib, flkrec);
                flkrec._kchain.getKey(lowestKey);
                break;
            }
        }
    }
    free(data);
    return 0;
}


int getHighestKey(IXFileHandle& ixfileHandle, PageNum rootPage, Attribute attrib, void* highestKey)
{
    void* data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(rootPage, data);

    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);

    while (nodeinfo._type != NodeType::LeafNode) {
        int idx = nodeinfo._numKeys - 1;
        NodeKeyRecord fnkrec;
        getNodePageIndexKeyRecord(data, attrib, nodeinfo, idx, fnkrec);

        PageNum child = fnkrec._rightchild;
        if (fnkrec._rightchild == 0) {
            child = fnkrec._leftchild;
        }
        if (child == 0)
            return -1;

        ixfileHandle.readPage(child, data);
        getNodeInfo(data, nodeinfo);

        if (nodeinfo._type == NodeType::LeafNode) {
            LeafKeyRecord flkrec;
            int idx = nodeinfo._numKeys - 1;
            getLeafPageIndexKeyRecord(data, attrib, nodeinfo, idx, flkrec);
            flkrec._kchain.getKey(highestKey);
            break;
        }
    }

    free(data);
    return 0;
}

int getLeafPageKeyInfo(const void* data, const Attribute &attrib, 
    KeyChain ikchain, NodeKeyInfo &nkinfo, LeafKeyRecord &lkrec) {

    NodeInfo nodeinfo;
    getNodeInfo(data, nodeinfo);

    int i = 0;
    int ofst = 0;
    while (i < nodeinfo._numKeys) {
        LeafKeyRecord flkrec;
        getLeafKeyRecord((char*)data + ofst, attrib, flkrec);
        if (ikchain.compare(flkrec._kchain) == 0) {
            nkinfo._index = i;
            nkinfo._offset = ofst;
            nkinfo._present = true;
            lkrec = flkrec;
            break;
        }
        else if (ikchain.compare(flkrec._kchain) < 0) {
            nkinfo._index = i;
            nkinfo._offset = ofst;
            nkinfo._present = false;
            lkrec = flkrec;
            break;
        }
        ofst += flkrec.length();
        ++i;
        if (i == nodeinfo._numKeys) {
            nkinfo._index = -2;
            nkinfo._offset = ofst;
            nkinfo._present = false;
            lkrec = flkrec;
            break;
        }
    }
    return 0;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    int retval = 0;
    if (_curPageNum == 0) {
        PageNum rootPage = _ixfileHandle.getRootPageNum();
        //if (nullptr == _lowKey) {
        //    _dealloc_lowkey = true;
        //    _lowKey = malloc(PAGE_SIZE);
        //    getLowestKey(_ixfileHandle, rootPage, _attribute, _lowKey);
        //}
        if (nullptr == _lowKey) {
            _curPageNum = _ixfileHandle.getFirstPageNum();
        }
        else {
            vector<NodeKeyChildInfo> parents;
            NodeKeyChildInfo nkcinfo;
            searchKeyPage(_ixfileHandle, rootPage, _attribute, _lowKey, nkcinfo, parents);
            _curPageNum = nkcinfo._child;
            getKeyChain(_lowKey, _attribute, _lowKeyChain);
        }

        //if (nullptr == _highKey) {
        //    _dealloc_highkey = true;
        //    _highKey = malloc(PAGE_SIZE);
        //    getHighestKey(_ixfileHandle, rootPage, _attribute, _highKey);
        //}
        if (nullptr != _highKey) {
            getKeyChain(_highKey, _attribute, _highKeyChain);
        }


        if (_curPageNum == 0)
            return IX_EOF;

        //getKeyChain(_lowKey, _attribute, _lowKeyChain);
        
        //getKeyChain(_highKey, _attribute, _highKeyChain);
        _ixfileHandle.readPage(_curPageNum, _curPageData);

        NodeInfo nodeinfo;
        getNodeInfo(_curPageData, nodeinfo);

        if (nullptr == _lowKey) {
            _curKeyIndex = 0;
            _curKeyOffset = 0;
            return getNextEntry(rid, key);
        }

        //_ixfileHandle.readPage(_curPageNum, _curPageData);

        //NodeInfo nodeinfo;
        //getNodeInfo(_curPageData, nodeinfo);

        NodeKeyInfo lkinfo;
        LeafKeyRecord lkrec;
        getLeafPageKeyInfo(_curPageData, _attribute, _lowKeyChain, lkinfo, lkrec);
        _curKeyIndex = lkinfo._index;
        _curKeyOffset = lkinfo._offset;
        if (lkinfo._present) {
            if (_lowKeyInclusive) {
                lkrec.getKey(key);
                rid = lkrec._rid;
                ++_curKeyIndex; ;
                _curKeyOffset += lkrec.length();
                return 0;
            }
            else {
                ++_curKeyIndex;
                _curKeyOffset += lkrec.length();
                return getNextEntry(rid, key);
            }
        }
        else {
            if (lkinfo._index == -2) {
                _curKeyIndex = nodeinfo._numKeys;
                _curKeyOffset = 0;
                return getNextEntry(rid, key);
            }

            lkrec.getKey(key);
            rid = lkrec._rid;
            ++_curKeyIndex; ;
            _curKeyOffset += lkrec.length();
            return 0;
        }
        return 0;
    }

    NodeInfo nodeinfo;
    getNodeInfo(_curPageData, nodeinfo);

    if (_curKeyIndex == nodeinfo._numKeys)
    {
        _curKeyIndex = 0;
        _curKeyOffset = 0;
        if (nodeinfo._overflowPage) {
            _curPageNum = nodeinfo._overflowPage;
        }
        else {
            _curPageNum = nodeinfo._nextPage;
        }

        if (_curPageNum == 0)
            return IX_EOF;

        _ixfileHandle.readPage(_curPageNum, _curPageData);
        getNodeInfo(_curPageData, nodeinfo);
    }

    LeafKeyRecord lkrec;
    getLeafPageOffsetKeyRecord(_curPageData, _attribute, nodeinfo, _curKeyOffset, lkrec);
    
    if (_highKeyInclusive) {
        if (_highKey && _highKeyChain.compare(lkrec._kchain) < 0)
            return IX_EOF;
    }
    else {
        if (_highKey && _highKeyChain.compare(lkrec._kchain) <= 0)
            return IX_EOF;
    }
               
    ++_curKeyIndex;
    _curKeyOffset += lkrec.length();
    lkrec.getKey(key);
    rid = lkrec._rid;

    return 0;
}

RC IX_ScanIterator::close()
{
    if (_lowKey && _dealloc_lowkey) {
        free(_lowKey);
        _lowKey = nullptr;
        _dealloc_lowkey = false;
    }
    if (_highKey && _dealloc_highkey) {
        free(_highKey);
        _highKey = nullptr;
        _dealloc_highkey = false;
    }
    if (_curPageData) {
        free(_curPageData);
        _curPageData = nullptr;
    }
    _curPageNum = 0;
    return 0;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    _fp = NULL;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    int stat = readCountersFromFile();
    if (stat) return stat;

    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    
    return 0;
}


int IXFileHandle::openFile(const string &fname)
{
    if (_fp != NULL)
        return -1;

    struct stat stFileInfo;
    if (stat(fname.c_str(), &stFileInfo) != 0) {
        return -1;
        //_fp = fopen(fname.c_str(), "wb");
        //if (_fp == NULL)
        //    return -1;
        //fclose(_fp);
    }

    _fp = fopen(fname.c_str(), "r+b");
    if (_fp == NULL)
        return -1;
    
    int stat = 0;
    if (getTotalNumberOfPages() == 0) {
        stat = initFile();
        if (stat) return stat;
    }  else {
        stat = readCountersFromFile();
        if (stat) return stat;
    }

    return 0;
}

int IXFileHandle::closeFile()
{
    if (_fp == NULL)
        return -1;

    fclose(_fp);
    _fp = NULL;
    return 0;
}

int IXFileHandle::getTotalNumberOfPages()
{
    if (NULL == _fp)
        return -1;
    if (fseek(_fp, 0, SEEK_END)) return -1;
    //    fseek(fp, 0L, SEEK_END);
    long sz = ftell(_fp);
    return ceil(sz / (double)PAGE_SIZE);
}

int IXFileHandle::updataHeaderPage()
{
    if (NULL == _fp)
        return -1;

    void* data = malloc(PAGE_SIZE);
    int offset = 0;
    memcpy((char*)data + offset, (char*)&ixReadPageCounter, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)data + offset, (char*)&ixWritePageCounter, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)data + offset, (char*)&ixAppendPageCounter, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)data + offset, (char*)&ixRootPageNum, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)data + offset, (char*)&ixFirstPageNum, sizeof(int));

    rewind(_fp);
    if (fseek(_fp, HEADER_PAGE_OFFSET, SEEK_SET)) return -1;
    if (PAGE_SIZE != fwrite(data, 1, PAGE_SIZE, _fp)) return -1;
    fflush(_fp);

    free(data);

    return 0;

}

int IXFileHandle::initFile()
{
    if (NULL == _fp)
        return -1;

    return updataHeaderPage();

}

int IXFileHandle::readCountersFromFile()
{
    if (NULL == _fp)
        return -1;

    void* data = malloc(PAGE_SIZE);

    rewind(_fp);
    if (fseek(_fp, HEADER_PAGE_OFFSET, SEEK_SET)) return -1;
    if (PAGE_SIZE != fread(data, 1, PAGE_SIZE, _fp)) return -1;

    int offset = 0;
    memcpy((char*)&ixReadPageCounter, (char*)data + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)&ixWritePageCounter, (char*)data + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)&ixAppendPageCounter, (char*)data + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)&ixRootPageNum, (char*)data + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)&ixFirstPageNum, (char*)data + offset, sizeof(int));

    free(data);

    return 0;

}


RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    if (NULL == _fp)
        return -1;
    size_t pgoffset = (pageNum + DATA_PAGE_OFFSET)*PAGE_SIZE;
    rewind(_fp);
    if (fseek(_fp, pgoffset, SEEK_SET)) return -1;
    if (PAGE_SIZE != fread(data, 1, PAGE_SIZE, _fp)) return -1;
    ixReadPageCounter++;

    int stat = updataHeaderPage();
    if (stat) return stat;

    return 0;
}


RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
    if (NULL == _fp)
        return -1;
    //    if(pageNum > appendPageCounter)
    //        return -1;
    if (pageNum > getNumberOfPages())
        return -1;
    size_t pgoffset = (pageNum + DATA_PAGE_OFFSET)*PAGE_SIZE;
    rewind(_fp);
    if (fseek(_fp, pgoffset, SEEK_SET)) return -1;
    if (PAGE_SIZE != fwrite(data, 1, PAGE_SIZE, _fp)) return -1;
    fflush(_fp);
    ixWritePageCounter++;

    int stat = updataHeaderPage();
    if (stat) return stat;

    return 0;
}


RC IXFileHandle::appendPage(const void *data)
{
    if (NULL == _fp)
        return -1;
    if (fseek(_fp, 0, SEEK_END)) return -1;
    if (PAGE_SIZE != fwrite(data, 1, PAGE_SIZE, _fp)) return -1;
    fflush(_fp);
    ixAppendPageCounter++;

    int stat = updataHeaderPage();
    if (stat) return stat;

    return 0;
}


unsigned IXFileHandle::getNumberOfPages()
{
    int stat = 0;
    stat = readCountersFromFile();
    if (stat) return stat;

    return ixAppendPageCounter;
}

