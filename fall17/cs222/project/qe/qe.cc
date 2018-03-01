#include <set>
#include <map>
#include <cassert>

#include "qe.h"

int getConditionRecordField(const Condition &cond, RecordField &datarec)
{
    datarec._isnull = false;
    int offset = 0;
    datarec._attrib.type = cond.rhsValue.type;
    AttrType atype = cond.rhsValue.type;
    const void *data = cond.rhsValue.data;
    if (atype == TypeVarChar) {
        int length;
        memcpy(&length, (char*)data + offset, sizeof(int));
        offset += sizeof(int);
        char buff[PAGE_SIZE];
        memcpy(buff, (char*)data + offset, length);
        buff[length] = '\0';
        offset += length;
        datarec._strkey = buff;
    }
    else if (atype == TypeInt) {
        int val;
        memcpy(&val, (char*)data + offset, sizeof(int));
        offset += sizeof(int);
        datarec._intkey = val;
    }
    if (atype == TypeReal) {
        float val;
        memcpy(&val, (char*)data + offset, sizeof(int));
        offset += sizeof(float);
        datarec._realkey = val;
    }

    return 0;
}


Filter::Filter(Iterator* input, const Condition &condition)
    :_itr(input), _condition(condition)
{}

RC Filter::getNextTuple(void *data) 
{ 
    vector<Attribute> attrs;
    _itr->getAttributes(attrs);

    string atrbname = _condition.lhsAttr;

    RC rc = 0;
    while (rc != IX_EOF) {
        rc = _itr->getNextTuple(data);
        if (rc) return rc;

        map<string, RecordField> datrec;
        getDataRecordFields(attrs, data, datrec);

        if (!_condition.bRhsIsAttr) {
            RecordField condfld;
            getConditionRecordField(_condition, condfld);
            map<string, RecordField>::iterator it = datrec.find(atrbname);
            if (it == datrec.end())
                return -1;
            const RecordField &datfld = it->second;
            int cval = datfld.compareKey(condfld);
            if (cval == -1 && (_condition.op == LT_OP || _condition.op == LE_OP))
                break;
            if (cval == 0 && (_condition.op == LE_OP ||
                _condition.op == EQ_OP || _condition.op == GE_OP))
                break;
            if (cval == 1 && (_condition.op == GE_OP || _condition.op == GT_OP))
                break;
        }
        else {
            map<string, RecordField>::iterator it = datrec.find(atrbname);
            if (it == datrec.end())
                return -1;

            map<string, RecordField>::iterator it2 = datrec.find(_condition.rhsAttr);
            if (it2 == datrec.end())
                return -1;

            const RecordField &datfld1 = it->second;
            const RecordField &datfld2 = it2->second;
            int cval = datfld1.compareKey(datfld2);
            if (cval == -1 && _condition.op == LT_OP)
                break;
            if (cval == 0 && (_condition.op == LE_OP ||
                _condition.op == EQ_OP || _condition.op == GE_OP))
                break;
            if (cval == 1 && _condition.op == GT_OP)
                break;
        }
    }
    return 0; 
}

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const
{
    return _itr->getAttributes(attrs);
}


Project::Project(Iterator *input,                    // Iterator of input R
    const vector<string> &attrNames) 
    :_itr(input), _attrNames(attrNames)
{
    vector<Attribute> fattrs;
    _itr->getAttributes(fattrs);

    set<string> attrset(_attrNames.begin(), _attrNames.end());

    for (int i = 0; i < fattrs.size(); ++i) {
        if (attrset.end() == attrset.find(fattrs[i].name))
            continue;
        _attribs.push_back(fattrs[i]);
    }
};   // vector containing attribute names

RC Project::getNextTuple(void *data) 
{ 
    vector<Attribute> attrs;
    _itr->getAttributes(attrs);

    RC rc = 0;
    void *data2 = malloc(PAGE_SIZE);
    rc = _itr->getNextTuple(data2);
    if (rc) return rc;

    map<string, RecordField> datrec;
    getDataRecordFields(attrs, data2, datrec);

    int nfields = _attrNames.size();
    int nnullbytes = ceil(nfields / 8.0);
    int offset = nnullbytes;
    unsigned char* nullbytes = new unsigned char[nnullbytes];
    memset(nullbytes, 0, nnullbytes);
//    void* data3 = malloc(PAGE_SIZE);
    for (int i = 0; i< nfields; ++i) {
        string atbname = _attrNames[i];
        map<string, RecordField>::iterator it = datrec.find(atbname);
        if (it == datrec.end()) {
            nullbytes[i / 8] |= 1 << (7 - i % 8);
            continue;
        }

        const RecordField &recfld = it->second;
        if (recfld._isnull) {
            nullbytes[i/8] |= 1 << (7 - i % 8);
            continue;
        }
        char key[PAGE_SIZE];
        recfld.getKey(key);
        int len = recfld.length();
//        memcpy((char*)data3 + offset, key, len);
        memcpy((char*)data + offset, key, len);
        offset += len;
    }

    memcpy((char*)data, nullbytes, nnullbytes);

    free(data2);


    return 0; 
};
// For attribute in vector<Attribute>, name it as rel.attr

void Project::getAttributes(vector<Attribute> &attrs) const 
{
    attrs =  _attribs;
};

//--------------------------------------------------------------------------------------


BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
    TableScan *rightIn,           // TableScan Iterator of input S
    const Condition &condition,   // Join condition
    const unsigned numPages       // # of pages that can be loaded into memory,
                                  //   i.e., memory block size (decided by the optimizer)
)
    :_leftin(leftIn), _rightin(rightIn), _condition(condition), _numPages(numPages)
{};

RC BNLJoin::freeTupleBlock()
{
    for (multimap<RecordField, void*>::iterator it = _tupleblock.begin();
        it != _tupleblock.end(); ++it) {
        free(it->second);
        it->second = nullptr;
    }
    _tupleblock.clear();

    return 0;
}

RC BNLJoin::getNextTupleBlock()
{
    freeTupleBlock();

    if (_blockend)
        return QE_EOF;

    vector<Attribute> lattrs;
    _leftin->getAttributes(lattrs);

    Attribute lhsattrib;
    for (const Attribute &attb : lattrs) {
        if (attb.name.compare(_condition.lhsAttr) == 0) {
            lhsattrib = attb;
            break;
        }
    }


    RC rc = 0;
    int totsz = 0;
    void *data2 = malloc(PAGE_SIZE);
    while (totsz < _numPages*PAGE_SIZE) {
        rc = _leftin->getNextTuple(data2);
        if (rc) {
            _blockend = true;
            break;
        }
        RecFldMap datarec;
        getDataRecordFields(lattrs, data2, datarec);
        RecFldMap::iterator it = datarec.find(_condition.lhsAttr);
        if (datarec.end() == it)
            continue;
        const RecordField & krecfld = it->second;
        if (krecfld._isnull)
            continue;
        int len = getDataSize(lattrs, data2);
        void* val = malloc(len);
        memcpy(val, data2, len);
        _tupleblock.insert(make_pair(krecfld, val));
        totsz += len + krecfld.length();
    }
    free(data2);

    return 0;
}

RC BNLJoin::getNextTuple(void *data)
{ 
    //read in numPages*PAGE_SIZE size tuples from the leaftin
    //store tham in a maphashes wrt condition attribute
    vector<Attribute> lattrs;
    _leftin->getAttributes(lattrs);

    Attribute lhsattrib;
    for (const Attribute &attb : lattrs) {
        if (attb.name.compare(_condition.lhsAttr) == 0) {
            lhsattrib = attb;
            break;
        }
    }

    vector<Attribute> rattrs;
    _rightin->getAttributes(rattrs);

    Attribute rhsattrib;
    for (const Attribute &attb : rattrs) {
        if (attb.name.compare(_condition.rhsAttr) == 0) {
            rhsattrib = attb;
            break;
        }
    }

    if (_firsttime) {
        _firsttime = false;
        RC rc = getNextTupleBlock();
        if (rc) return QE_EOF;
    }
    
    RC rc = 0;
    void *data2 = malloc(PAGE_SIZE);
    while (true) {
        rc = _rightin->getNextTuple(data2);
        if (rc) {
            rc = getNextTupleBlock();
            if (rc) {
                free(data2);
                return QE_EOF;
            }
            _rightin->setIterator();
        }
        RecFldMap datarec;
        getDataRecordFields(rattrs, data2, datarec);

        //check conditions is assumed to be EQ_OP
        assert(_condition.op == EQ_OP);
        RecFldMap::iterator it = datarec.find(_condition.rhsAttr);
        if (datarec.end() == it)
            continue;
    
        const RecordField & krecfld = it->second;
        if (krecfld._isnull)
            continue;

        multimap<RecordField, void*>::iterator it2 = _tupleblock.find(krecfld);
        if (_tupleblock.end() == it2)
            continue;

        const void* data3 = it2->second;

        int len = getDataSize(lattrs, data3);

        //join lhs rhs data
        int nlhs = lattrs.size();
        int nrhs = rattrs.size();
        int nnullbytes = ceil((nlhs + nrhs) / 8.0);
        char* nb1 = new char[nnullbytes];
        memset(nb1, 0, nnullbytes);
        char* nb2 = new char[nnullbytes];
        memset(nb2, 0, nnullbytes);

        int nnb1 = ceil(nlhs / 8.0);
        int nnb2 = ceil(nrhs / 8.0);
        memcpy((char*)nb1, (char*)data3, nnb1);
        memcpy((char*)nb2, (char*)data2, nnb2);

        *nb1 = *nb1 | (*nb2 >> nlhs);
        memcpy((char*)data, (char*)nb1, nnullbytes);
        int ofst = nnullbytes;
        memcpy((char*)data + ofst, (char*)data3 + nnb1, len-nnb1);
        ofst += len - nnb1;
        int len2 = getDataSize(rattrs, data2);
        memcpy((char*)data + ofst, (char*)data2 + nnb2, len2 - nnb2);
        ofst += len2 - nnb2;
        break;
    }
    free(data2);


    return 0; 
};
// For attribute in vector<Attribute>, name it as rel.attr

void BNLJoin::getAttributes(vector<Attribute> &attrs) const
{
    vector<Attribute> lattrs;
    _leftin->getAttributes(lattrs);

    vector<Attribute> rattrs;
    _rightin->getAttributes(rattrs);

    attrs.insert(attrs.end(), lattrs.begin(), lattrs.end());
    attrs.insert(attrs.end(), rattrs.begin(), rattrs.end());
};

//--------------------------------------------------------------------------------------

INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
    IndexScan *rightIn,          // IndexScan Iterator of input S
    const Condition &condition   // Join condition
) :_leftin(leftIn), _rightin(rightIn), _condition(condition)
{};

RC INLJoin::getNextTuple(void *data) 
{
    vector<Attribute> lattrs;
    _leftin->getAttributes(lattrs);

    Attribute lhsattrib;
    for (const Attribute &attb : lattrs) {
        if (attb.name.compare(_condition.lhsAttr) == 0) {
            lhsattrib = attb;
            break;
        }
    }

    vector<Attribute> rattrs;
    _rightin->getAttributes(rattrs);

    Attribute rhsattrib;
    for (const Attribute &attb : rattrs) {
        if (attb.name.compare(_condition.rhsAttr) == 0) {
            rhsattrib = attb;
            break;
        }
    }

    RC rc = 0;
    int totsz = 0;
    void *data2 = malloc(PAGE_SIZE);
    while (true) {
        rc = _leftin->getNextTuple(data2);
        if (rc) {
            free(data2);
            return rc;
        }
        

        RecFldMap datarec;
        getDataRecordFields(lattrs, data2, datarec);
        RecFldMap::iterator it = datarec.find(_condition.lhsAttr);
        if (datarec.end() == it)
            continue;
        const RecordField & krecfld = it->second;
        if (krecfld._isnull)
            continue;

        char lowkey[PAGE_SIZE];
        krecfld.getKey(lowkey);
        _rightin->setIterator(lowkey, lowkey, true, false);
        void* data3 = malloc(PAGE_SIZE);
        RC rc2 = _rightin->getNextTuple(data3);
        if (rc2) 
            continue;

        int len1 = getDataSize(lattrs, data2);
        int len2 = getDataSize(rattrs, data3);
        
        //join lhs rhs data
        int nlhs = lattrs.size();
        int nrhs = rattrs.size();
        int nnullbytes = ceil((nlhs + nrhs) / 8.0);
        char* nb1 = new char[nnullbytes];
        memset(nb1, 0, nnullbytes);
        char* nb2 = new char[nnullbytes];
        memset(nb2, 0, nnullbytes);

        int nnb1 = ceil(nlhs / 8.0);
        int nnb2 = ceil(nrhs / 8.0);
        memcpy((char*)nb1, (char*)data2, nnb1);
        memcpy((char*)nb2, (char*)data3, nnb2);

        *nb1 = *nb1 | (*nb2 >> nlhs);
        memcpy((char*)data, (char*)nb1, nnullbytes);
        int ofst = nnullbytes;
        memcpy((char*)data + ofst, (char*)data2 + nnb1, len1 - nnb1);
        ofst += len1 - nnb1;
        memcpy((char*)data + ofst, (char*)data3 + nnb2, len2 - nnb2);
        ofst += len2 - nnb2;

        free(data3);
        break;
    }
    free(data2);


    return 0;
};

// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
    vector<Attribute> lattrs;
    _leftin->getAttributes(lattrs);

    vector<Attribute> rattrs;
    _rightin->getAttributes(rattrs);

    attrs.insert(attrs.end(), lattrs.begin(), lattrs.end());
    attrs.insert(attrs.end(), rattrs.begin(), rattrs.end());
};

//--------------------------------------------------------------------------------------

int GHJoin::_joincount = 0;


GHJoin::GHJoin(Iterator *leftIn,               // Iterator of input R
    Iterator *rightIn,               // Iterator of input S
    const Condition &condition,      // Join condition (CompOp is always EQ)
    const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
) :_leftin(leftIn), _rightin(rightIn), _condition(condition)
, _numPartitions(numPartitions), _isinit(false), _isterminated(false)
{
    ++_joincount;
    _curjoinindx = _joincount;
    
    _leftin->getAttributes(_lattrs);
    _rightin->getAttributes(_rattrs);

};


string GHJoin::getPartitionName(bool isleft, int hashindx)
{
    string jname;
    if (isleft) {
        jname = "left_join_" + to_string(_curjoinindx) + "_" + to_string(hashindx);
    }
    else {
        jname = "right_join_" + to_string(_curjoinindx) + "_" + to_string(hashindx);
    }
    return jname;
}

int GHJoin::createPartition(const string& partname, FileHandle& fhndl)
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    RC stat = 0;

    stat = rbfm->createFile(partname);

//    FileHandle partfhndl;
//    stat = rbfm->openFile(partname, partfhndl);

//    stat = rbfm->closeFile(partfhndl);
    stat = rbfm->openFile(partname, fhndl);

    return 0;
}


int GHJoin::addToPartition(bool isleft, const RecordField& krecfld, 
    const vector<Attribute>& attrs, const void* data)
{
    size_t rechash = krecfld.getKeyHash();
    size_t keyhash = rechash%_numPartitions;

//    FileHandle pfhndl;
    if (isleft) {
        if (keyhash == 0)
            ++_debugcount;

         PartitionInfo& partinfo = _lpartitions[keyhash];
         if (false == partinfo._isinit) {
             string pname = getPartitionName(isleft, keyhash);
             createPartition(pname, partinfo._fhndl);
             partinfo._filename = pname;
             partinfo._isinit = true;
             partinfo._isleft = isleft;
         }
         RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
         RC stat = 0;

         RID rid;
         stat = rbfm->insertRecord(partinfo._fhndl, attrs, data, rid);
         if (stat)
             return stat;
    }
    else {
        PartitionInfo& partinfo = _rpartitions[keyhash];
        if (false == partinfo._isinit) {
            string pname = getPartitionName(isleft, keyhash);
            createPartition(pname, partinfo._fhndl);
            partinfo._filename = pname;
            partinfo._isinit = true;
            partinfo._isleft = isleft;
        }
        RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
        RC stat = 0;

        RID rid;
        stat = rbfm->insertRecord(partinfo._fhndl, attrs, data, rid);
        if (stat)
            return stat;
    }
    
    //if (false == partinfo._isinit) {
    //    string pname = getPartitionName(isleft, keyhash);
    //    createPartition(pname, partinfo._fhndl);
    //    partinfo._filename = pname;
    //    partinfo._isinit = true;
    //    partinfo._isleft = isleft;
    //    if (isleft)
    //        _lpartitions[keyhash] = partinfo;
    //    else
    //        _rpartitions[keyhash] = partinfo;
    //}


    //RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    //RC stat = 0;

    //RID rid;
    //stat = rbfm->insertRecord(partinfo._fhndl, attrs, data, rid);
    //if (stat)
    //    return stat;
    
    return 0;
}


int GHJoin::process()
{
    if (_isinit)
        return 0;

    _lpartitions.resize(_numPartitions);
    _rpartitions.resize(_numPartitions);

    RC rc = 0;
    int totsz = 0;
    char data2[PAGE_SIZE];

    int count = 0;
    bool isleft = true;
    while (true) {
        rc = _leftin->getNextTuple(data2);
        if (rc) 
            break;
        

        RecFldMap datarec;
        getDataRecordFields(_lattrs, data2, datarec);

        RecFldMap::iterator it = datarec.find(_condition.lhsAttr);
        if (datarec.end() == it)
            continue;
        const RecordField & krecfld = it->second;
        if (krecfld._isnull)
            continue;

        addToPartition(isleft, krecfld, _lattrs, data2);
        ++count;
    }

    isleft = false;
    while (true) {
        rc = _rightin->getNextTuple(data2);
        if (rc)
            break;


        RecFldMap datarec;
        getDataRecordFields(_rattrs, data2, datarec);

        RecFldMap::iterator it = datarec.find(_condition.rhsAttr);
        if (datarec.end() == it)
            continue;
        const RecordField & krecfld = it->second;
        if (krecfld._isnull)
            continue;

        addToPartition(isleft, krecfld, _rattrs, data2);
        ++count;
    }

    for (int i = 0; i < _numPartitions; ++i) {
        _lpartitions[i]._fhndl.closeFile();
        _rpartitions[i]._fhndl.closeFile();
    }

    for (int i = 0; i < _numPartitions; ++i) {
        _lpartitions[i]._fhndl.openFile(_lpartitions[i]._filename);
        _rpartitions[i]._fhndl.openFile(_rpartitions[i]._filename);
    }

    _isinit = true;

    return 0;
}

int prepareData(const vector<Attribute>& lattrs, const void* ldata, 
    const vector<Attribute>& rattrs, const void* rdata, void* data) 
{

   //join lhs rhs data
    int nlhs = lattrs.size();
    int nrhs = rattrs.size();
    int nnullbytes = ceil((nlhs + nrhs) / 8.0);
    char* nb1 = new char[nnullbytes];
    memset(nb1, 0, nnullbytes);
    char* nb2 = new char[nnullbytes];
    memset(nb2, 0, nnullbytes);

    int nnb1 = ceil(nlhs / 8.0);
    int nnb2 = ceil(nrhs / 8.0);
    memcpy((char*)nb1, (char*)ldata, nnb1);
    memcpy((char*)nb2, (char*)rdata, nnb2);

    *nb1 = *nb1 | (*nb2 >> nlhs);
    memcpy((char*)data, (char*)nb1, nnullbytes);
    int ofst = nnullbytes;
    int len1 = getDataSize(lattrs, ldata);
    memcpy((char*)data + ofst, (char*)ldata + nnb1, len1 - nnb1);
    ofst += len1 - nnb1;
    int len2 = getDataSize(rattrs, rdata);
    memcpy((char*)data + ofst, (char*)rdata + nnb2, len2 - nnb2);
    ofst += len2 - nnb2;

    return 0;
}

RC GHJoin::freeTupleBlock()
{
    for (multimap<RecordField, void*>::iterator it = _tupleblock.begin();
        it != _tupleblock.end(); ++it) {
        free(it->second);
        it->second = nullptr;
    }
    _tupleblock.clear();

    return 0;
}

//RC GHJoin::getNextTupleBlock(bool isleft, FileHandle& fhndl, int indx)
RC GHJoin::getNextTupleBlock(PartitionInfo& pinfo)
{
    freeTupleBlock();

    //if (_blockend)
    //    return QE_EOF;

//    PartitionInfo pinfo;
    vector<Attribute> attrs; 
    string condattr;
    if (pinfo._isleft) {
//        pinfo = _lpartitions[indx];
        _leftin->getAttributes(attrs);
        condattr = _condition.lhsAttr;
    }
    else {
//        pinfo = _rpartitions[indx];
        _rightin->getAttributes(attrs);
        condattr = _condition.rhsAttr;
    }

    FileHandle& fhndl = pinfo._fhndl;
  
    vector<string> attrnames;
    for (const Attribute& attrib : attrs)
        attrnames.push_back(attrib.name);


    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RBFM_ScanIterator rbfmitr;
    rbfm->scan(fhndl, attrs, string(), NO_OP, nullptr, attrnames, rbfmitr);

    int count = 0;
    RC rc = 0;
    char data[PAGE_SIZE];
    RID rid;
    while (rc != -1) {
        rc = rbfmitr.getNextRecord(rid, data);
        if (rc == -1) {
            break;
        }

        RecFldMap datarec;
        getDataRecordFields(attrs, data, datarec);

        RecFldMap::iterator it = datarec.find(condattr);
        if (datarec.end() == it)
            continue;
        const RecordField & krecfld = it->second;
        if (krecfld._isnull)
            continue;

        int len = getDataSize(attrs, data);
        void* val = malloc(len);
        memcpy(val, data, len);
        _tupleblock.insert(make_pair(krecfld, val));

        ++count;
    }

    rbfmitr.close();

    _lcount += count;

    return 0;
}

int GHJoin::getNextTuple(PartitionInfo& lpinfo, PartitionInfo& rpinfo, void* data) 
{
    //if (_indx == _numPartitions) {
    //    freeTupleBlock();
    //    _rbfmitr.close();
    //    return -1;
    //}


    if (_indx != _curBlockIndx) {
        getNextTupleBlock(lpinfo);
        _curBlockIndx = _indx;


 //       _rbfmitr.close();

        FileHandle &fhndl = rpinfo._fhndl;

        string condattr;
        if (rpinfo._isleft) {
            _rbfmattrs  = _lattrs;
            _condattr = _condition.lhsAttr;
        }
        else {
            _rbfmattrs = _rattrs;
            _condattr = _condition.rhsAttr;
        }

        _rbfmattrsname.clear();
        for (const Attribute& attrib : _rbfmattrs)
            _rbfmattrsname.push_back(attrib.name);


        RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
        rbfm->scan(fhndl, _rbfmattrs, string(), NO_OP, nullptr, _rbfmattrsname, _rbfmitr);

    }


    RC rc = 0;
    char data2[PAGE_SIZE];
    RID rid;
    while (true) {
        rc = _rbfmitr.getNextRecord(rid, data2);
        if (rc == -1) {
            _rbfmitr.close();
            return -1;
        }

        RecFldMap datarec;
        getDataRecordFields(_rbfmattrs, data2, datarec);

        RecFldMap::iterator it = datarec.find(_condattr);
        if (datarec.end() == it)
            continue;
        const RecordField & krecfld = it->second;
        if (krecfld._isnull)
            continue;

        ++_rcount;

        multimap<RecordField, void*>::iterator it2 = _tupleblock.find(krecfld);
        if (_tupleblock.end() == it2)
            continue;

        const void* data3 = it2->second;

        if(lpinfo._isleft)
            rc = prepareData(_lattrs, data3, _rattrs, data2, data);
        else 
            rc = prepareData(_lattrs, data2, _rattrs, data3, data);

        break;
    }

    return 0;
}

int GHJoin::clearPartitions()
{
    RC rc = 0;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    for (int i = 0; i < _numPartitions; ++i) {
        PartitionInfo& lpinfo = _lpartitions[i];
        PartitionInfo& rpinfo = _rpartitions[i];

        lpinfo._fhndl.closeFile();
        rpinfo._fhndl.closeFile();

        rbfm->destroyFile(lpinfo._filename);
        rbfm->destroyFile(rpinfo._filename);
    }
    _lpartitions.clear();
    _rpartitions.clear();

    return 0;
}

RC GHJoin::getNextTuple(void *data)
{
    if (!_isinit) {
        process();
    }
 
    if (_indx == _numPartitions) {
        terminate();
        return -1;
    }

    RC rc = 0;

    PartitionInfo& lpinfo = _lpartitions[_indx];
    PartitionInfo& rpinfo = _rpartitions[_indx];

    //int lnpgs = lpinfo._fhndl.getNumberOfPages();
    //int rnpgs = rpinfo._fhndl.getNumberOfPages();

    //if (lnpgs < rnpgs) {
    //    //FileHandle &fhndl = lpinfo._fhndl;
    //    //bool isleft = true;
    //    //getNextTupleBlock(isleft, fhndl, indx);
    //    rc = getNextTuple(lpinfo, rpinfo, data);
    //}
    //else {
    //    //FileHandle &fhndl = rpinfo._fhndl;
    //    //bool isleft = false;
    //    //getNextTupleBlock(isleft, fhndl, indx);
    //    rc = getNextTuple(rpinfo, lpinfo, data);
    //}
    rc = getNextTuple(lpinfo, rpinfo, data);
    if (rc == -1) {
        _indx++;
        return getNextTuple(data);
    }

    if (_indx > _numPartitions) {
        return -1;
    }


    return 0; 
};

// For attribute in vector<Attribute>, name it as rel.attr
void GHJoin::getAttributes(vector<Attribute> &attrs) const
{
    for (const Attribute& attrib : _lattrs)
        attrs.push_back(attrib);
    for (const Attribute& attrib : _rattrs)
        attrs.push_back(attrib);
};

int GHJoin::terminate()
{
    if (_isterminated)
        return 0;

    freeTupleBlock();
    _rbfmitr.close();
    clearPartitions();
    _isinit = false;

    _isterminated = true;

    return 0;
}

GHJoin::~GHJoin()
{
    terminate();
};

//--------------------------------------------------------------------------------------

int Aggregate::init() {
    _defaultRecFld._attrib.type = TypeVarChar;
    _defaultRecFld._attrib.name = "DefaultAggregateField";
    _defaultRecFld._strkey = "DefaultAggregateField";

    return 0;
}

Aggregate::Aggregate(Iterator *input,          // Iterator of input R
    Attribute aggAttr,        // The attribute over which we are computing an aggregate
    AggregateOp op            // Aggregate operation
) : _itr(input), _aggattr(aggAttr), _aggop(op), _end(false), _isgroup(false), _isinit(false)
{
    init();
};

// Optional for everyone: 5 extra-credit points
// Group-based hash aggregation
Aggregate::Aggregate(Iterator *input,             // Iterator of input R
    Attribute aggAttr,           // The attribute over which we are computing an aggregate
    Attribute groupAttr,         // The attribute over which we are grouping the tuples
    AggregateOp op              // Aggregate operation
) : _itr(input), _aggattr(aggAttr), _aggop(op), _groupattr(groupAttr), 
_end(false), _isgroup(true), _isinit(false)
{
    init();
};

int Aggregate::process() 
{
    vector<Attribute> lattrs;
    _itr->getAttributes(lattrs);

    RC rc = 0;
    char data2[PAGE_SIZE];

    while (0 == rc) {
        rc = _itr->getNextTuple(data2);
        if (rc) break;

        RecFldMap datarec;
        getDataRecordFields(lattrs, data2, datarec);
        RecFldMap::iterator it = datarec.find(_aggattr.name);
        if (datarec.end() == it)
            continue;

        const RecordField & krecfld = it->second;
        if (krecfld._isnull)
            continue;

        if (_isgroup) {
            RecFldMap::iterator it2 = datarec.find(_groupattr.name);
            if (datarec.end() == it2)
                continue;

            const RecordField & grecfld = it2->second;
            if (grecfld._isnull)
                continue;

            aggvalsmap[grecfld].addRecordField(krecfld);
        }
        else {
            if (aggvalsmap.empty()) 
                aggvalsmap.insert(make_pair(_defaultRecFld, AggregateValues()));

            aggvalsmap[_defaultRecFld].addRecordField(krecfld);
        }

    }

    _citr = aggvalsmap.begin();

    return 0;
}

int Aggregate::getAggregateValue(AggregateValues aggvals, void* key) const
{
    float avg = aggvals.getAvg();
    switch (_aggop) {
    case MIN: memcpy(key, (char*)&aggvals.min, sizeof(float)); break;
    case MAX: memcpy(key, (char*)&aggvals.max, sizeof(float)); break;
    case COUNT: memcpy(key, (char*)&aggvals.count, sizeof(float)); break;
    case SUM: memcpy(key, (char*)&aggvals.sum, sizeof(float)); break;
    case AVG: memcpy(key, (char*)&avg, sizeof(float)); break;
    default: assert(0); break;
    }
    return 0;
}

RC Aggregate::getNextTuple(void *data) 
{
    if (_end)
        return -1;

    if (false == _isinit) {
        process();
        _isinit = true;
    }

    if (_isgroup) {
        if (_citr == aggvalsmap.end()) {
            _end = true;
            return -1;
        }

        const RecordField& recfld = _citr->first;
        AggregateValues aggvals = _citr->second;


        char gkey[PAGE_SIZE];
        recfld.getKey(gkey);
        int len = recfld.length();

        char key[PAGE_SIZE];
        getAggregateValue(aggvals, key);

        memset((char*)data, 0, 1);
        memcpy((char*)data + 1, gkey, len);
        memcpy((char*)data + 1+len, key, sizeof(float));

        ++_citr;
    }
    else {
        map<RecordField, AggregateValues>::iterator it3
            = aggvalsmap.find(_defaultRecFld);
        if (aggvalsmap.end() == it3) 
            return -1;
        
        AggregateValues aggvals = it3->second;
        char key[PAGE_SIZE];
        getAggregateValue(aggvals, key);

        memset((char*)data, 0, 1);
        memcpy((char*)data + 1, key, sizeof(float));

        _end = true;
    }

    return 0;
};
// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
//MIN=0, MAX, COUNT, SUM, AVG
void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
    Attribute oattr = _aggattr;
    string aname;
    switch (_aggop) {
    case MIN: aname = "MIN"; break;
    case MAX: aname = "MAX"; break;
    case COUNT: aname = "COUNT"; break;
    case SUM: aname = "SUM"; break;
    case AVG: aname = "AVG"; break;
    default: aname = "NONE"; break;
    }
    aname = aname + "(" + oattr.name + ")";
    oattr.name = aname;

    if (_aggop == COUNT) 
        oattr.type = TypeInt;
    
    if (_aggop == AVG)
        oattr.type = TypeReal;

    if (_isgroup)
        attrs.push_back(_groupattr);

    attrs.push_back(oattr);
};