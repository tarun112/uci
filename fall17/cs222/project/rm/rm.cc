#include <cstdio>
#include <cstring>
#include <iostream>
#include <cmath>
#include <map>
#include "rm.h"
#include "../ix/ix.h"
#include "../qe/qe.h"

const string TABLES_FILE_NAME = "Tables.cs222";
const string COLUMNS_FILE_NAME = "Columns.cs222";
const string INDEX_FILE_NAME = "Indices.cs222";

int getTableIdAndSystem(const string &tableName, int &tableid, int &issystem);

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if (!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

int createTablesAttribs(vector<Attribute>& tblAttrs)
{
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = 4;
    tblAttrs.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = 50;
    tblAttrs.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = 50;
    tblAttrs.push_back(attr);

    attr.name = "system-table";
    attr.type = TypeInt;
    attr.length = 4;
    tblAttrs.push_back(attr);
    
    return 0;
}

int createColumnsAttribs(vector<Attribute>& clmAttrs)
{
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = 4;
    clmAttrs.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = 50;
    clmAttrs.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = 4;
    clmAttrs.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = 4;
    clmAttrs.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = 4;
    clmAttrs.push_back(attr);

    return 0;
}

int createIndexAttribs(vector<Attribute>& indxAttrs)
{
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = 4;
    indxAttrs.push_back(attr);

    attr.name = "attribute-name";
    attr.type = TypeVarChar;
    attr.length = 50;
    indxAttrs.push_back(attr);

    attr.name = "attribute-type";
    attr.type = TypeInt;
    attr.length = 4;
    indxAttrs.push_back(attr);

    attr.name = "attribute-length";
    attr.type = TypeInt;
    attr.length = 4;
    indxAttrs.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = 50;
    indxAttrs.push_back(attr);

    return 0;
}

int createTablesData(const vector<Attribute>& tblAttrs, int tableid, const string &tableName, int issystem, void* data)
{
    char nulldesc;
    memset(&nulldesc, 0, 1);
    int offset = 0;
    memcpy((char*)data, &nulldesc, sizeof(char));
    offset += sizeof(char);

    memcpy((char*)data + offset, (char*)&tableid, sizeof(int));
    offset += sizeof(int);

    int len = tableName.length();
    memcpy((char*)data + offset, (char*)&len, sizeof(int));
    offset += sizeof(int);

    const char* tblname = tableName.c_str();
    memcpy((char*)data + offset, (char*)tblname, len * sizeof(char));
    offset += len * sizeof(char);

    len = tableName.length();
    memcpy((char*)data + offset, (char*)&len, sizeof(int));
    offset += sizeof(int);

    const char* fname = tableName.c_str();
    memcpy((char*)data + offset, (char*)fname, len * sizeof(char));
    offset += len * sizeof(char);

    memcpy((char*)data + offset, (char*)&issystem, sizeof(int));

    return 0;
}


int createColumnsAttribData(const vector<Attribute>& clmAttrs, const Attribute& attrib, 
    int tableid, int position, void* data)
{
    char nulldesc;
    memset(&nulldesc, 0, 1);
    int offset = 0;
    memcpy((char*)data, &nulldesc, sizeof(char));
    offset += sizeof(char);

    memcpy((char*)data + offset, (char*)&tableid, sizeof(int));
    offset += sizeof(int);

    int len = attrib.name.length();
    memcpy((char*)data + offset, (char*)&len, sizeof(int));
    offset += sizeof(int);

    const char* clmname = attrib.name.c_str();
    memcpy((char*)data + offset, (char*)clmname, len * sizeof(char));
    offset += len * sizeof(char);

    int clmtype = attrib.type;
    memcpy((char*)data + offset, (char*)&clmtype, sizeof(int));
    offset += sizeof(int);

    int clmlen = attrib.length;
    memcpy((char*)data + offset, (char*)&clmlen, sizeof(int));
    offset += sizeof(int);

    memcpy((char*)data + offset, (char*)&position, sizeof(int));

    return 0;
}

int createIndexData(const vector<Attribute>& indxAttrs, const Attribute &attrib,
    int tableid, const string& filename, void* data)
{
    char nulldesc;
    memset(&nulldesc, 0, 1);
    int offset = 0;
    memcpy((char*)data, &nulldesc, sizeof(char));
    offset += sizeof(char);

    memcpy((char*)data + offset, (char*)&tableid, sizeof(int));
    offset += sizeof(int);

    int len = attrib.name.length();
    memcpy((char*)data + offset, (char*)&len, sizeof(int));
    offset += sizeof(int);

    const char* clmname = attrib.name.c_str();
    memcpy((char*)data + offset, (char*)clmname, len * sizeof(char));
    offset += len * sizeof(char);

    int clmtype = attrib.type;
    memcpy((char*)data + offset, (char*)&clmtype, sizeof(int));
    offset += sizeof(int);

    int clmlen = attrib.length;
    memcpy((char*)data + offset, (char*)&clmlen, sizeof(int));
    offset += sizeof(int);

    int len2 = filename.length();
    memcpy((char*)data + offset, (char*)&len2, sizeof(int));
    offset += sizeof(int);

    memcpy((char*)data + offset, (char*)filename.c_str(), filename.length());

    return 0;
}


string createIndexFileName(const string& tableName,
    const string &attributeName)
{
    return tableName + "_" + attributeName;
}

int getIndexInfo(const string &tableName, const string &attributeName,
    Attribute &oAttrib, bool &indxExists)
{
    int tableid = -1; // getTableId(tableName);
    int issystem = -1;
    RID rid;
    getTableIdAndSystem(tableName, tableid, issystem);
    if (tableid < 0)
        return -1;

    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    RC stat = 0;

    FileHandle indxFHndl;
    stat = rbfm->openFile(INDEX_FILE_NAME, indxFHndl);

    vector<Attribute> indxAttrs;
    createIndexAttribs(indxAttrs);

    RBFM_ScanIterator rbfmit;
    vector<string> matchattrbs;
    matchattrbs.push_back(string("attribute-name"));
    matchattrbs.push_back(string("attribute-type"));
    matchattrbs.push_back(string("attribute-length"));

    stat = rbfm->scan(indxFHndl, indxAttrs, string("table-id"), EQ_OP, (void*)&tableid, matchattrbs, rbfmit);
    if (stat) return stat;

    void *data = malloc(PAGE_SIZE);
    indxExists = false;
    while (RBFM_EOF != rbfmit.getNextRecord(rid, data)) {
        int offset = 1;
        Attribute attrb;
        int len;
        memcpy((char*)&len, (char*)data + offset, sizeof(int));
        offset += sizeof(int);
        char name[50];
        memcpy(name, (char*)data + offset, len * sizeof(char));
        offset += len * sizeof(char);
        name[len] = '\0';
        attrb.name = name;
        memcpy((char*)&attrb.type, (char*)data + offset, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)&attrb.length, (char*)data + offset, sizeof(int));

        if (attrb.name.compare(attributeName) == 0) {
            oAttrib = attrb;
            indxExists = true;
            break;
        }
    }
    free(data);

    stat = rbfmit.close();
    if (stat) return stat;

    stat = rbfm->closeFile(indxFHndl);
    if (stat) return stat;

    return 0;
}

int getIndexAttribute(const string &tableName, const string &attributeName, Attribute &oAttrib)
{

    RC stat = 0;

    RelationManager *rm = RelationManager::instance();

    vector<Attribute> attrs;
    stat = rm->getAttributes(tableName, attrs);

    for (const Attribute &attrib : attrs) {
        if (attrib.name.compare(attributeName) == 0) {
            oAttrib = attrib;
            return 0;
        }
    }

    return -1;
}


RC RelationManager::createCatalog()
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    RC stat = 0;
    stat = rbfm->createFile(TABLES_FILE_NAME);
    FileHandle tblFHndl;
    rbfm->openFile(TABLES_FILE_NAME, tblFHndl);

    stat = rbfm->createFile(COLUMNS_FILE_NAME);
    FileHandle clmFHndl;
    rbfm->openFile(COLUMNS_FILE_NAME, clmFHndl);

    stat = rbfm->createFile(INDEX_FILE_NAME);
    FileHandle indxFHndl;
    rbfm->openFile(INDEX_FILE_NAME, indxFHndl);

    RID rid;

    vector<Attribute> tblAttrbs;
    createTablesAttribs(tblAttrbs);
    int tableid = 1;
    int issytem = 1;

    void* data = malloc(PAGE_SIZE);
    
    createTablesData(tblAttrbs, tableid, TABLES_FILE_NAME, issytem, data);
    stat = rbfm->insertRecord(tblFHndl, tblAttrbs, data, rid);

//    rbfm->readRecord(tblFHndl, tblAttrbs, rid, data);
//    rbfm->printRecord(tblAttrbs, data);

    vector<Attribute> clmAttrbs;
    createColumnsAttribs(clmAttrbs);

    int nattr = tblAttrbs.size();
    for (int i = 0; i < nattr; ++i) {
        int pos = i + 1;
        createColumnsAttribData(clmAttrbs, tblAttrbs[i], tableid, pos, data);
        stat = rbfm->insertRecord(clmFHndl, clmAttrbs, data, rid);
    }

    tableid = 2;
    issytem = 1;
    createTablesData(tblAttrbs, tableid, COLUMNS_FILE_NAME, issytem, data);
    stat = rbfm->insertRecord(tblFHndl, tblAttrbs, data, rid);

    nattr = clmAttrbs.size();
    for (int i = 0; i < nattr; ++i) {
        int pos = i + 1;
        createColumnsAttribData(clmAttrbs, clmAttrbs[i], tableid, pos, data);
        stat = rbfm->insertRecord(clmFHndl, clmAttrbs, data, rid);
    }

    //check: create index catalog file
    vector<Attribute> indxAttrbs;
    createIndexAttribs(indxAttrbs);
    tableid = 3;
    issytem = 1;
    createTablesData(tblAttrbs, tableid, INDEX_FILE_NAME, issytem, data);
    stat = rbfm->insertRecord(tblFHndl, tblAttrbs, data, rid);

    nattr = clmAttrbs.size();
    for (int i = 0; i < nattr; ++i) {
        int pos = i + 1;
        createColumnsAttribData(clmAttrbs, indxAttrbs[i], tableid, pos, data);
        stat = rbfm->insertRecord(clmFHndl, clmAttrbs, data, rid);
    }

    stat = rbfm->closeFile(indxFHndl);

    stat = rbfm->closeFile(tblFHndl);
    stat = rbfm->closeFile(clmFHndl);
    free(data);
    return 0;
}

RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    RC stat = 0;
    stat = rbfm->destroyFile(TABLES_FILE_NAME);
    if (stat) return stat;
    stat = rbfm->destroyFile(COLUMNS_FILE_NAME);
    if (stat) return stat;
    stat = rbfm->destroyFile(INDEX_FILE_NAME);
    if (stat) return stat;
    return 0;
}

int getMaxDataSize(const vector<Attribute> &attrs) 
{
    int maxsize = 0;
    for (int i = 0; i < attrs.size(); ++i) {
        maxsize += attrs[i].length;
    }
    maxsize += ceil(attrs.size() / 8.0);
    return maxsize;
}

int getNextTableID() {
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    RC stat = 0;
    FileHandle tblFHndl;
    stat = rbfm->openFile(TABLES_FILE_NAME, tblFHndl);
    if (stat) return stat;

    vector<Attribute> tblAttrbs;
    createTablesAttribs(tblAttrbs);
    int tableid = -1;
    int issytem = 1;

    int maxtableid = -1;
    RBFM_ScanIterator rbfmit;
    void *value = NULL;
    vector<string> matchattrbs;
    matchattrbs.push_back(string("table-id"));
    rbfm->scan(tblFHndl, tblAttrbs, string(""), NO_OP, value, matchattrbs, rbfmit);

    RID rid;
    void *data = malloc(PAGE_SIZE);
    while (RBFM_EOF != rbfmit.getNextRecord(rid, data)) {
        memcpy((char*)&tableid, (char*)data+1, sizeof(int));
        if (tableid > maxtableid)
            maxtableid = tableid;
    }
    free(data);

    stat = rbfmit.close();
    if (stat) return stat;

    return maxtableid+1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    RC stat = 0;

    string tblFName = tableName;
    stat = rbfm->createFile(tblFName);


    FileHandle tblFHndl;
    stat = rbfm->openFile(TABLES_FILE_NAME, tblFHndl);
    FileHandle clmFHndl;
    stat = rbfm->openFile(COLUMNS_FILE_NAME, clmFHndl);

    RID rid;

    vector<Attribute> tblAttrs;
    createTablesAttribs(tblAttrs);
    int tableid = getNextTableID();
    int issystem = 0;
    void* data = malloc(PAGE_SIZE);
    int res = createTablesData(attrs, tableid, tableName, issystem, data);
    rbfm->insertRecord(tblFHndl, tblAttrs, data, rid);

    vector<Attribute> clmAttrs;
    createColumnsAttribs(clmAttrs);
    int nattr = attrs.size();
    for (int i = 0; i < nattr; ++i) {
        int pos = i + 1;
        createColumnsAttribData(clmAttrs, attrs[i], tableid, pos, data);
        stat = rbfm->insertRecord(clmFHndl, clmAttrs, data, rid);
    }

    stat = rbfm->closeFile(tblFHndl);
    stat = rbfm->closeFile(clmFHndl);
    free(data);

    return 0;
}

int getTableId(const string &tableName) 
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    RC stat = 0;
    FileHandle tblFHndl;
    stat = rbfm->openFile(TABLES_FILE_NAME, tblFHndl);
    if (stat) return stat;

    vector<Attribute> tblAttrbs;
    createTablesAttribs(tblAttrbs);
    int tableid = -1;
    int issytem = 1;

    RBFM_ScanIterator rbfmit;
    void *value = malloc(PAGE_SIZE);
    int len = tableName.length();
    memcpy((char*)value, (char*)&len, sizeof(int));
    memcpy((char*)value + sizeof(int), tableName.c_str(), len * sizeof(char));
    vector<string> matchattrbs;
    matchattrbs.push_back(string("table-id"));
    stat = rbfm->scan(tblFHndl, tblAttrbs, string("table-name"), EQ_OP, value, matchattrbs, rbfmit);
    if (stat) {
        free(value);
        return stat;
    }

    RID rid;
    void *data = malloc(PAGE_SIZE);
    if (RBFM_EOF != rbfmit.getNextRecord(rid, data)) {
        memcpy((char*)&tableid, (char*)data + 1, sizeof(int));
    }

    free(value);
    free(data);

    stat = rbfmit.close();
    if (stat) return stat;
    
    stat = rbfm->closeFile(tblFHndl);
    if (stat) return stat;

    return tableid;
}

int getTableIdAndSystem(const string &tableName, int &tableid, int &issystem)
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    RC stat = 0;
    FileHandle tblFHndl;
    stat = rbfm->openFile(TABLES_FILE_NAME, tblFHndl);
    if (stat) return stat;

    vector<Attribute> tblAttrbs;
    createTablesAttribs(tblAttrbs);

    RBFM_ScanIterator rbfmit;
    void *value = malloc(PAGE_SIZE);
    int len = tableName.length();
    memcpy((char*)value, (char*)&len, sizeof(int));
    memcpy((char*)value + sizeof(int), tableName.c_str(), len * sizeof(char));
    vector<string> matchattrbs;
    matchattrbs.push_back(string("table-id"));
    matchattrbs.push_back(string("system-table"));

    stat = rbfm->scan(tblFHndl, tblAttrbs, string("table-name"), EQ_OP, value, matchattrbs, rbfmit);
    if (stat) {
        free(value);
        return stat;
    }

    RID rid;
    void *data = malloc(200);
    if (RBFM_EOF != rbfmit.getNextRecord(rid, data)) {
        memcpy((char*)&tableid, (char*)data + 1, sizeof(int));
        memcpy((char*)&issystem, (char*)data + 1 + sizeof(int), sizeof(int));
    }

    free(value);
    free(data);

    stat = rbfmit.close();
    if (stat) return stat;

    stat = rbfm->closeFile(tblFHndl);
    if (stat) return stat;

    return 0;
}


RC RelationManager::deleteTable(const string &tableName)
{
    int tableid = -1; // getTableId(tableName);
    int issystem = -1;
    getTableIdAndSystem(tableName, tableid, issystem);
    if (tableid < 0 || issystem == 1)
        return -1;

    RID rid;

    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    RC stat = 0;

    {
        FileHandle tblFHndl;
        stat = rbfm->openFile(TABLES_FILE_NAME, tblFHndl);

        vector<Attribute> tblAttrs;
        createTablesAttribs(tblAttrs);
        rbfm->deleteRecord(tblFHndl, tblAttrs, rid);

        stat = rbfm->closeFile(tblFHndl);
        if (stat) return stat;
    }

    {
        FileHandle clmFHndl;
        stat = rbfm->openFile(COLUMNS_FILE_NAME, clmFHndl);

        vector<Attribute> clmAttrs;
        createColumnsAttribs(clmAttrs);

        RBFM_ScanIterator rbfmit;
        vector<string> matchattrbs;
        matchattrbs.push_back(string("column-name"));
        stat = rbfm->scan(clmFHndl, clmAttrs, string("table-id"), EQ_OP, (void*)&tableid, matchattrbs, rbfmit);
        if (stat) return stat;

        void *data = malloc(PAGE_SIZE);
        vector<RID> clmrids;
        while (RBFM_EOF != rbfmit.getNextRecord(rid, data)) {
            clmrids.push_back(rid);
        }
        free(data);

        stat = rbfmit.close();
        if (stat) return stat;

        int nattr = clmrids.size();
        for (int i = 0; i < nattr; ++i) {
            stat = rbfm->deleteRecord(clmFHndl, clmAttrs, clmrids[i]);
            if (stat) return stat;
        }

        stat = rbfm->closeFile(clmFHndl);
        if (stat) return stat;
    }

    {
        FileHandle indxFHndl;
        stat = rbfm->openFile(INDEX_FILE_NAME, indxFHndl);

        vector<Attribute> indxAttrs;
        createIndexAttribs(indxAttrs);

        RBFM_ScanIterator rbfmit;
        vector<string> matchattrbs;
        matchattrbs.push_back(string("file-name"));
        stat = rbfm->scan(indxFHndl, indxAttrs, string("table-id"), EQ_OP, (void*)&tableid, matchattrbs, rbfmit);
        if (stat) return stat;

        void *data = malloc(PAGE_SIZE);
        vector<RID> indxrids;
        while (RBFM_EOF != rbfmit.getNextRecord(rid, data)) {
            indxrids.push_back(rid);
            
            char buf[PAGE_SIZE];
            int len = -1;
            memcpy((char*)&len, (char*)data + 1, sizeof(int));
            memcpy((char*)&buf, (char*)data + 1+sizeof(int), len);
            buf[len] = '\0';
            string indxfname = buf;
            rbfm->destroyFile(indxfname);
        }
        free(data);

        stat = rbfmit.close();
        if (stat) return stat;

        int nattr = indxrids.size();
        for (int i = 0; i < nattr; ++i) {
            stat = rbfm->deleteRecord(indxFHndl, indxAttrs, indxrids[i]);
            if (stat) return stat;
        }

        stat = rbfm->closeFile(indxFHndl);
        if (stat) return stat;
    }


    stat = rbfm->destroyFile(tableName);
    if (stat) return stat;

    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RID rid;
    int tableid = getTableId(tableName);
    if (tableid < 0 )
        return -1;

    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    RC stat = 0;

    FileHandle clmFHndl;
    stat = rbfm->openFile(COLUMNS_FILE_NAME, clmFHndl);

    vector<Attribute> clmAttrs;
    createColumnsAttribs(clmAttrs);

    RBFM_ScanIterator rbfmit;
    vector<string> matchattrbs;
    matchattrbs.push_back(string("column-name"));
    matchattrbs.push_back(string("column-type"));
    matchattrbs.push_back(string("column-length"));

    stat = rbfm->scan(clmFHndl, clmAttrs, string("table-id"), EQ_OP, (void*)&tableid, matchattrbs, rbfmit);
    if (stat) return stat;

    void *data = malloc(200);
    while (RBFM_EOF != rbfmit.getNextRecord(rid, data)) {
        int offset = 1;
        Attribute attrb;
        int len;
        memcpy((char*)&len, (char*)data + offset, sizeof(int));
        offset += sizeof(int);
        char name[50];
        memcpy(name, (char*)data + offset, len * sizeof(char));
        offset += len * sizeof(char);
        name[len] = '\0';
        attrb.name = name;
        memcpy((char*)&attrb.type, (char*)data + offset, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)&attrb.length, (char*)data + offset, sizeof(int));
        attrs.push_back(attrb);
    }
    free(data);

    stat = rbfmit.close();
    if (stat) return stat;

    stat = rbfm->closeFile(clmFHndl);
    if (stat) return stat;

    return 0;
}

int getDataRecordFields(const vector<Attribute>& attribs, const void *data,
    vector<RecordField>& datarec) 
{
    int nfields = attribs.size();
    int nnullbytes = ceil(nfields / 8.0);
    int offset = nnullbytes;
    unsigned char* nullbytes = new unsigned char[nnullbytes];
    memcpy(nullbytes, data, nnullbytes * sizeof(char));
    for (int i = 0; i< nfields; ++i) {
        const Attribute& atrb = attribs[i];
        RecordField recfld;
        recfld._isnull = false;
//        recfld._type = atrb.type;
        recfld._attrib = atrb;

        if ((nullbytes[i / 8] >> (7 - i % 8)) & 1) {
            recfld._isnull = true;
            datarec.push_back(recfld);
            continue;
        }

        if (atrb.type == TypeVarChar) {
            int length;
            memcpy(&length, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            char buff[PAGE_SIZE];
            memcpy(buff, (char*)data + offset, length);
            buff[length] = '\0';
            offset += length;
//            recfld._type = TypeVarChar;
            recfld._strkey = buff;
        }
        else if (atrb.type == TypeInt) {
            int val;
            memcpy(&val, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
//            recfld._type = TypeInt;
            recfld._intkey = val;
        }
        if (atrb.type == TypeReal) {
            float val;
            memcpy(&val, (char*)data + offset, sizeof(int));
            offset += sizeof(float);
//            recfld._type = TypeFloat;
            recfld._realkey = val;
        }
        
        datarec.push_back(recfld);
    }
    delete[] nullbytes;
    return 0;
}

int getDataRecordFields(const vector<Attribute>& attribs, const void *data,
    map<string,RecordField>& datarec)
{
    int nfields = attribs.size();
    int nnullbytes = ceil(nfields / 8.0);
    int offset = nnullbytes;
    unsigned char* nullbytes = new unsigned char[nnullbytes];
    memcpy(nullbytes, data, nnullbytes * sizeof(char));
    for (int i = 0; i< nfields; ++i) {
        const Attribute& atrb = attribs[i];
        RecordField recfld;
        recfld._isnull = false;
        //        recfld._type = atrb.type;
        recfld._attrib = atrb;

        if ((nullbytes[i / 8] >> (7 - i % 8)) & 1) {
            recfld._isnull = true;
            datarec.insert(make_pair(atrb.name, recfld));
//            datarec.push_back(recfld);
            continue;
        }

        if (atrb.type == TypeVarChar) {
            int length;
            memcpy(&length, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            char buff[PAGE_SIZE];
            memcpy(buff, (char*)data + offset, length);
            buff[length] = '\0';
            offset += length;
            //            recfld._type = TypeVarChar;
            recfld._strkey = buff;
        }
        else if (atrb.type == TypeInt) {
            int val;
            memcpy(&val, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            //            recfld._type = TypeInt;
            recfld._intkey = val;
        }
        if (atrb.type == TypeReal) {
            float val;
            memcpy(&val, (char*)data + offset, sizeof(int));
            offset += sizeof(float);
            //            recfld._type = TypeFloat;
            recfld._realkey = val;
        }

        datarec.insert(make_pair(atrb.name, recfld));
//        datarec.push_back(recfld);
    }
    delete[] nullbytes;
    return 0;
}

int getDataRecordField(const Attribute& attrb, const void *data,
    RecordField &datarec) {

    vector<Attribute> fattribs;
    fattribs.push_back(attrb);

    vector<RecordField> fdatarec;

    int stat = getDataRecordFields(fattribs, data, fdatarec);
    if (stat) return stat;

    datarec = fdatarec[0];
    return 0;
}


RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    int tableid = -1; // getTableId(tableName);
    int issystem = -1;
    getTableIdAndSystem(tableName, tableid, issystem);
    if (tableid < 0 || issystem == 1)
        return -1;

    vector<Attribute> attrs;
    RC stat = 0;
    stat = getAttributes(tableName, attrs);
    if (stat) return stat;

    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

    FileHandle fileHndl;
    stat = rbfm->openFile(tableName, fileHndl);
    if (stat) return stat;

    stat = rbfm->insertRecord(fileHndl, attrs, data, rid);
    if (stat) return stat;

    stat = rbfm->closeFile(fileHndl);
    if (stat) return stat;

    vector<RecordField> datarec;
    getDataRecordFields(attrs, data, datarec);
    for (int i = 0; i < datarec.size(); ++i) {
        const RecordField &recfld = datarec[i];
        if (recfld._isnull) 
            continue;

        const string &attribname = recfld._attrib.name;

        bool indxexists = false;
        Attribute indxattrib;
        int res = getIndexInfo(tableName, attribname, indxattrib, indxexists);
        if (!indxexists)
            continue;

        string indxfname = createIndexFileName(tableName, attribname);
        IndexManager *im = IndexManager::instance();
        IXFileHandle ixfhndl;
        im->openFile(indxfname, ixfhndl);
        char key[PAGE_SIZE];
        recfld.getKey(key);
        im->insertEntry(ixfhndl, indxattrib, key, rid);
        im->closeFile(ixfhndl);
    }

    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    int tableid = -1; // getTableId(tableName);
    int issystem = -1;
    getTableIdAndSystem(tableName, tableid, issystem);
    if (tableid < 0 || issystem == 1)
        return -1;

    vector<Attribute> attrs;
    RC stat = 0;
    stat = getAttributes(tableName, attrs);
    if (stat) return stat;

    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

    FileHandle fileHndl;
    stat = rbfm->openFile(tableName, fileHndl);
    if (stat) return stat;

    char buff[PAGE_SIZE];
    stat = rbfm->readRecord(fileHndl, attrs, rid, buff);
    vector<RecordField> datarec;
    getDataRecordFields(attrs, buff, datarec);

    stat = rbfm->deleteRecord(fileHndl, attrs, rid);
    if (stat) return stat;

    stat = rbfm->closeFile(fileHndl);
    if (stat) return stat;

    //vector<RecordField> datarec;
    //getDataRecordFields(attrs, data, datarec);
    for (int i = 0; i < datarec.size(); ++i) {
        const RecordField &recfld = datarec[i];
        if (recfld._isnull)
            continue;

        const string &attribname = recfld._attrib.name;

        bool indxexists = false;
        Attribute indxattrib;
        int res = getIndexInfo(tableName, attribname, indxattrib, indxexists);
        if (!indxexists)
            continue;

        string indxfname = createIndexFileName(tableName, attribname);
        IndexManager *im = IndexManager::instance();
        IXFileHandle ixfhndl;
        im->openFile(indxfname, ixfhndl);
        char key[PAGE_SIZE];
        recfld.getKey(key);
        im->deleteEntry(ixfhndl, indxattrib, key, rid);
        im->closeFile(ixfhndl);
    }


    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    int tableid = -1; // getTableId(tableName);
    int issystem = -1;
    getTableIdAndSystem(tableName, tableid, issystem);
    if (tableid < 0 || issystem == 1)
        return -1;

    vector<Attribute> attrs;
    RC stat = 0;
    stat = getAttributes(tableName, attrs);
    if (stat) return stat;

    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

    FileHandle fileHndl;
    stat = rbfm->openFile(tableName, fileHndl);
    if (stat) return stat;

    char buff[PAGE_SIZE];
    stat = rbfm->readRecord(fileHndl, attrs, rid, buff);
    vector<RecordField> pdatrec;
    getDataRecordFields(attrs, buff, pdatrec);

    stat = rbfm->updateRecord(fileHndl, attrs, data, rid);
    if (stat) return stat;

    stat = rbfm->closeFile(fileHndl);
    if (stat) return stat;

    vector<RecordField> datarec;
    getDataRecordFields(attrs, data, datarec);
    
    for (int i = 0; i < datarec.size(); ++i) {
        const RecordField &recfld = datarec[i];
        if (recfld._isnull)
            continue;

        const string &attribname = recfld._attrib.name;

        bool indxexists = false;
        Attribute indxattrib;
        int res = getIndexInfo(tableName, attribname, indxattrib, indxexists);
        if (!indxexists)
            continue;

        const RecordField &precfld = pdatrec[i];
        if (recfld.compareKey(precfld) == 0)
            continue;

        string indxfname = createIndexFileName(tableName, attribname);
        IndexManager *im = IndexManager::instance();
        IXFileHandle ixfhndl;
        im->openFile(indxfname, ixfhndl);
        
        char key[PAGE_SIZE];
        precfld.getKey(key);
        im->deleteEntry(ixfhndl, indxattrib, key, rid);
        
        char key2[PAGE_SIZE];
        recfld.getKey(key2);
        im->insertEntry(ixfhndl, indxattrib, key2, rid);
        
        im->closeFile(ixfhndl);
    }

    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    vector<Attribute> attrs;
    RC stat = 0;
    stat = getAttributes(tableName, attrs);
    if (stat) return stat;

    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

    FileHandle fileHndl;
    stat = rbfm->openFile(tableName, fileHndl);
    if (stat) return stat;

    stat = rbfm->readRecord(fileHndl, attrs, rid, data);
    if (stat) {
        rbfm->closeFile(fileHndl);
        return stat;
    }

    stat = rbfm->closeFile(fileHndl);
    if (stat) return stat;

    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    RC stat = 0;

    stat = rbfm->printRecord(attrs, data);
    if (stat) return stat;

    return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    vector<Attribute> attrs;
    RC stat = 0;
    stat = getAttributes(tableName, attrs);
    if (stat) return stat;

    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

    FileHandle fileHndl;
    stat = rbfm->openFile(tableName, fileHndl);
    if (stat) return stat;

    stat = rbfm->readAttribute(fileHndl, attrs, rid, attributeName, data);
    if (stat) return stat;

    stat = rbfm->closeFile(fileHndl);
    if (stat) return stat;

    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    vector<Attribute> attrs;
    RC stat = 0;
    stat = getAttributes(tableName, attrs);
    if (stat) return stat;

    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

    stat = rbfm->openFile(tableName, rm_ScanIterator.fileHndl);
    if (stat) return stat;

    stat = rbfm->scan(rm_ScanIterator.fileHndl, attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfmit);
    if (stat) return stat;

    return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) 
{ 
    return rbfmit.getNextRecord(rid, data); 
}

RC RM_ScanIterator::close() 
{ 
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    RC stat = 0;
    stat = rbfmit.close();
    if (stat) return stat;

    stat = rbfm->closeFile(fileHndl);
    if (stat) return stat;

    return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    int tableid = -1; // getTableId(tableName);
    int issystem = -1;
    RID rid;
    getTableIdAndSystem(tableName, tableid, issystem);
    if (tableid < 0 || issystem == 1)
        return -1;

    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    RC stat = 0;

    FileHandle clmFHndl;
    stat = rbfm->openFile(COLUMNS_FILE_NAME, clmFHndl);

    vector<Attribute> clmAttrs;
    createColumnsAttribs(clmAttrs);

    RBFM_ScanIterator rbfmit;
    vector<string> matchattrbs;
    matchattrbs.push_back(string("column-name"));
    stat = rbfm->scan(clmFHndl, clmAttrs, string("table-id"), EQ_OP, (void*)&tableid, matchattrbs, rbfmit);
    if (stat) return stat;

    void *data = malloc(PAGE_SIZE);
    RID rmRid;
    bool found = false;
    while (RBFM_EOF != rbfmit.getNextRecord(rid, data)) {
        int len = 0;
        memcpy((char*)&len, (char*)data + 1, sizeof(int));
        char attribname[51];
        memcpy(attribname, (char*)data + 1 + sizeof(int), len * sizeof(char));
        attribname[len] = '\0';
        if (! attributeName.compare(attribname)) {
            rmRid = rid;
            found = true;
            break;
        }
    }
    free(data);

    stat = rbfmit.close();
    if (stat) return stat;

    if (found) {
        stat = rbfm->deleteRecord(clmFHndl, clmAttrs, rmRid);
        if (stat) {
            stat = rbfm->closeFile(clmFHndl);
            return stat;
        }
    }

    stat = rbfm->closeFile(clmFHndl);
    if (stat) return stat;

    return 0;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}


RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
    bool indxexists = false;
    Attribute indxattrib;
    int res = getIndexInfo(tableName, attributeName, indxattrib, indxexists);
    if (indxexists) //index already present
        return -1;

    RC stat = 0;

    int tableid = getTableId(tableName);


    vector<Attribute> indxAttrs;
    createIndexAttribs(indxAttrs);
  
    vector<Attribute> tblAttrbs;
    getAttributes(tableName, tblAttrbs);
    bool found = false;
    for (const Attribute &tmp : tblAttrbs) {
        if (tmp.name.compare(attributeName) == 0) {
            indxattrib = tmp;
            found = true;
            break;
        }
    }

    if (!found)
        return -1;

    //enter index entry in index catalog INDEX_FILE_NAME
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

    {
        IndexManager* im = IndexManager::instance();
        string indxfname = createIndexFileName(tableName, attributeName);
        stat = im->createFile(indxfname);
        if (stat) return stat;

        void* data = malloc(PAGE_SIZE);
        createIndexData(indxAttrs, indxattrib, tableid, indxfname, data);

        FileHandle indxFHndl;
        rbfm->openFile(INDEX_FILE_NAME, indxFHndl);
        RID irid;
        rbfm->insertRecord(indxFHndl, indxAttrs, data, irid);
        rbfm->closeFile(indxFHndl);

        free(data);
    }

    //insert existing keys into the index file
    {
        FileHandle tblFHndl;
        rbfm->openFile(tableName, tblFHndl);
        RBFM_ScanIterator rbfmit;
        vector<string> matchattrbs;
        matchattrbs.push_back(attributeName);
        stat = rbfm->scan(tblFHndl, tblAttrbs, string(), NO_OP, NULL, matchattrbs, rbfmit);
        if (stat) return stat;

        void *data = malloc(PAGE_SIZE);
        string indxfname = createIndexFileName(tableName, attributeName);
        IndexManager *im = IndexManager::instance();
        IXFileHandle ixfhndl;
        im->openFile(indxfname, ixfhndl);
        RID rid;
        while (RBFM_EOF != rbfmit.getNextRecord(rid, data)) {
            RecordField datarec;
            getDataRecordField(indxattrib, data, datarec);
            if (datarec._isnull)
                continue;
            char key[PAGE_SIZE];
            datarec.getKey(key);
            im->insertEntry(ixfhndl, indxattrib, key, rid);
        }
        im->closeFile(ixfhndl);
    }

    return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
    bool indxexists = false;
    Attribute indxattrib;
    int res = getIndexInfo(tableName, attributeName, indxattrib, indxexists);
    if (! indxexists) //index does not exists
        return -1;

    RC stat = 0;

    int tableid = getTableId(tableName);

    vector<Attribute> indxAttrs;
    createIndexAttribs(indxAttrs);

    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    FileHandle indxFHndl;
    rbfm->openFile(INDEX_FILE_NAME, indxFHndl);

    RBFM_ScanIterator rbfmit;
    vector<string> matchattrbs;
    matchattrbs.push_back(string("attribute-name"));
    stat = rbfm->scan(indxFHndl, indxAttrs, string("table-id"), EQ_OP, (void*)&tableid, matchattrbs, rbfmit);
    if (stat) return stat;

    void *data = malloc(PAGE_SIZE);
    vector<RID> indxrids;
    RID rid;
    while (RBFM_EOF != rbfmit.getNextRecord(rid, data)) {
        RecordField frecfld;
        getDataRecordField(indxattrib, data, frecfld);

        if (frecfld._isnull)
            continue;

        if (attributeName.compare(frecfld._strkey) == 0) {
            indxrids.push_back(rid);
            break;
        }
    }
    free(data);

    stat = rbfmit.close();
    if (stat) return stat;

    for (const RID& rid : indxrids) 
        rbfm->deleteRecord(indxFHndl, indxAttrs, rid);
    
    rbfm->closeFile(indxFHndl);

    IndexManager* im = IndexManager::instance();
    string indxfname = createIndexFileName(tableName, attributeName);
    stat = im->destroyFile(indxfname);
    if (stat) return stat;

    return 0;
}

RC RelationManager::indexScan(const string &tableName,
    const string &attributeName,
    const void *lowKey,
    const void *highKey,
    bool lowKeyInclusive,
    bool highKeyInclusive,
    RM_IndexScanIterator &rm_IndexScanIterator)
{
    RC stat = 0;
    IndexManager* im = IndexManager::instance();
    string indxfname = createIndexFileName(tableName, attributeName);

    
    Attribute attrib;
    bool indxExists = false;
    stat = getIndexInfo(tableName, attributeName, attrib, indxExists);

    if (!indxExists)
        return -1;

    IXFileHandle ixfhndl;
    stat = im->openFile(indxfname, ixfhndl);
    if (stat) return stat;

    IX_ScanIterator ixScanItr;
    im->scan(ixfhndl, attrib, lowKey, highKey, 
        lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator._ixScanItr);

    return 0;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key)
{ 
    //check: if scanitr is valid
//    if(_ixScanItr)

    return _ixScanItr.getNextEntry(rid, key);

//    return RM_EOF; 
};  	// Get next matching entry

RC RM_IndexScanIterator::close() 
{ 
    //check: if scanitr is valid
    //    if(_ixScanItr)
    RC stat = 0;
    IndexManager* im = IndexManager::instance();
    stat = im->closeFile(_ixScanItr._ixfileHandle);
    if (stat) return stat;

    return _ixScanItr.close();
};             			
