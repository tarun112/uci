
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <map>
#include <limits>
#include <cmath>
#include <functional>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

class RecordField {
public:
    string _strkey;
    int _intkey;
    float _realkey;
    bool _isnull = true;
    //    AttrType _type;
    Attribute _attrib;

    int getKey(void* key) const {
        if (TypeVarChar == _attrib.type) {
            int len = _strkey.length();
            memcpy((char*)key, (char*)&len, sizeof(int));
            memcpy((char*)key + sizeof(int), (char*)_strkey.c_str(), len);
            return 0;
        }
        else if (TypeInt == _attrib.type) {
            memcpy((char*)key, (char*)&_intkey, sizeof(int));
            return 0;
        }
        else if (TypeReal == _attrib.type) {
            memcpy((char*)key, (char*)&_realkey, sizeof(float));
            return 0;
        }
        return -1;
    }

    int compareKey(const RecordField& rhs) const {
        if (TypeVarChar == _attrib.type) {
            return _strkey.compare(rhs._strkey);
        }
        else if (TypeInt == _attrib.type) {
            if (_intkey < rhs._intkey)
                return -1;
            else if (_intkey == rhs._intkey)
                return 0;
            else if (_intkey > rhs._intkey)
                return 1;
        }
        else if (TypeReal == _attrib.type) {
            if (fabs(_realkey - rhs._realkey) < numeric_limits<float>::epsilon())
                return 0;
            if (_realkey < rhs._realkey)
                return -1;
            else if (_realkey > rhs._realkey)
                return 1;
        }
    }

    int length() const {
        if (TypeVarChar == _attrib.type) {
            return _strkey.length() + sizeof(int);
        }
        else if (TypeInt == _attrib.type) {
            return sizeof(int);
        }
        else if (TypeReal == _attrib.type) {
            return sizeof(float);
        }
        return -1;
    }

    bool operator <(const RecordField &rhs) const
    {
        if (compareKey(rhs) < 0)
            return true;
        return false;
    }

    size_t getKeyHash() const {
        if (TypeVarChar == _attrib.type) {
            hash<string> strhash;
            return strhash(_strkey);
        }
        else if (TypeInt == _attrib.type) {
            hash<int> inthash;
            return inthash(_intkey);
        }
        else if (TypeReal == _attrib.type) {
//            hash<float> realhash;
//            return realhash(_realkey);
            int intkey = (_realkey >=0 )? int(_realkey + .5) : int(_realkey - .5);
            hash<int> inthash;
            return inthash(intkey);
        }
    }

    //RecordField operator +(const RecordField &rhs) const
    //{
    //    RecordField orecfld = rhs; 
    //    if (TypeVarChar == _attrib.type) {
    //        orecfld._strkey =  _strkey + rhs._strkey;
    //    }
    //    else if (TypeInt == _attrib.type) {
    //        orecfld._intkey = _intkey + rhs._intkey;
    //    }
    //    else if (TypeReal == _attrib.type) {
    //        orecfld._realkey = _realkey + rhs._realkey;
    //    }
    //    return orecfld;
    //}

};

typedef map<string, RecordField> RecFldMap;

int getDataRecordFields(const vector<Attribute>& attribs, const void *data,
    vector<RecordField>& datarec);

int getDataRecordFields(const vector<Attribute>& attribs, const void *data,
    RecFldMap& datarec);

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data); // { return RM_EOF; };
  RC close(); // { return -1; };

  FileHandle fileHndl;
  RBFM_ScanIterator rbfmit;

};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
public:
    RM_IndexScanIterator() {};  	// Constructor
    ~RM_IndexScanIterator() {}; 	// Destructor

                                    // "key" follows the same format as in IndexManager::insertEntry()
    RC getNextEntry(RID &rid, void *key); // { return RM_EOF; };  	// Get next matching entry
    RC close(); // { return -1; };             			// Terminate index scan

    IX_ScanIterator _ixScanItr;
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
      const string &attributeName,
      const void *lowKey,
      const void *highKey,
      bool lowKeyInclusive,
      bool highKeyInclusive,
      RM_IndexScanIterator &rm_IndexScanIterator);

// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);


protected:
  RelationManager();
  ~RelationManager();

private:
    static RelationManager *_rm;
};

#endif
