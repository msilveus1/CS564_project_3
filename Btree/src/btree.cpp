/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{
	IndexMetaInfo data;

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	this -> bufMgr = bufMgrIn;//initialize Buffer Manager to Buffer Manager Instance
	this -> attrByteOffset = attrByteOffset;//initialize the byte offset
	this -> attributeType = attrType;//set the attribute type
	this -> nodeOccupancy = INTARRAYNONLEAFSIZE;//initialize the occupancy of nonleaf 
	this -> leafOccupancy = INTARRAYLEAFSIZE;//initialize the occupancy of leaves

	//constructing name using code in page 3	
	std::ostringstream idxStr;
	idxStr << relationName << "." << attrByteOffset;
	std::string outIndexName = idxStr.str();//outIndexName is the name of output index file

	Page *page;

	try{
			//open the index file while it exists
			this -> file = new BlobFile(outIndexName, false);
			this -> headerPageNum = this -> file -> getFirstPageNo();
			this -> bufMgr -> readPage(file, this -> headerPageNum, page);
			IndexMetaInfo *indexInfo = reinterpret_cast<IndexMetaInfo *>(page);
			this -> bufMgr -> unPinPage(file, this -> headerPageNum, false);
			
			if(relationName != data.relationName || this -> attributeType != data.attrType || 
			this -> attrByteOffset != data.attrByteOffset){
				throw BadIndexInfoException(outIndexName);
			}
			rootPageNum = indexInfo -> rootPageNo; 
		}
		catch(FileNotFoundException e){
			//create a new index file while it doesn't exists
			this -> file = new BlobFile(outIndexName, true);

			PageId pid;
			Page *metaPage;

		}
	

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	bufMgr->flushFile(file);
    file->~File();
}

// -----------------------------------------------------------------------------
// checkOccupancy:
// checks whether keyArray is full 
// ------------------------------------------------------------------------------
const int checkOccupancy(int keyArray[],int size,int isLeaf,PageId children[],RecordId records[]){
	if(isLeaf == 1){
		if(keyArray[size - 1] == 0){
			if(keyArray[size - 2] == 0){
				return 0;
			}else{
				if(keyArray[size - 2] > 0){
					return 0;
				}else{
					if(records[size+1] == 0){
						return 0;
					}else{
						return 1;
					}
				}
			}
		}else{
			return 1;
		}
	}else{
		if(keyArray[size - 1] == 0){
			if(keyArray[size - 2] == 0){
				return 0;
			}else{
				if(keyArray[size - 2] > 0){
					return 0;
				}else{
					if(children[size+1] == 0){
						return 0;
					}else{
						return 1;
					}
				}
			}
		}else{
			return 1;
		}		
	}
}

// ------------------------------------------------------------------------------
// findIndex:
// returns index at which we should go to next
// ------------------------------------------------------------------------------
const int findIndex(int keyArray[],int size,int key){
	for(int i = 0; i < size; i++){
		if(key < keyArray[i]){
			return i;
		}else{
			if((i+1) == size){
				return size;
			}
		}
	}
}





// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	//TODO: We need to figure out how to deal with the case when the root is a leaf node or not. Perhaps some sort of boolean 
	Page* rootPage = &(file->BlobFile::readPage(rootPageNum)); 
	

	int * keyValue = (int *) key;
	if(height == 1){
		struct LeafNodeInt *rootNode = (LeafNodeInt *) rootPage;
		int currentKeys[INTARRAYNONLEAFSIZE] = rootNode->keyArray;

	


	}else{
		struct NonLeafNodeInt *rootNode = (NonLeafNodeInt *) rootPage;		
		Stack depthList(rootNode);

	}


	//This will be the boolean for the while loop 

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}
//TODO: Check rules for private and non-private declarations in C++
private:
	class Stack{
		//TODO: Check the allocation 
		struct stackNode{
			void * currentValue;
			struct stackNode nextValue;
		}
		struct stackNode currentTop;

		Stack::Stack(void *initValue){
			currentTop->currentValue = initValue;
			currentTop->nextValue = NULL;
		}
		Stack::~Stack(){
			currentTop = NULL;
		}
		const void pushNode(void * newNode){
			//Places the node on top of stack
			struct stackNode current;
			current.currentValue = newNode;
			current.nextValue = currentTop;
			currentTop = current;

		}
		const void* peek(){
			//Returns the top value without taking it off the top
			void * current;
			current = currentTop.currentValue;
			return current;
		}
		const void* pop(){
			//This returns the top value and takes it off the top
			void * current;
			current = currentTop.currentValue;
			currentTop = currentTop.nextValue;
			return current;
		}
	}

}