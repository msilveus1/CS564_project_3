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

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	//TODO: We need to figure out how to deal with the case when the root is a leaf node or not. Perhaps some sort of boolean 
	Page* rootPage = &(file->BlobFile::readPage(rootPageNum)); 
	struct NonLeafNodeInt *rootNode = (NonLeafNodeInt *) rootPage;


	

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
private:
	class Stack{
		//TODO: Check the allocation 
		struct stackNode{
			void * currentValue;
			struct stackNode nextValue;
		}
		struct stackNode currentTop;

		Stack::Stack(void *initNode){
			currentTop->currentValue = initNode
			currentTop->nextValue = NULL;

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
