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
	this -> bufMgr = bufMgrIn;//initialize Buffer Manager to Buffer Manager Instance
	this -> attrByteOffset = attrByteOffset;//initialize the byte offset
	this -> attributeType = attrType;//set the attribute type
	this -> nodeOccupancy = INTARRAYNONLEAFSIZE;//initialize the occupancy of nonleaf 
	this -> leafOccupancy = INTARRAYLEAFSIZE;//initialize the occupancy of leaves

	//constructing name using code in page 3	
	std::ostringstream idxStr;
	idxStr << relationName << "." << attrByteOffset;
	std::string outIndexName = idxStr.str();//outIndexName is the name of output index file

	try{
			//open the index file while it exists
			this -> file = new BlobFile(outIndexName, false);
			this -> headerPageNum = this -> file -> getFirstPageNo();//get first page number for the file
			Page *page;
			this -> bufMgr -> readPage(file, this -> headerPageNum, page);//call readPage
			IndexMetaInfo *indexInfo = reinterpret_cast<IndexMetaInfo *>(page);//get the information for the exception throw later
			/** 
			 * throws  BadIndexInfoException 
			 * If the index file already exists for the corresponding attribute, but values in 
			 * metapage(relationName, attribute byte offset, attribute type etc.)*/
			if(relationName != indexInfo -> relationName || this -> attributeType != indexInfo -> attrType || 
			this -> attrByteOffset != indexInfo -> attrByteOffset){
				throw BadIndexInfoException(outIndexName);
			}
			rootPageNum = indexInfo -> rootPageNo;//set the rootPageNum
			this -> bufMgr -> unPinPage(file, this -> headerPageNum, false);//unpin page
		}
		catch(FileNotFoundException e){
			//create a new index file while it doesn't exists
			this -> file = new BlobFile(outIndexName, true);
			
			//create metadata page 
			PageId metaPid;
			Page *metaPage;
			//alloc the metadata page 
			bufMgr -> allocPage (file, metaPid, metaPage);
			headerPageNum = metaPid;//set the headerPage Number

			//create root page
			PageId rootPageID;
			Page *rootPage;
			//alloc the root page
			bufMgr -> allocPage (file, rootPageID, rootPage);
			rootPageNum = rootPageID;//set the rootpage number
			
			//initialize the root node to be an empty leaf node
			LeafNodeInt *rootNode = reinterpret_cast<LeafNodeInt*> (rootPageID);
			for(int i = 0; i < leafOccupancy; ++i) {
				rootNode->keyArray[i] = 0;
			}
			bufMgr->unPinPage(file, rootPageID, true);

			//insert a meta page's infomation to file including relationName, attrByteOffset, attrType,
			//rootPageNum
			IndexMetaInfo metaPageInfo;
			unsigned int i;
			for (i = 0; i < relationName.length();++i){
				metaPageInfo.relationName[i] = relationName[i];
			}
			metaPageInfo.attrByteOffset = attrByteOffset;
			metaPageInfo.attrType = attrType;
			metaPageInfo.rootPageNo = rootPageNum;
			//create a string of Bytes that compose the record.
			std::string metaInfoStr (reinterpret_cast<char *> (&metaPageInfo), sizeof(metaPageInfo));
			metaPage -> insertRecord(metaInfoStr);

			//Store the header meta page & root page 
			bufMgr -> flushFile(file);

			//use the FileScan to insert entries for every tuple
			FileScan scanFile(relationName, bufMgr);
			RecordId recordID;
			while(true){
				try{
					scanFile.scanNext(recordID);
					std::string storeRecords = scanFile.getRecord();
					//convert store record to struct record
					auto record = reinterpret_cast(storeRecords.c_str());
					//get the pointer to the key from it
					auto key = reinterpret_cast(&record.i);
					insertEntry((void*)(key), recordID);
				} 
				//when reach the end of the file, flush the file
				catch(EndOfFileException e){
					bufMgr -> flushFile(file);
				}
			}
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
  /**
     * Fetch the record id of the next index entry that matches the scan.
     * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
   * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
     * @throws ScanNotInitializedException If no scan has been initialized.
     * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
    **/
const void BTreeIndex::scanNext(RecordId& outRid) 
{
    //if the scan not initialized
    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }
    //set current node to be the current page
    LeafNodeInt* curNode = (LeafNodeInt*)currentPageData;
    int key = curNode->keyArray[newEntry];
    //check if key is valid
    if (is_key_in_range(key, lowval, lowOp, highval, highOp)) {
        //return the rid of the next entry
        outRid = curNode->ridArray[iterator];
        iterator++;
    }
    else {
    throw IndexScanCompletedException();
    }
    //if reach the end of node, go to sibling
    if (curNode->ridArray[iterator].page_number == 0 || iterator == INTARRAYLEAFSIZE) {
        bufMgr->unPinPage(file, currentPageNum, false);
        // if reach end of leaf
        if (curNode->rightSibPageNo == 0)
        {
            throw IndexScanCompletedException();
        }
        PageId siblingPageNum = curNode->rightSibPageNo;
        //fetch sibling page
        bufMgr->readPage(file, siblingPageNum, currentPageData);
        curNode = (LeafNodeInt*)currentPageData;
        //set iterator to 0
        iterator = 0;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{	//throws ScanNotInitializedException If no scan has been initialized.
	if (!scanExecuting) {
            throw ScanNotInitializedException();
    }
		//Unpin the current page being scanned 
        bufMgr->unPinPage(file, currentPageNum, false);
		/**
		 * terminate the scanning by setting the scanExecuting to false,
		 * set the page currently being scanned to null
		*/
        scanExecuting = false;
        currentPageData = nullptr;
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
