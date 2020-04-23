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
	outIndexName = idxStr.str();//outIndexName is the name of output index file

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
			height = 1;
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
			int key;
			while(true){
				try{
					scanFile.scanNext(recordID);
					std::string storeRecords = scanFile.getRecord();//store the record in a string
					const char *record = storeRecords.c_str();
					key = *((int *)(record + attrByteOffset));
					insertEntry((void*)&key, recordID);
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
				// Case:  {... ,0,0}
				return 0;
			}else{
				//Case: {...,1,0} where 1 is standin for some positive number
				if(keyArray[size - 2] > 0){
					return 0;
				}else{
					if(records[size+1] == 0){
						//Case: keys = {..., -1, 0}, records = {..., 1, 0}
						return 0;
					}else{
						//Case: keys = {..., -1,0},  records = {..., 1, 1}
						return 1;
					}
				}
			}
		}else{
			//Case {..,1}
			return 1;
		}
	}else{
		if(keyArray[size - 1] == 0){
			if(keyArray[size - 2] == 0){
				// Case:  {... ,0,0}
				return 0;
			}else{
				if(keyArray[size - 2] > 0){
					//Case: {...,1,0}
					return 0;
				}else{
					if(children[size+1] == 0){
						//Case: keys = {...,-1,0}, childArray = {..., 0}
						return 0;
					}else{
						//Case: keys = {...,-1,0}, childArray = {...,1}
						return 1;
					}
				}
			}
		}else{
			//Case: {...,1}
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
		//Case: The first key from key array that is greater than the key inserted
		if(key > keyArray[i]){
			return i;
		}else if(key == keyArray[i]){
			throw duplicateKeyException();
		}else{
			// Assuming we are i serting an array with actual size + 1  
			if((i+1) == size){
				return size;
			}
		}
	}
}
// ------------------------------------------------------------------------------
// moveKeyIndex:
// We are moving the key array over assuming we have checked occupancy
// ------------------------------------------------------------------------------
const void moveKeyIndex(int* keyArray,int size,int index){
	int lastKey = keyArray[index];
	int currentKey = 0;	
	for(int i = index+1; i < size + 1; i++){
		currentKey = keyArray[i];
		keyArray[i] = lastKey;
		lastKey = currentKey;
	}		
}
const void movePgIdIndex(PageId* pageIdArray,int size,int index){
	PageId lastKey = pageIdArray[index];
	PageId currentKey = 0;	
	for(int i = index+1; i < size; i++){
		currentKey = pageIdArray[i];
		pageIdArray[i] = lastKey;
		lastKey = currentKey;
	}		
}

const void moveRecordIndex(RecordId recordIdArray[],int size,int index,PageId pageNo){
	RecordId lastRecord = recordIdArray[index];
	RecordId currentRecord;
	for(int i = index+1; i < size; i++){
		currentRecord = recordIdArray[i];
		recordIdArray[i] = lastRecord;
		lastRecord = currentRecord;
	}			
}
const void insertMovePage(PageId tempPage[],PageId childPageId_1,PageId childPageId_2,int size){
	int index = -1;

	//We assume that childPageId_1 is already present in tempPage	
	for(int i = 0; i < size; i++){
		if(tempPage[i] == childPageId_1){
			index = i; 
		}	
	}

	//these are for moving the the ids over
	PageId lastID = tempPage[index + 1];
	PageId currentID = 0;


	for(int i = index + 1; i < size + 1; i++){
		currentID = tempPage[i];
		tempPage[i] = lastID;
		lastID = currentID;
	}

	//We finally insert new key into the function
	tempPage[index + 1] = childPageId_2;
}

const void BTreeIndex::correctHeight(){
	Page *&newPage
	// BufMgr->readPage(file,rootPageNum,newPage);
	// NonLeafNode *currentNode = (NonLeafNode *) newPage;
	Queue queueList(&rootPageNum);
	// int isLeaf = 0;
	for(int i = 0; i < height - 1; i++){
		PageId currentArray[(int) pow((INTARRAYNONLEAFSIZE + 1),i+1)] = {};
		int j = 0;
		while(!queueList.isEmpty()){
			PageId current = *((PageId *) queueList.pop());
			Page *tempPage = 0;
			Page *&newPage = tempPage;
			bufMgr->readPage(file,current,newPage);
			NonLeafNodeInt *currentNode = (NonLeafNodeInt *) newPage;
			for(int k = j * (INTARRAYNONLEAFSIZE + 1); k < (j + 1) * INTARRAYNONLEAFSIZE; k++){
				currentArray[k] = currentNode->keyArray[k - (j * (INTARRAYNONLEAFSIZE + 1))];
			}
			if(i != 0){
				currentNode->level++;
			}
		}
		for(int k = 0; k < pow((INTARRAYNONLEAFSIZE + 1),i+1); k++){
			queueList.pushNode(&currentArray[k]);
		}
	}
}


const int BTreeIndex::split(void *childNode,int isLeaf, PageId &newID,PageId currentId,int keyValue, RecordId rid,PageId childPageId_1,PageId childPageId_2){
	// this->file;
	try{
		if(isLeaf){
			//The case that inserted node is a leaf node
			LeafNodeInt * childNode_1 = (LeafNodeInt *) childNode;
			int size = INTARRAYLEAFSIZE;
			
			// Temp arrrays that will be eventually split
			int tempKeyArray[INTARRAYLEAFSIZE+1] = {};
			RecordId tempRecord[INTARRAYLEAFSIZE+1] = {};

			//Placement the new values into a temporay array that is larger than the node keya array size 
			for(int i = 0; i < size; i++){
			
				tempKeyArray[i] = childNode_1->keyArray[i];
				tempRecord[i] = childNode_1->ridArray[i];
			}
			//Then we can use the find index function to help find the index where to insert the key
			int index = findIndex(tempKeyArray,size, keyValue);		
			

			//Now we are going to move the key and or the record over 
			moveKeyIndex(tempKeyArray,size + 1,index);
			moveRecordIndex(tempRecord,size + 1,index);

			//We then set the value of the temp arrays to the value because we moved everything over
			tempRecord[index] = rid;
			tempKeyArray[index] = keyValue;

			//Now we can calculate the split index
			//example: {5,8,10,12,15,17}
			//size = 5, spIndex = (5+1)/2 + (5 + 1)%2 = 3
			// tempKeyArray[spIndex] = 12
			// resulting key arrays:
			// child_key_1 = {5,8,10}, child_key_2 = {12,15,17}
			
			//Part 1: We look at the left side of the split 
			int spIndex = (size + 1)/2 + (size + 1)%2;
			// RecordId recordIdArray_1[size] = {};
			// int keyArray_1[size] = {}; 
			LeafNodeInt leafNode_1 = {{},{}, newID};
			for(int i = 0; i < spIndex; i++){
				leafNode_1.ridArray[i] = tempRecord[i];
				leafNode_1.keyArray[i] =  tempKeyArray[i];
			}


			//Part 2: We look at the right side of the split
			
			LeafNodeInt leafNode_2 = {{},{},childNode_1->rightSibPageNo};
			for(int i = spIndex; i < size + 1; i++){
				leafNode_2.ridArray[i - spIndex] = tempRecord[i];
				leafNode_2.keyArray[i - spIndex] = tempKeyArray[i];
			}


			//Creation of the new leaf nodes 
			// Design Choice: The right side will be associated with current ID so that there is no need to reasign the
			// a leaf that is not currently found here
			
			Page *newPage_1 = 0; 
			Page *tempPage_1 = 0;
			Page *&newPage_2 = tempPage_1;
			bufMgr->allocPage(file,newID,newPage_2);
			
			PageId temp_test = newID;



			
			newPage_1 = (Page *) &leafNode_1;
			newPage_2 = (Page *) &leafNode_2;
			
			//The keys then are writen to the file
			PageId tempPage = newID;
			PageId tempPageID = currentId;
			file->writePage(tempPage,*newPage_1);
			file->writePage(tempPageID,*newPage_2);

			//This is the key that got split up
			return tempKeyArray[spIndex];		
		}else{
			//Case when we have a non-leaf node split


			NonLeafNodeInt * childNode_1 = (NonLeafNodeInt *) childNode_1;

			//We establish the two temp arrays that will be split between the two nodes
			int tempKeyArray[INTARRAYNONLEAFSIZE + 1] = {};
			PageId tempPage[INTARRAYNONLEAFSIZE + 2] = {};

			//A transfer of keys from the orign
			for(int i = 0; i < INTARRAYNONLEAFSIZE; i++){
				tempKeyArray[i] = childNode_1->keyArray[i];	
				tempPage[i] = childNode_1->pageNoArray[i];
			}


			// Adding the last pageID to temporary page array
			tempPage[INTARRAYNONLEAFSIZE] = childNode_1->pageNoArray[INTARRAYNONLEAFSIZE];

			// We find the index where the split should occur and move it
			int index = findIndex(tempKeyArray,INTARRAYNONLEAFSIZE,keyValue);
			moveKeyIndex(tempKeyArray,INTARRAYNONLEAFSIZE,index);
			


			//This is making sure the pageid is moving over and we are inserting the new one
			insertMovePage(tempPage,childPageId_1,childPageId_2,INTARRAYNONLEAFSIZE + 1);
			

			//We calculate the index for the split 
			int spIndex = INTARRAYNONLEAFSIZE/2 + INTARRAYNONLEAFSIZE%2;


			
			NonLeafNodeInt childNode_1_1 = {{},{},childPageId_1};
			NonLeafNodeInt childNode_2 = {{},{},childPageId_2};
																	 
			//Part 1: We split the left
			for(int i = 0; i < spIndex; i++){
				childNode_1_1.keyArray[i] = tempKeyArray[i];
				childNode_1_1.pageNoArray[i] = tempPage[i];
			}
			childNode_1_1.pageNoArray[spIndex] = tempPage[spIndex];
			
			

			//We split the right
			for(int i = spIndex + 1; i < INTARRAYNONLEAFSIZE; i++){
				childNode_2.keyArray[i-(spIndex + 1)] = tempKeyArray[i];
				childNode_2.pageNoArray[i - (spIndex + 1)] = tempPage[i];
			}
			childNode_2.pageNoArray[INTARRAYNONLEAFSIZE - spIndex + 1] = tempPage[INTARRAYNONLEAFSIZE];
			
			//Writing your pages
			Page *tempPage_2 = 0;
			Page *&newPage_1 = tempPage_2;
			bufMgr->allocPage(file,newID,newPage_1);
			//for consistency the currentId is associated with the left side of the split while the newID is associated with the right side of the split
			newPage_1 = (Page *) &childNode_1_1;	
			PageId tempPageId = currentId;	
			file->writePage(tempPageId,*newPage_1);
			newPage_1 = (Page *) &childNode_2;
			PageId tempId_2 = newID;
			file->writePage(tempId_2,*newPage_1);

			//We return the key
			return tempKeyArray[spIndex];		
		}
	} catch(duplicateKeyException e){
		throw duplicateKeyException();
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	// The try catch is for a duplicate key instance.  
	// It is easier than having some condition checking
	try{		
		Page* rootPage = NULL;
		this->bufMgr->readPage(file,rootPageNum,rootPage);
		int * keyValue = (int *) key;

		if(this->height == 1){
			//Edge Case: When root node is leafNode 
			//

			struct LeafNodeInt *rootNode = (LeafNodeInt *) rootPage;

			if(checkOccupancy(rootNode->keyArray,INTARRAYLEAFSIZE,1,NULL,rootNode->ridArray)){
				/**
				// Case: When Root Node is full
				// example: | 3 | 5 | 10 | 20 | 
				//		   /    |	|	 |	   \
				//		  *		*   *	 *      *
				*/
				PageId tempID = 0;
				PageId &newID = tempID;
				// We are spliting down the array
				// However we are encapsulating this in a helper function to increase reusabilty
				int key = split(rootNode,1,newID,this->rootPageNum,*keyValue,rid,NULL,NULL);
				// in the example 7 was inserted and is the key that gets pushed up
				// We just need to allocate it as a root node 
				/**
				// example:        | 7 |
				//                /     \
				//	  	 | 3 | 5 |       | 7 | 10 | 20 |
				*/


				//We create a new root node.
				// int newRootKeys[INTARRAYNONLEAFSIZE] = {key};

				
				NonLeafNodeInt newRootNode = {1,{key},{this->rootPageNum,newID}};
				
				//We write the new page to memory
				Page *tempPage = 0;			
				Page *&newPage = tempPage; 
				PageId tempRootNum = 0;
				PageId &newRootNum = tempRootNum; 
				this->bufMgr->allocPage(file,newRootNum,newPage);
				newPage = (Page *) &newRootNode;
				PageId tempValue = tempRootNum;
				this->file->writePage(tempValue,*newPage);
				
				//We then maintain some externals
				this->rootPageNum = newRootNum;
				this->height++;
				//A level corrective measure
				correctHeight();
			}else{
				// We simply insert the new key and the new rid 
				// Starting with finding the index were we need to add
				int index = findIndex(rootNode->keyArray, INTARRAYLEAFSIZE, *(keyValue));
				/**
				// Case: When the index is the right most key slot in the array
				// example: key_to_insert = 20
				//        | 11 | 12 | 13 | 15| * |
				//       /     |	|    |   |    \
				//      *	   *    *    *   *     *
				*/
				if(index == INTARRAYLEAFSIZE-1){
					rootNode->keyArray[index] = *keyValue;
					rootNode->ridArray[index] = rid;
				}else{
					//Case: When the index is in the middle somewhere
					
					//We move the values over at these points
					moveKeyIndex(rootNode->keyArray,INTARRAYLEAFSIZE,index);
					moveRecordIndex(rootNode->ridArray,INTARRAYLEAFSIZE,index);
				
					//We set the values
					rootNode->keyArray[index] = *keyValue;
					rootNode->ridArray[index] = rid;


				}
			}
		}else { 
			//Case: This is the more generic case when the root node is not a leaf node
			NonLeafNodeInt *rootNode = (NonLeafNodeInt *) rootPage;		
			
			// The stacks are to help us keep track of the nodes that have been visited already
			// Simulating recursion but more memory efficient
			Stack depthListNode(rootNode);
			Stack depthListID(&rootPageNum);

			//These serve as place holders in our loop
			PageId tempID = 0;
			PageId &currentId = tempID;
			Page *tempPage = 0;
			Page *&currentPage = tempPage;
			//We are now iterating down the chain to the leaf node
			//We Start at one because we alread added the root node to our list
			
			for(int i = 1; i < height - 1; i++){
				//We find the index  This works stil because the child page id array
				// is one size larger than the key array
				int index = findIndex(rootNode->keyArray,INTARRAYNONLEAFSIZE,*keyValue);
				currentId = rootNode->pageNoArray[index];
				//Reading of the new Node into memory
				this->bufMgr->readPage(file,currentId,currentPage);
				//Casting the new node to a non-leaf node
				rootNode = (NonLeafNodeInt *) currentPage;
				//We push this level of itteration on to tree
				depthListNode.pushNode(rootNode);
				depthListID.pushNode(&currentId);
			}
			
			//Now we can cast the last one as a leaf node.
			int index = findIndex(rootNode->keyArray,INTARRAYNONLEAFSIZE,*keyValue);
			currentId = rootNode->pageNoArray[index];
			this->bufMgr->readPage(file,currentId,currentPage);
			LeafNodeInt * leafNode = (LeafNodeInt *) currentPage;
			
			// A check is completed for a the need  to split
			if(checkOccupancy(leafNode->keyArray,INTARRAYLEAFSIZE,1,NULL,leafNode->ridArray))	{
				PageId tempID = 0;
				PageId &newID = tempID;
				//We complete the initial split
				int currentkey = split(leafNode,1,newID,currentId,*keyValue,rid,NULL,NULL);

				//These are the children Ids that may nee to be used in the next split 
				PageId child_Id_1 = currentId;
				PageId child_Id_2 = newID;
				
				//We iterate back up the stack to verify 
				for(int i = 0; i < height; i++){				
					NonLeafNodeInt *currentNode = (NonLeafNodeInt *) depthListNode.peek();
					PageId *currentId = (PageId *) depthListID.peek();
					if(checkOccupancy(currentNode->keyArray,INTARRAYNONLEAFSIZE,0,currentNode->pageNoArray,NULL)){
						if(i + 1 == height){
							//Case: a non-leaf root node is full and a split is neccesary 						
							currentkey = split(currentNode,0,newID,*currentId,currentkey,NULL,child_Id_1,child_Id_2);

							//The set up for the new root node
						
							NonLeafNodeInt newNode = {1, {currentkey},{*currentId,newID}};

							//Placement of page into memory of the root node
							Page *tempPage2 = 0;
							Page *&newPage = tempPage2;
							PageId tempID = 0;
							PageId &newID_1 = tempID;
							this->bufMgr->allocPage(this->file,newID_1,newPage);
							newPage = (Page *) &newNode;
							PageId tempID2 = newID_1;
							this->file->writePage(tempID2,*newPage);

							//Taking care of some externals
							this->rootPageNum = newID_1;
							this->height++;
							correctHeight();
	 					}else{
							currentkey = split(currentNode,0,newID,*currentId,currentkey,NULL,child_Id_1,child_Id_2);
							//We reassign the two child nodes for the next time we go through
							child_Id_1 = newID;
							child_Id_2 = *currentId;
							//Now we finish with poping the last one off the top.
							depthListNode.pop();
							depthListID.pop();
						}
					}else{
						//Case: a non leaf node has room for us to insert the key
						int index = findIndex(currentNode->keyArray,INTARRAYNONLEAFSIZE,currentkey);
						moveKeyIndex(currentNode->keyArray,INTARRAYNONLEAFSIZE,index);
						//We need to move the pageId children over 1
						insertMovePage(currentNode->pageNoArray,currentId,newID,INTARRAYNONLEAFSIZE);
						currentNode->keyArray[index] = currentkey;
						
						break;
					}
				}
			}else{
				 	
				int index = findIndex(leafNode->keyArray,INTARRAYLEAFSIZE,*keyValue);
				if(index == (INTARRAYLEAFSIZE - 1)){
					//insertion on to the right side
					leafNode->keyArray[index] = *keyValue;
					leafNode->ridArray[index] = rid;
				}else{
					//Now we might need to move the key and record id over to fit it in
					moveKeyIndex(leafNode->keyArray,INTARRAYLEAFSIZE,index);
					moveRecordIndex(leafNode->ridArray,INTARRAYLEAFSIZE,index);
					leafNode->keyArray[index] = *keyValue;
					leafNode->ridArray[index] = rid;
				}
				currentPage = (Page *) leafNode;
				PageId tempId_3 = (PageId) currentId;
				this->file->writePage(tempId_3,*currentPage);			
			}

			//Now we need to figure out what we need to do for inserting into array
		}
	} catch(duplicateKeyException &e){
		// Silence is Golden.
		// Do nothing		
	}

		//This will be the boolean for the while loop 
}

    // Helper function to find the page ID of the next level of page, return the  page ID of node in the next level

    const void BTreeIndex::find_next_nonleaf_node(NonLeafNodeInt* curpage, PageId& nextpageID, int key)
    {
        int q = nodeOccupancy;
        //compare the key values in the node with the parameter key, if greater go left
        while (q > 0 && (curpage->keyArray[q - 1] >= key))
        {
            q--;
        }
        //if the child page is null, go left
        while (q >= 0 && (curpage->pageNoArray[q] == 0))
        {
            q--;
        }
        //return the page ID
        nextpageID = curpage->pageNoArray[q];
    }
    // -----------------------------------------------------------------------------
    // BTreeIndex::startScan
    // -----------------------------------------------------------------------------
     /**
         * Begin a filtered scan of the index.  For instance, if the method is called
         * using ("a",GT,"d",LTE) then we should seek all entries with a value
         * greater than "a" and less than or equal to "d".
         * If another scan is already executing, that needs to be ended here.
         * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
         * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
       * @param lowVal	Low value of range, pointer to integer / double / char string
       * @param lowOp		Low operator (GT/GTE)
       * @param highVal	High value of range, pointer to integer / double / char string
       * @param highOp	High operator (LT/LTE)
       * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values
       * @throws  BadScanrangeException If lowVal > highval
         * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
        **/
    const void BTreeIndex::startScan(const void* lowValParm,
        const Operator lowOpParm,
        const void* highValParm,
        const Operator highOpParm)
    {
        lowValInt = *((int*)lowValParm);
        highValInt = *((int*)highValParm);
        //throws  BadScanrangeException If lowVal > highval
        if (lowValInt > highValInt) {
            throw BadScanrangeException();
        }
        //throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values
        if ((lowOpParm == GT or lowOpParm == GTE) and (highOpParm == LT or highOpParm == LTE) == false) {
            throw BadOpcodesException();
        }
        //If another scan is already executing, that needs to be ended here.
        if (scanExecuting)
        {
            endScan();
        }


        //scanning root page to the buffer pool
        bufMgr->readPage(file, rootPageNum, currentPageData);
        currentPageNum = rootPageNum;
        // in case the root is a non-leaf 
        if (height != 1) {
            NonLeafNodeInt* curNode = (NonLeafNodeInt*)currentPageData;
            bool check_level = false;
            //interate till reach the curNode->level =1 
            while (!check_level) {
                curNode = (NonLeafNodeInt*)currentPageData;
                //once found the correct level right above leaf
                if (curNode->level == 1) {
                    check_level = true;
                }
                PageId nextPageID;
                //fetch the next page Id at the next level
                find_next_nonleaf_node(curNode, nextPageID, lowValInt);
                //unpin page for current page
                bufMgr->unPinPage(file, currentPageNum, false);
                currentPageNum = nextPageID;
                //pin page for this leaf node
                bufMgr->readPage(file, currentPageNum, currentPageData);


            }
        }

        //assume, we are at the leaf node
        int iter = 0;
        bool is_found = false;
        while (!is_found) {
            LeafNodeInt* curNode = (LeafNodeInt*)currentPageData;
            //check if the node is empty
            if (curNode->ridArray[iter].page_number == 0) {
                throw NoSuchKeyFoundException();
            }
            //iterate through the node
            for (int i = 0; i < leafOccupancy; i++) {
                int key = curNode->keyArray[i];
                //cases found the key
                if (is_key_in_range(key, lowValInt, lowOpParm, highValInt, highOpParm)) {
                    is_found = true;
                    nextEntry = i;
                    scanExecuting = true;
                    break;
                }
                else if (!(highOp == LTE and key <= highValInt) && (highOp == LT and key < highValInt)) {
                    throw NoSuchKeyFoundException();
                }

                // if did not find any matching key in this leaf, go to its sibling
                if (i == leafOccupancy - 1) {
                    //unpin page
                    bufMgr->unPinPage(file, currentPageNum, false);
                    //if even did not find the matching one in right leaf
                    if (curNode->rightSibPageNo == 0)
                    {
                        throw NoSuchKeyFoundException();
                    }
                    //else
                    currentPageNum = curNode->rightSibPageNo;
                    //read the siblin page and pin it
                    bufMgr->readPage(file, currentPageNum, currentPageData);
                }

            }


        }



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
        //if reach the end of node, go to sibling
        if (curNode->ridArray[nextEntry].page_number == 0 || nextEntry == leafOccupancy) {
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
            //set the index of next entry to 0
            nextEntry = 0;
        }

        int key = curNode->keyArray[nextEntry];
        //check if key is valid
        if (is_key_in_range(key, lowValInt, lowOp, highValInt, highOp)) {
            //return the rid of the next entry
            outRid = curNode->ridArray[nextEntry];
            nextEntry++;
        }
        else {
            throw IndexScanCompletedException();
        }
    }

        // Helper function to see whether the key is in the range

    const bool BTreeIndex::is_key_in_range(int key, int lowVal, const Operator lowOp, int highVal, const Operator highOp)
    {
        if (lowOp == GT) {
            if (highOp == LT)
                return key > lowVal && key < highVal;
            else
                return key > lowVal && key <= highVal;
        }
        if (lowOp == GTE) {
            if (highOp == LT)
                return key >= lowVal && key < highVal;
            else
                return key >= lowVal && key <= highVal;
        }
        return false;
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


}
