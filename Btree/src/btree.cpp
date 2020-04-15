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
// ------------------------------------------------------------------------------
// moveKeyIndex:
// We are moving the key array over assuming we have checked occupancy
// ------------------------------------------------------------------------------
const void moveKeyIndex(int* keyArray,int size,int index){
	int lastKey = keyArray[index];
	int currentKey = 0;	
	for(int i = index+1; i < size; i++){
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

const void moveRecordIndex(RecordId* recordIdArray,int size,int index){
	RecordId lastRecord = recordIdArray[index];
	RecordId currentKey = 0;	
	for(int i = index+1; i < size; i++){
		currentKey = keyArray[i];
		keyArray[i] = lastKey;
		lastKey = currentKey;
	}			
}

const void splitDown(int isLeaf,void * Node,int* key,RecordId rid,PageId rtPgNum){
	if(isLeaf)
		//This is more of an edge case for the root node
		//This is the split for the leaf node
		int tempArray[INTARRAYLEAFSIZE+1] = {};
		RecordId tempRArray[INTARRAYLEAFSIZE +1] = {};
		LeafNodeInt* rootNode = (LeafNodeInt*) Node;
		for(int i = 0; i < INTARRAYNONLEAFSIZE ; i++){
				tempRArray[i] = rootNode->ridArray[i];
				tempArray[i] = keyArray[i];
		}
		moveKeyIndex(tempArray,INTARRAYLEAFSIZE+1,index);
		tempArray[index] = *key;
		tempRArray[index] = rid;

		//We now calculate where to split
		int spIndex = (INTARRAYLEAFSIZE)/2 + INTARRAYLEAFSIZE % 2;
		int rootKey = tempArray[spIndex];
		int newKeyArray[INTARRAYNONLEAFSIZE] = {};
		PageId newChildArray[INTARRAYNONLEAFSIZE+1] = {};
		struct NonLeafNode newRootNode = {1, newKeyArray,newChildArray};
		newKeyArray[0] = rootKey;

		//Now we get neww pages for the new nodes
		PageId& tempID_1 = NULL;
		Page *& tempPage_1 = NULL;			
		bufMgr->allocPage(file,tempID_1,tempPage_1);

		PageId& tempID_2 = NULL;
		Page *& tempPage_2 = NULL;			
		bufMgr->allocPage(file,tempID_2,tempPage_2);			

		//We now allocate the new pageIDs for our new node

		newRootNode.newChildArray[0] = tempID_1;
		newRootNode.newChildArray[1] = tempID_2;

		// We will now write all the values the new nodes to the 
		int childKeyArray_1[INTARRAYNONLEAFSIZE] = {};
		int childKeyArray_2[INTARRAYNONLEAFSIZE] = {};

		RecordId ridArray_1[INTARRAYNONLEAFSIZE] = {};
		RecordId ridArray_2[INTARRAYNONLEAFSIZE] = {};
		for(int i = 0;i < spIndex; i++){
			//We insert the left side first
			childKeyArray_1[i] = tempArray[i]
			ridArray_1[i] = tempRArray[i]
		}
		struct LeafNodeInt child_1 = {childKeyArray_1,ridArray_1,tempID_2};

		for(int i = spIndex; i < INTARRAYLEAFSIZE + 1; i++){
			//Now we write the second child
			childKeyArray_2[i-spIndex] = tempArray[i];
			ridArray_2[i-spIndex] = tempRArray[i];
		}

		struct LeafNodeInt child_2 = {childKeyArray_2,ridArray_2,NULL};
	}else{
		NonLeafNodeInt rootNode = (NonLeafNodeInt *) Node;

		int index = findIndex(rootNode->keyArray,INTARRAYONLEAFSIZE,*key);
		int tempArray[INTARRAYNONLEAFSIZE + 1] = {};		
		for(int i = 0; i < INTARRAYNONLEAFSIZE; i++){
			tempArray[i] = rootNode->keyArray[i];			
		}
		tempPageID[INTARRAYNONLEAFSIZE] = rootNode->pageNoArray[INTARRAYNONLEAFSIZE];
		moveKeyIndex(tempArray,INTARRAYLEAFSIZE+1,index);
		movePgIdIndex(tempPageID,INTARRAYNONLEAFSIZE+2,index);
		tempArray[index] = *key;

		//Now we split the index
		int spIndex = (INTARRAYNONLEAFSIZE)/2 + INTARRAYNONLEAFSIZE % 2;
	
		int childKeyArray_1[INTARRAYNONLEAFSIZE] = {};
		int childKeyArray_2[INTARRAYNONLEAFSIZE] = {};

		//Now we are moving the array of children into the new array for new nodes 	
		for(int i = 0; i < spIndex; i++){
			childKeyArray_1[i] = tempArray[i]			
		}
		for(int i = spIndex+1; i < INTARRAYNONLEAFSIZE + 1; i++){
			childKeyArray_2[i - spIndex+1] = tempArray[i];
		}
		
		//We now need to assign page ids to each node
		PageId childPageArr_1[INTARRAYNONLEAFSIZE] = {};
		PageId childPageArr_2[INTARRAYNONLEAFSIZE] = {};
		for(int i = 0; i < spIndex + 1; i++){
			childPageArr_1[i] = rootNode->pageNoArray[i];
		}

		//TODO: Figure out how get the new node pageId that got split and how to insert it at the right index
		for(int i = spIndex; i < INTARRAYNONLEAFSIZE + 1; i++) {
			childPageArr_2[i - spIndex] = rootNode->pageNoArray[i];
		}


		//We grab new pages for these new nodes
		PageId& tempID_1 = NULL;
		Page *& tempPage_1 = NULL;			
		bufMgr->allocPage(file,tempID_1,tempPage_1);

		PageId& tempID_2 = NULL;
		Page *& tempPage_2 = NULL;			
		bufMgr->allocPage(file,tempID_2,tempPage_2);


		int newKeyArray[INTARRAYNONLEAFSIZE] = {}
		newKeyArray[0] = tempArray[spIndex];
		//Now we create the new nodes
		struct NonLeafNodeInt childeNode_1 = {rootNode->level+1,childKeyArray_1,childPageArr_1};
		struct NonLeafNodeInt childeNode_2 = {rootNode->level+1,childKeyArray_2,childPageArr_2};
		stuct NonLeafNodeInt newRootNode = {rootNode->level,}




	}

}
const void splitUp(){

}


// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	//TODO: We need to figure out how to deal with the case when the root is a leaf node or not. Perhaps some sort of boolean 
	
	Page* rootPage = NULL;
	bufMgr->readPage(file,rootPageNum,rootPage);
	int * keyValue = (int *) key;

	if(height == 1){



		struct LeafNodeInt *rootNode = (LeafNodeInt *) rootPage;
		int keyArray[INTARRAYONLEAFSIZE] = rootNode->keyArray;
		if(checkOccupancy(keyArray,INTARRAYONLEAFSIZE,1,NULL,currentKeys->ridArray)){
			int index = findIndex(keyArray,INTARRAYONLEAFSIZE,*key);
			//We are now looking for the middle entry and that will become the new root node key
			//and then we will insert things accordingly 

			//First we put them into a seperate array		
			int tempArray[INTARRAYONLEAFSIZE+1] = {};
			RecordId tempRArray[INTARRAYONLEAFSIZE +1] = {};


			for(int i = 0; i < INTARRAYNONLEAFSIZE ; i++){
				tempRArray[i] = rootNode->ridArray[i];
				tempArray[i] = keyArray[i];
			}
			moveKeyIndex(tempArray,INTARRAYONLEAFSIZE+1,index);
			tempArray[index] = *key;
			tempRArray[index] = rid;
			//We now calculate where to split
			int spIndex = (INTARRAYLEAFSIZE + 1)/2 + 1;
			int rootKey = tempArray[spIndex];
			int newKeyArray[INTARRAYNONLEAFSIZE] = {};
			PageId newChildArray[INTARRAYNONLEAFSIZE+1] = {};
			struct NonLeafNode newRootNode = {1, newKeyArray,newChildArray};
			newKeyArray[0] = rootKey;
			//Now we get neww pages for the new nodes
			PageId& tempID_1 = NULL;
			Page *& tempPage_1 = NULL;			
			bufMgr->allocPage(file,tempID_1,tempPage_1);

			PageId& tempID_2 = NULL;
			Page *& tempPage_2 = NULL;			
			bufMgr->allocPage(file,tempID_2,tempPage_2);			

			//We now allocate the new pageIDs for our new node

			newRootNode.newChildArray[0] = tempID_1;
			newRootNode.newChildArray[1] = tempID_2;

			// We will now write all the values the new nodes to the 
			int childKeyArray_1[INTARRAYLEAFSIZE] = {};
			int childKeyArray_2[INTARRAYLEAFSIZE] = {};

			RecordId ridArray_1[INTARRAYLEAFSIZE] = {};
			RecordId ridArray_2[INTARRAYLEAFSIZE] = {};
			for(int i = 0;i < spIndex; i++){
				//We insert the left side first
				childKeyArray_1[i] = tempArray[i]
				ridArray_1[i] = tempRArray[i]
			}
			struct LeafNodeInt child_1 = {childKeyArray_1,ridArray_1,tempID_2};

			for(int i = spIndex; i < INTARRAYLEAFSIZE + 1; i++){
				//Now we write the second child
				childKeyArray_2[i-spIndex] = tempArray[i];
				ridArray_2[i-spIndex] = tempRArray[i];
			}
			struct LeafNodeInt child_2 = {childKeyArray_2,ridArray_2,NULL};


			//Now we assign the pointers to the proper pointers
			rootPage = (Page *) &newRootNode;
			tempPage_1 = (Page *) &child_1; 
			tempPage_2 = (Page *) &child_2;

			//Now we write these nodes to the file
			file->writePage(rootPageNum,rootPage);
			file->writePage(tempID_1,tempPage_1);
			file->writePage(tempID_2,tempPage_2);


			//We need to allocate the new noe


		}else{
			//The case when the occupancy is not filled and we move the index
			//to place new int into the tree
			//Probably the easiest case.
			//TODO: Need to check when it is greater than any other 
			int index = findIndex(keyArray, INTARRAYONLEAFSIZE, *(keyValue));
			moveKeyIndex(keyArray,INTARRAYONLEAFSIZE,index);
			moveRecordIndex(rootNode->ridArray;INTARRAYONLEAFSIZE,index);
			keyArray[index] = key;
			rootNode->ridArray[index] = rid;

		}
	


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
