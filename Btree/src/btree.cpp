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

const void moveRecordIndex(RecordId* recordIdArray,int size,int index,PageId pageNo){
	RecordId lastRecord = recordIdArray[index];
	RecordId currentKey = 0;	
	for(int i = index+1; i < size; i++){
		currentKey = keyArray[i];
		keyArray[i] = lastKey;
		lastKey = currentKey;
	}			
}
const void insertMovePage(PageId tempPage[],PageId childPageId_1,PageId childPageId_2,int size){
	int index = -1;
	for(int i = 0; i < size; i++){
		if(tempPage[i] == childPageId_1){
			index = i; 
		}	
	}
	PageId lastID = tempPage[index];
	PageId currentID = 0;
	for(int i = index + 1; i < size + 1; i++){
		currentID = tempPage[i];
		tempPage[i] = lastID;
		lastID = currentID;
	}
	//We finally insert new key into the function
	tempPage[index] = childPageId_2;
}



const int split(void *childNode,int isLeaf, PageId &newID,PageId currentId,int keyValue, RecordId rid,PageId childPageId_1,pageIdArray childPageId_2){
	int size;
	if(isLeaf){
		LeafNode * childeNode_1 = (LeafNode *) childNode;
		size = INTARRAYLEAFSIZE;
		// Temp arrrays that will be eventually split
		int tempKeyArray[INTARRAYLEAFSIZE + 1] = {};
		RecordId [INTARRAYLEAFSIZE + 1] = {};
		//Placing the new values into the array
		for(int i = 0; i < size; i++){
			tempKeyArray[i] = childNode->keyArray[i];
			tempRecord[i] = childNode->ridArray[i];
		}
		//Inserting the value 
		int index = findIndex(tempKeyArray,size, keyValue);		
		//TODO: Check if there is something wrong with the size
		moveKeyIndex(index,size + 1,tempKeyArray);
		moveRecordIndex(index,size + 1,tempRecord);
		tempRecord[index] = rid;
		tempKeyArray[index] = keyValue;
		//Now we can calculate the split index
		int spIndex = (size + 1)/2 + (size + 1)%2;
		RecordId recordIdArray_1[size] = {};
		int keyArray_1[size] = {}; 
		for(int i = 0; i < spIndex; i++){
			recordIdArray_1[i] = tempRecord[i];
			keyArray_1[i] =  tempKeyArray[i];
		}
		//Part 2 of the split 
		RecordId recordIdArray_2[size] = {};
		int keyArray_2[size] = {};

		for(int i = spIndex; i < size + 1; i++){
			recordIdArray_2[i - spIndex] = tempRecord[i];
			keyArray_2[i - spIndex] = tempKeyArray[i];
		}
		LeafNode leafNode_2 = {keyArray_1,recordIdArray_1,currentId};
		//We insert the new node
		childeNode_1 = {keyArray_2,recordIdArray_2,childNode_1->rightSibPageNo};
		Page *&newPage_1 = NULL;
		Page *newPage_2 = NULL;
		BufMgr->allocPage(file,newID,newPage_1);
		newPage_1 = (Page *) leafNode_2;
		newPage_2 = (Page *) childNode_1;
		//TODO: WE need to make sure that the numbers of our variables matchupp here
		file->writePage(newID,newPage_1);
		file->writePage(currentId,newPage_2);
		return tempKeyArray[spIndex];		
	}else{
		NonLeafNode * childNode_1 = (*NonLeafNode) childNode_1;
		//We establish the two temp arrays and then transfer to them the new keys
		int tempKeyArray[INTARRAYNONLEAFSIZE + 1] = {};
		PageId tempPage[INTARRAYNONLEAFSIZE + 2] = {};


		for(int i = 0; i < INTARRAYNONLEAFSIZE; i++){
			tempKeyArray[i] = childNode_1->keyArray[i];	
			tempPage[i] = childeNode_1->pageNoArray[i];
		}
		tempPage[INTARRAYNONLEAFSIZE] = childeNode_1->pageNoArray[INTARRAYNONLEAFSIZE];
		int index = findIndex(tempKeyArray,INTARRAYNONLEAFSIZE,keyValue);
		moveKeyIndex(tempKeyArray,INTARRAYNONLEAFSIZE,index);
		//This is the means by which we specifically insert the new pageID into 
		insertMovePage(tempPage,childPageId_1,childPageId_2,INTARRAYNONLEAFSIZE + 1);
		//We calculate the index 
		int spIndex = INTARRAYNONLEAFSIZE/2 + INTARRAYNONLEAFSIZE%2;
		int childKeyArray_1[INTARRAYNONLEAFSIZE] = {};
		int childKeyArray_2[INTARRAYNONLEAFSIZE] = {};
		pageId childPage_Id_1[INTARRAYNONLEAFSIZE + 1];
		pageId childPage_Id_2[INTARRAYNONLEAFSIZE + 1];
		//Moving the keys into there seperate spots 
		for(int i = 0; i < spIndex; i++){
			childKeyArray_1[i] = tempKeyArray[i];
			child_Id_1[i] = tempPage[i];
		}
		child_Id_1[spIndex] = tempPage[spIndex];
		
		//This is the child key array 2 
		for(int i = spIndex + 1; i < INTARRAYNONLEAFSIZE; i++){
			childKeyArray_2[i-(spIndex + 1)] = tempKeyArray[i];
			child_Id_2[i - (spIndex + 1)] = tempPage[i];
		}
		child_Id_2[INTARRAYNONLEAFSIZE - spIndex + 1] = tempPage[INTARRAYNONLEAFSIZE];
		
		//Writing your pages
		Page *&newPage_1 = NULL;
		BufMgr->allocPage(file,newID,newPage_1);
		NonLeafNode childNode_1 = {childNode_1->level,childKeyArray_1,childPageId_1};
		NonLeafNode childNode_2 = {childeNode_2->level,childKeyArray_2,childPageId_2};
		newPage_1 = (Page *) &childeNode_1;
		file->writePage(newID,newPage_1);
		newPage_1 = (Page *) &childNode_2;
		file->writePage(currentID,newPage_2);
		return tempKeyArray[spIndex];		
	}
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

		if(checkOccupancy(keyArray,INTARRAYONLEAFSIZE,1,NULL,currentKeys->ridArray)){
			PageId &newID;
			//We are spliting down the array
			int key = split(rootNode,1,newID,rootPageNum,*keyValue,rid,NULL,NULL);
			//Now we do some new stuff for the new page node
			int newRootKeys[INTARRAYNONLEAFSIZE] = {key};
			PageId newChildNode[INTARRAYNONLEAFSIZE + 1] = {newID,rootPageNum};
			NonLeafNode newRootNode = {1,newRootKeys,newChildNode};
			Page *&newPage; 
			PageId &newRootNum; 
			bufMgr->allocPage(file,newRootNum,newPage);
			newPage = (NonLeafNode *) newRootNum;
			file->writePage(newRootNum,newPage);
			rootPageNum = newRootNum;
		}else{
			//The case when the occupancy is not filled and we move the index
			//to place new int into the tree
			//Probably the easiest case.
			//TODO: Need to check when it is greater than any other 
			int index = findIndex(keyArray, INTARRAYLEAFSIZE, *(keyValue));
			if(index = INTARRAYLEAFSIZE-1){
				//The case when the value is on the right most side of the node
				keyArray[index] = key;
				rootNode->ridArray[index] = rid;
			}else{
				moveKeyIndex(keyArray,INTARRAYLEAFSIZE,index);
				moveRecordIndex(rootNode->ridArray;INTARRAYLEAFSIZE,index);
				keyArray[index] = key;
				rootNode->ridArray[index] = rid;
			}
		}
	}else{		

		struct NonLeafNodeInt *rootNode = (NonLeafNodeInt *) rootPage;		
		Stack depthList(rootNode);
		PageId &currentId;
		Page *&currentPage = NULL;
		//We are now iterating down the chain to the leaf node
		for(int i = 1; i < height; i++){
			int index = findIndex(rootNode->keyArray,INTARRAYNONLEAFSIZE,*key);
			currentId = rootNode->pageNoArray[index];
			bufMgr(file,currentId,currentPage);
			rootNode = (NonLeafNode *) currentPage;
			depthList.pushNode(rootNode);
		}
		
		//Now we can cast the last one as a leaf node.
		int index = findIndex(rootNode->keyArray,INTARRAYNONLEAFSIZE,*key);
		currentId = rootNode->pageNoArray[index];
		bufMgr(file,currentId,currentPage);
		struct LeafNode * leafNode = (LeafNode *) currentPage;
		// depthList.pushNode(leafNode);
		if(checkOccupancy(leafNode->keyArray,INTARRAYLEAFSIZE))	{
			// if(checkOccupanfcy())
			PageId &newID;
			int currentkey = split(leafNode,1,newID,currentId,*keyValue,rid,NULL,NULL);
			for(int i = 0; i = height - 1; i++){
				NonLeafNode *currentNode = depthList.peek();
				if(checkOccupancy(currentNode->keyArray,INTARRAYNONLEAFSIZE)){
					split(currentNode,0,newID,currentId,key,NULL,)
				}else{
					int index = findIndex(currentNode->keyArray,INTARRAYNONLEAFSIZE,currentKey);
					moveKeyIndex(currentNode->keyArray,INTARRAYNONLEAFSIZE,index);
					insertMovePage(currentNode->keyArray,currentId,newID,INTARRAYNONLEAFSIZE);
					currentNode->keyArray[index] = currentKey;
					//Write these to memory
					break;
				}
			}



		}else{	
			int index = findIndex(leafNode->keyArray,INTARRAYLEAFSIZE,keyValue);
			if(index == (INTARRAYLEAFSIZE - 1)){
				//insertion on to the right side
				LeafNode->keyArray[index] = *keyValue;
				LeafNode->ridArray[index] = rid;
			}else{
				//Now we might need to move the key and record id over to fit it in
				moveKeyIndex(LeafNode->keyArray,INTARRAYLEAFSIZE,index);
				moveRecordIndex(LeafNode->ridArray,INTARRAYLEAFSIZE,index);
				leafNode->keyArray[index] = *key;
				leafNode->ridArray[index] = rid;
			}
			currentPage = (*Page) leafNode;
			file->writePage(currentId,currentPage);			
		}

		//Now we need to figure out what we need to do for inserting into array


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
