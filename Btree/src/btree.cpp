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



const int split(void *childNode,int isLeaf, PageId &newID,PageId currentId,int keyValue, RecordId rid,PageId childPageId_1,PageId childPageId_2){
	
	if(isLeaf){
		//The case that inserted node is a leaf node
		LeafNode * childeNode_1 = (LeafNode *) childNode;
		size = INTARRAYLEAFSIZE;
		
		// Temp arrrays that will be eventually split
		int tempKeyArray[INTARRAYLEAFSIZE + 1] = {};
		RecordId [INTARRAYLEAFSIZE + 1] = {};

		//Placement the new values into a temporay array that is larger than the node keya array size 
		for(int i = 0; i < size; i++){
		
			tempKeyArray[i] = childNode->keyArray[i];
			tempRecord[i] = childNode->ridArray[i];
		}
		//Then we can use the find index function to help find the index where to insert the key
		int index = findIndex(tempKeyArray,size, keyValue);		
		

		//Now we are going to move the key and or the record over 
		moveKeyIndex(index,size + 1,tempKeyArray);
		moveRecordIndex(index,size + 1,tempRecord);

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
		RecordId recordIdArray_1[size] = {};
		int keyArray_1[size] = {}; 
		for(int i = 0; i < spIndex; i++){
			recordIdArray_1[i] = tempRecord[i];
			keyArray_1[i] =  tempKeyArray[i];
		}


		//Part 2: We look at the right side of the split
		RecordId recordIdArray_2[size] = {};
		int keyArray_2[size] = {};
		for(int i = spIndex; i < size + 1; i++){
			recordIdArray_2[i - spIndex] = tempRecord[i];
			keyArray_2[i - spIndex] = tempKeyArray[i];
		}


		//Creation of the new leaf nodes 
		// Design Choice: The right side will be associated with current ID so that there is no need to reasign the
		// a leaf that is not currently found here
		
		Page *newPage_1 = NULL;
		Page *&newPage_2 = NULL;
		BufMgr->allocPage(file,newID,newPage_2);
		
		LeafNode leafNode_1 = {keyArray_1,recordIdArray_1,newID};
		LeafNode leafNode_2 = {keyArray_2,recordIdArray_2,childNode_1->rightSibPageNo};
		
		newPage_1 = (Page *) &leafNode_1;
		newPage_2 = (Page *) &leafNode_2;
		
		//The keys then are writen to the file
		file->writePage(newID,newPage_1);
		file->writePage(currentId,newPage_2);

		//This is the key that got split up
		return tempKeyArray[spIndex];		
	}else{
		//Case when we have a non-leaf node split


		NonLeafNode * childNode_1 = (*NonLeafNode) childNode_1;

		//We establish the two temp arrays that will be split between the two nodes
		int tempKeyArray[INTARRAYNONLEAFSIZE + 1] = {};
		PageId tempPage[INTARRAYNONLEAFSIZE + 2] = {};

		//A transfer of keys from the orign
		for(int i = 0; i < INTARRAYNONLEAFSIZE; i++){
			tempKeyArray[i] = childNode_1->keyArray[i];	
			tempPage[i] = childeNode_1->pageNoArray[i];
		}


		// Adding the last pageID to temporary page array
		tempPage[INTARRAYNONLEAFSIZE] = childeNode_1->pageNoArray[INTARRAYNONLEAFSIZE];

		// We find the index where the split should occur and move it
		int index = findIndex(tempKeyArray,INTARRAYNONLEAFSIZE,keyValue);
		moveKeyIndex(tempKeyArray,INTARRAYNONLEAFSIZE,index);
		


		//This is making sure the pageid is moving over and we are inserting the new one
		insertMovePage(tempPage,childPageId_1,childPageId_2,INTARRAYNONLEAFSIZE + 1);
		

		//We calculate the index for the split 
		int spIndex = INTARRAYNONLEAFSIZE/2 + INTARRAYNONLEAFSIZE%2;

		//We make new child key arrays
		int childKeyArray_1[INTARRAYNONLEAFSIZE] = {};
		int childKeyArray_2[INTARRAYNONLEAFSIZE] = {};

		//The child page id arrays
		pageId childPage_Id_1[INTARRAYNONLEAFSIZE + 1];
		pageId childPage_Id_2[INTARRAYNONLEAFSIZE + 1];
		

		//  example:   |   25   |   50   |   66   |   300   |        ==>                     |  66  |
		//   		  /         |        |        |          \ 					            /        \
		//	    |20|22|      |25|29|  |50|60|  |66|80|    |300|350|			 |  25  |  50  |			|  300  |
		//																	/		|		\		   /		 \
		//															 |20|22|	 |25|29|  |50|60|  |66|80| 	  |300|350| 
		//Part 1: We split the left
		for(int i = 0; i < spIndex; i++){
			childKeyArray_1[i] = tempKeyArray[i];
			child_Id_1[i] = tempPage[i];
		}
		child_Id_1[spIndex] = tempPage[spIndex];
		
		

		//We split the right
		for(int i = spIndex + 1; i < INTARRAYNONLEAFSIZE; i++){
			childKeyArray_2[i-(spIndex + 1)] = tempKeyArray[i];
			child_Id_2[i - (spIndex + 1)] = tempPage[i];
		}
		child_Id_2[INTARRAYNONLEAFSIZE - spIndex + 1] = tempPage[INTARRAYNONLEAFSIZE];
		
		//Writing your pages
		Page *&newPage_1 = NULL;
		BufMgr->allocPage(file,newID,newPage_1);
		//for consistency the currentId is associated with the left side of the split while the newID is associated with the right side of the split
		NonLeafNode childNode_1 = {childNode_1->level,childKeyArray_1,childPageId_1};
		NonLeafNode childNode_2 = {childeNode_2->level,childKeyArray_2,childPageId_2};
		newPage_1 = (Page *) &childeNode_1;		
		file->writePage(currentId,newPage_1);
		newPage_1 = (Page *) &childNode_2;
		file->writePage(newID,newPage_2);

		//We return the key
		return tempKeyArray[spIndex];		
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	
	
	Page* rootPage = NULL;
	bufMgr->readPage(file,rootPageNum,rootPage);
	int * keyValue = (int *) key;

	if(height == 1){
		//Edge Case: When root node is leafNode 
		//

		struct LeafNodeInt *rootNode = (LeafNodeInt *) rootPage;

		if(checkOccupancy(keyArray,INTARRAYONLEAFSIZE,1,NULL,currentKeys->ridArray)){
			// Case: When Root Node is full
			// example: | 3 | 5 | 10 | 20 | 
			//		   /    |	|	 |	   \
			//		  *		*   *	 *      *


			PageId &newID;
			// We are spliting down the array
			// However we are encapsulating this in a helper function to increase reusabilty
			int key = split(rootNode,1,newID,rootPageNum,*keyValue,rid,NULL,NULL);
			// in the example 7 was inserted and is the key that gets pushed up
			// We just need to allocate it as a root node 
			// example:        | 7 |
			//                /     \
			//	  	 | 3 | 5 |       | 7 | 10 | 20 |



			//We create a new root node.
			int newRootKeys[INTARRAYNONLEAFSIZE] = {key};			
			PageId newChildNode[INTARRAYNONLEAFSIZE + 1] = {rootPageNum,newID};
			NonLeafNode newRootNode = {1,newRootKeys,newChildNode};
			
			//We write the new page to memory			
			Page *&newPage; 
			PageId &newRootNum; 
			bufMgr->allocPage(file,newRootNum,newPage);
			newPage = (NonLeafNode *) newRootNode;
			file->writePage(newRootNum,newPage);
			
			//We then maintain some externals
			rootPageNum = newRootNum;
			height++;
			//TODO: add a height corrective measure function

		}else{
			// We simply insert the new key and the new rid 
			// Starting with finding the index were we need to add
			int index = findIndex(keyArray, INTARRAYLEAFSIZE, *(keyValue));
			
			// Case: When the index is the right most key slot in the array
			// example: key_to_insert = 20
			//        | 11 | 12 | 13 | 15| * |
			//       /     |	|    |   |    \
			//      *	   *    *    *   *     *
			if(index == INTARRAYLEAFSIZE-1){
				keyArray[index] = key;
				rootNode->ridArray[index] = rid;
			}else{
				//Case: When the index is in the middle somewhere
				
				//We move the values over at these points
				moveKeyIndex(keyArray,INTARRAYLEAFSIZE,index);
				moveRecordIndex(rootNode->ridArray;INTARRAYLEAFSIZE,index);
			
				//We set the values
				keyArray[index] = key;
				rootNode->ridArray[index] = rid;

			}
		}
	}else{		
		//Case: This is the more generic case when the root node is not a leaf node
		NonLeafNodeInt *rootNode = (NonLeafNodeInt *) rootPage;		
		
		// The stacks are to help us keep track of the nodes that have been visited already
		// Simulating recursion but more memory efficient
		Stack depthListNode(rootNode);
		Stack depthListID(&rootPageNum);

		//These serve as place holders in our loop
		PageId &currentId;
		Page *&currentPage = NULL;
		//We are now iterating down the chain to the leaf node
		//We Start at one because we alread added the root node to our list
		// example:
		// | | 			
		//    \
		//     | |          i = 1   
		//        \   
 		//         | |      i = 2  Stops here  
		//            \ 
		//             | |
		for(int i = 1; i < height - 1; i++){
			//We find the index  This works stil because the child page id array
			// is one size larger than the key array
			int index = findIndex(rootNode->keyArray,INTARRAYNONLEAFSIZE,*key);
			currentId = rootNode->pageNoArray[index];
			//Reading of the new Node into memory
			bufMgr->readPage(file,currentId,currentPage);
			//Casting the new node to a non-leaf node
			rootNode = (NonLeafNode *) currentPage;
			//We push this level of itteration on to tree
			depthList.pushNode(rootNode);
			depthListID.pushNode(&currentID);
		}
		
		//Now we can cast the last one as a leaf node.
		int index = findIndex(rootNode->keyArray,INTARRAYNONLEAFSIZE,*key);
		currentId = rootNode->pageNoArray[index];
		bufMgr->readPage(file,currentId,currentPage);
		LeafNode * leafNode = (LeafNode *) currentPage;
		
		// A check is completed for a the need  to split
		if(checkOccupancy(leafNode->keyArray,INTARRAYLEAFSIZE,1,leafNode->ridArray,NULL))	{
			
			PageId &newID;
			//We complete the initial split
			int currentkey = split(leafNode,1,newID,currentId,*keyValue,rid,NULL,NULL);

			//These are the children Ids that may nee to be used in the next split 
			PageId child_Id_1 = currentId;
			PageId child_Id_2 = newID;
			
			//We iterate back up the stack to verify 
			for(int i = 0; i < height; i++){				
				NonLeafNode *currentNode = depthList.peek();
				PageId *currentId = depthListID.peek();
				if(checkOccupancy(currentNode->keyArray,INTARRAYNONLEAFSIZE)){
					if(i + 1 = height){
						//Case: a non-leaf root node is full and a split is neccesary 						
						currentKey = split(currentNode,0,newID,*currentId,currentKey,NULL,child_Id_1,child_Id_2);

						int newKeyArray[INTARRAYNONLEAFSIZE] = {currentKey}; 
						PageId newPageId[INTARRAYNONLEAFSIZE + 1] = {newID, *currentId};
						
						NonLeafNode newNode = {1, newKeyArray,newPageId};
						Page *&newPage = NULL;
						PageId &newID_1;
						BufMgr->allocPage(file,newID_1,newPage);
						newPage = (NonLeafNode *) &newNode;
						file->writePage(newID_1,newPage);
						height++;
 					}else{
						currentKey = split(currentNode,0,newID,*currentId,currentKey,NULL,child_Id_1,child_Id_2);
						//We reassign the two child nodes for the next time we go through
						chlid_Id_1 = newID;
						child_Id_2 = *currentId;
						//Now we finish with poping the last one off the top.
						depthList.pop();
						depthListID.pop();
					}
				}else{
					int index = findIndex(currentNode->keyArray,INTARRAYNONLEAFSIZE,currentKey);
					moveKeyIndex(currentNode->keyArray,INTARRAYNONLEAFSIZE,index);
					insertMovePage(currentNode->keyArray,currentId,newID,INTARRAYNONLEAFSIZE);
					currentNode->keyArray[index] = currentKey;
					//TODO: Write these to memory
					Page *newPage 
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
