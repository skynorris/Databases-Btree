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
#include "exceptions/file_open_exception.h"


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

		//create index name 
		std::ostringstream	idxStr;
		idxStr	<<	relationName	<<	'.'	<<	attrByteOffset;
		std::string	indexName	=	idxStr.str();
		
		//fields && meta data
		bufMgr= bufMgrIn;
		this->attrByteOffset= attrByteOffset;
		attributeType= attrType;
		outIndexName = indexName;
		FileScan scanner(relationName, bufMgrIn);
		Page * headerPagePtr;
		Page * rootPagePtr;
		headerPageNum = 1;
		bool scanExecuting = false;

		
		if(!File::exists(relationName)){
			throw FileNotFoundException("relation file doesnt exist");
		}

		//Occupancy initialization
		switch(attributeType){

			case INTEGER: 
			leafOccupancy = INTARRAYLEAFSIZE;
			nodeOccupancy = INTARRAYNONLEAFSIZE;
			break;
			case DOUBLE: 
			leafOccupancy = DOUBLEARRAYLEAFSIZE;
			nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
			break;
			case STRING: 
			leafOccupancy = STRINGARRAYLEAFSIZE;
			nodeOccupancy = STRINGARRAYNONLEAFSIZE;
			break;		
		}


		// --------- INDEX FILE CREATE/OPEN -------------
		//If Index File is already on disc
		//File just repersents an object that lets us communicate with disc
		try{
			file = new BlobFile(indexName, false);
			bufMgr->readPage(file, headerPageNum, headerPagePtr);

			
			//get a info, store it all in metaPtr
			IndexMetaInfo * metaPtr = (IndexMetaInfo*) headerPagePtr;
			
			// //checks
			// if(strncmp((const char*)&(metaPtr->relationName), (const char*)&relationName, 20) != 0){
			// 	throw BadIndexInfoException("Bad info");
			// }
			// if(metaPtr->attrByteOffset != attrByteOffset){
			// 	throw BadIndexInfoException("Bad info");
			// }
			// if(metaPtr->attrType != attrType){
			// 	throw BadIndexInfoException("Bad info");
			// }



			///Checks that the file name given and the other param match
			rootPageNum = ((IndexMetaInfo*)headerPagePtr)->rootPageNo;
			bufMgr->unPinPage(file, headerPageNum, false);


		//If Index does not already exist
		}catch(FileNotFoundException e){

			file = new BlobFile(outIndexName, true);


			//create meta page
			bufMgr->allocPage(file, headerPageNum, headerPagePtr);

			//create root page
			bufMgr->allocPage(file, rootPageNum, rootPagePtr);


			//fill meta info 
			struct IndexMetaInfo metaInfo;
			strncpy((char*)&(metaInfo.relationName),(const char*)&relationName, STRINGSIZE);
			metaInfo.attrByteOffset = attrByteOffset;
			metaInfo.attrType = attrType;
			metaInfo.rootPageNo = rootPageNum;
			metaInfo.rootLeaf = true;
			memcpy(headerPagePtr, &metaInfo, sizeof(IndexMetaInfo));

			//probably a better way to do this but initialize root node
			switch(attributeType){

				case INTEGER: 
				((LeafNodeInt*)(rootPagePtr))->slot = 0;
				break;

				case DOUBLE: 	
				((LeafNodeDouble*)(rootPagePtr))->slot = 0;
				break;

				case STRING: 
				((LeafNodeString*)(rootPagePtr))->slot = 0;
				break;		
			}

			//fill index file
			try{

				RecordId scanRid;

				while(1)
				{
					scanner.scanNext(scanRid);
					std::string recordStr = scanner.getRecord();
					const char *record = recordStr.c_str();

				//keys
					int intkey;
					double doublekey;
					const char * charkey;
					std::string stringkey;

					switch(attributeType){

						case INTEGER: 
						intkey = *((int *)(record + attrByteOffset));
						insertEntry(&intkey, scanRid);
						break;

						case DOUBLE: 
						doublekey = *((double *)(record + attrByteOffset));
						insertEntry(&doublekey, scanRid);
						break;

						case STRING: 
						stringkey  = recordStr.substr(attrByteOffset, STRINGSIZE);	
						charkey = stringkey.c_str();
						insertEntry(&charkey, scanRid);
						break;		
					}

				}
			}
			catch(EndOfFileException e){

				//can we unpin before try???
				bufMgr->unPinPage(file, headerPageNum, true);
				bufMgr->unPinPage(file, rootPageNum, true);
			}

		}
	}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
//	** DOES NOT DELETE INDEX FILE, FILE IS STILL ON DISC
//	IT just deletes object associated with it = closes file!
// -----------------------------------------------------------------------------

	BTreeIndex::~BTreeIndex()
	{

		bufMgr->flushFile(file);
	///Deletes the file ptr to invoke blobsfile's destructor
		delete file;


	}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------


	const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
	{

		Page* headerPagePtr;
		bufMgr->readPage(file, headerPageNum, headerPagePtr);
		IndexMetaInfo * metaPtr = (IndexMetaInfo*) headerPagePtr;
		bool rootLeaf = metaPtr->rootLeaf;
		bufMgr->unPinPage(file, headerPageNum, false);

		//root is leaf
		if(rootLeaf){

			switch(attributeType){
				case INTEGER:	insertLeaf<int, struct LeafNodeInt, struct NonLeafNodeInt>(rootPageNum,  (void*)key, rid);
				break;
				break;
				case DOUBLE:	insertLeaf<double, struct LeafNodeDouble, struct NonLeafNodeDouble>(rootPageNum,  (void*)key, rid);
				break;
				case STRING:	insertLeaf<std::string, struct LeafNodeString, struct NonLeafNodeString>(rootPageNum,  (void*)key, rid);

			}


		}else{

			switch(attributeType){
				case INTEGER:	{

					RIDKeyPair<int>* intpair = new RIDKeyPair<int>();
					intpair->set(rid, *((int*)key));
					PageId leafNum = traversal<int, struct LeafNodeInt, struct NonLeafNodeInt>(rootPageNum, (RIDKeyPair<int>*) intpair);
					insertLeaf<int, struct LeafNodeInt, struct NonLeafNodeInt>(leafNum, (void*)key, rid);

				}
				break;
				case DOUBLE:	{

					RIDKeyPair<double>* doublepair = new RIDKeyPair<double>();
					doublepair->set(rid, *((double*)key));
					PageId leafNum = traversal<double, struct LeafNodeDouble, struct NonLeafNodeDouble>(rootPageNum, (RIDKeyPair<double>*) doublepair);
					insertLeaf<double, struct LeafNodeDouble, struct NonLeafNodeDouble>(leafNum, (void*)key, rid);

				}


				break;
				case STRING:	
				{

					RIDKeyPair<std::string>* stringpair = new RIDKeyPair<std::string>();
					stringpair->set(rid, *((std::string*)key));
					PageId leafNum = traversal<std::string, struct LeafNodeString, struct NonLeafNodeString>(rootPageNum, (RIDKeyPair<std::string>*) stringpair);
					insertLeaf<std::string, struct LeafNodeString, struct NonLeafNodeString>(leafNum, (void*)key, rid);
					break;
				}

			}

		}
	}




	template<class T, class leaf, class node>
	const PageId BTreeIndex::traversal(PageId &root, RIDKeyPair<T>* rid_pair){

			//get root info
		Page* rootPagePtr;
		bufMgr->readPage(file, rootPageNum, rootPagePtr);
		node* rootptr = (node*) rootPagePtr;
		int level = rootptr->level;
		T key = rid_pair->key;


		while(level >= 1){

			PageId childPageNum;
			Page* childPage;
			bool found = false;
			RecordId childRid;
			for(int i = 0; i <= rootptr->slot; i++){
				if(rid_pair->key < rootptr->keyArray[i]){
					childPageNum = rootptr->pageNoArray[i];
					found = true;
					i = rootptr->slot +1;
				}

			}

			if(!found){
				childPageNum = childPageNum = rootptr->pageNoArray[rootptr->slot];
			}


			bufMgr->unPinPage(file, root, false);
			bufMgr->readPage(file, childPageNum, childPage);

			if(level ==1){
				node* childnode = reinterpret_cast<node*> (childPage);
				for(int i =0; i<childnode->slot; i++){
					if(rid_pair->key < childnode->keyArray[i]){
						return childnode->pageNoArray[i];
					}
				}

			}else{

				return traversal<T, leaf, node>(childPageNum, rid_pair);
			}


		}

		//error
		const PageId error = -1;
		return error;

	}


	const void BTreeIndex::insertLeafData(Page* current, void* key, const RecordId &rid){


		switch(attributeType){
			case INTEGER:	{

				LeafNodeInt* curr = (LeafNodeInt*) current;
				curr->keyArray[curr->slot] = *((int*) key);
				curr->ridArray[curr->slot] = rid;

			}
			break;
			case DOUBLE:	{

				LeafNodeDouble* curr = (LeafNodeDouble*) current;
				curr->keyArray[curr->slot] = *((double*) key);
				curr->ridArray[curr->slot] = rid;

			}
			break;
			case STRING:	
			{
				LeafNodeString* curr = (LeafNodeString*) current;
				strncpy(curr->keyArray[curr->slot], (char*)key, 10);
				curr->ridArray[curr->slot] = rid;
			}
			break;

		}

	}

	const void BTreeIndex::insertNodeData(Page* current, void* key, const PageId &pagenum){


		switch(attributeType){
			case INTEGER:	{

				NonLeafNodeInt* curr = (NonLeafNodeInt*) current;
				curr->keyArray[curr->slot] = *((int*) key);
				curr->pageNoArray[curr->slot] = pagenum;

			}
			break;
			case DOUBLE:	{

				NonLeafNodeDouble* curr = (NonLeafNodeDouble*) current;
				curr->keyArray[curr->slot] = *((double*) key);
				curr->pageNoArray[curr->slot] = pagenum;

			}
			break;
			case STRING:	
			{
				NonLeafNodeString* curr = (NonLeafNodeString*) current;
				strncpy(curr->keyArray[curr->slot], (char*)key, 10);
				curr->pageNoArray[curr->slot] = pagenum;
			}
			break;

		}

	}





	template<class T, class leaf, class node>
	const void BTreeIndex::insertLeaf(PageId &target, void *key, const RecordId &rid){
		Page* curr;
		bufMgr->readPage(file, target, curr);
		leaf* targetNode =  reinterpret_cast<leaf*> (curr);

		//there is room
		if(targetNode->slot < leafOccupancy){
			insertLeafData(curr, key, rid);
			targetNode->slot = targetNode->slot+1;
			bufMgr->unPinPage(file, target, true);

			//no room
		}else{

			bufMgr->unPinPage(file, target, false);
			splitLeaf<T, leaf, node>( target, key, rid);


		}
	}

	template<class T, class leaf, class node>
	const void BTreeIndex::splitNon(PageId &firstID, PageId &newAlloc_pageId){

		//read and allocate pages needed to splitt
		Page* first_page;
		bufMgr->readPage(file, firstID, first_page);
		node* firstNode = reinterpret_cast<node*>(first_page);

		Page* parentptr;
		PageId parentID;
		bufMgr->allocPage(file, parentID, parentptr);
		node* parentNode = reinterpret_cast<node*>(parentptr);


		Page* secondptr;
		PageId secondID;
		bufMgr->allocPage(file, secondID, secondptr);
		node* secondNode = reinterpret_cast<node*>(secondptr);


		int start = nodeOccupancy/2;
		secondNode->slot = 0;

		//mcopy data into second node
		for(int i = start; i < firstNode->slot; i++){
			insertNodeData(secondptr, (void*)&(firstNode->keyArray[i]), (firstNode->pageNoArray[i]));
			secondNode->slot = secondNode->slot + 1;
		}

		firstNode->slot = start;


		//put first and second into parent
		parentNode->slot = 0;
		parentNode->pageNoArray[0] = firstID;
		insertNodeData(parentptr, (void*)&(secondNode->keyArray[0]), secondNode->pageNoArray[0]);

		//update level and unpin
		parentNode->level = firstNode->level +1;
		bufMgr->unPinPage(file, parentID, true);

		//figure out whhich node to insert target to
		Page* entry_page;
		bufMgr->readPage(file, newAlloc_pageId, entry_page);
		node* entry_nonleafPage = reinterpret_cast<node*> (entry_page);
		node* target;

		if(entry_nonleafPage->keyArray[0] >= secondNode->keyArray[0] ){
			target = secondNode;
		}
		else{
			target = firstNode;
		}

		target->slot = target->slot +1;
		int inserter = 0;
		T newKeyArray[sizeof(nodeOccupancy)];
		PageId newPageNoArray[nodeOccupancy];

		for(int i = 0; inserter <target->slot; inserter++){
		//insert pageid to the right side and insert key

			if(inserter == 0){
				if(entry_nonleafPage->keyArray[0] <= target->keyArray[0]){
					newPageNoArray[0] = newAlloc_pageId;
				}else{
					newPageNoArray[0] = target->pageNoArray[0];
				}
			}

			if(entry_nonleafPage->keyArray[0] > target->keyArray[inserter]){
				newKeyArray[inserter+i] = target->keyArray[inserter];
				newPageNoArray[inserter+1+i] = target->pageNoArray[inserter + 1];
			}
			else{
				newKeyArray[inserter+i] = entry_nonleafPage->keyArray[0];
				newPageNoArray[inserter+1+i] = newAlloc_pageId;
				i++;
			}

		}

		memcpy(target->keyArray, newKeyArray, target->slot);
		memcpy(target->pageNoArray, newPageNoArray, target->slot);
		bufMgr->unPinPage(file, firstID, true);
		bufMgr->unPinPage(file, secondID, true);
		bufMgr->unPinPage(file, newAlloc_pageId, true);

		//find the non leaf of that level and insert it
		insertNonLeaf<T, leaf, node>(parentID, parentNode->level);
	}


	template<class T, class leaf, class node>
	const void BTreeIndex::insertNonLeaf(PageId &nodeID, int &level){

		Page* rootptr;
		bufMgr->readPage(file, rootPageNum, rootptr);
		node* root = reinterpret_cast<node*> (rootptr);

		//if we just split the root in method call uodate info, and were good
		if(level >= root->level){

			Page* metaPage;
			bufMgr->readPage(file, headerPageNum, metaPage);
			IndexMetaInfo* metaPtr = (IndexMetaInfo*)metaPage;
			metaPtr->rootPageNo = nodeID;
			rootPageNum = nodeID;
			bufMgr->unPinPage(file, headerPageNum, true);
			bufMgr->unPinPage(file, rootPageNum, false);
			return;

		}

		Page* current_page;
		bufMgr->readPage(file, nodeID, current_page);
		node* curr = reinterpret_cast<node*> (current_page);
		void* key = reinterpret_cast<void*> (&curr->keyArray[0]);

		PageId parentID = parentSearch<T, leaf, node>(rootPageNum, key, level);
		Page* parentPtr;
		bufMgr->readPage(file, parentID, parentPtr);
		node* target = reinterpret_cast<node*> (parentPtr);

		int i = target->slot;
		if(i < nodeOccupancy){

			//need to copy pageid and key
			target->pageNoArray[i] = curr->pageNoArray[0];

			for(int j=0; j< curr->slot; j++){
				insertNodeData(current_page,
					(void*)&(curr->keyArray[j]), curr->pageNoArray[j+1] );
				curr->slot = curr->slot +1;
			}

			bufMgr->unPinPage(file, nodeID, false);
			bufMgr->unPinPage(file, parentID, true);

			//since we over wrote page have to delete
			file->deletePage(nodeID);


		}
		//we have to split
		else{

			bufMgr->unPinPage(file, nodeID, false);
			bufMgr->unPinPage(file, parentID, false);
			splitNon<T, leaf, node>(parentID, nodeID);
		}
	}





	template<class T, class leaf, class node>
	const PageId BTreeIndex::parentSearch(PageId &root_id, void* key, int &target_level){

		Page* meta_page;
		bufMgr->readPage(file, headerPageNum, meta_page);
		IndexMetaInfo* metapage = (IndexMetaInfo*) meta_page;
		if(metapage->rootLeaf){
			bufMgr->unPinPage(file, headerPageNum, false);
			return rootPageNum;
		}

		Page* root_page;
		bufMgr->readPage(file, root_id, root_page);
		node* root = reinterpret_cast<node*> (root_page);

		T obj_key = *((T*)key);
		int level = root->level;

		while(level >= target_level){

			int i;
			while(i <target_level ){
				if(obj_key >= root->keyArray[i]){
					i++;
				}
				else{
					break;
				}

			}

			PageId childNum = root->pageNoArray[i];
			bufMgr->unPinPage(file, root_id, false);

			if(level == target_level){
				return childNum;
			}else{

				return parentSearch<T, leaf, node>(childNum, key, target_level);

			}
		}

		PageId error = -1;
		return error;

	}




	template<class T, class leaf, class node>
	const void BTreeIndex::splitLeaf(PageId &leafNum, void *key, const RecordId rid){

		Page* curr;
		bufMgr->readPage(file, leafNum, curr);
		leaf* orgLeaf = reinterpret_cast<leaf*> (curr);

		//make new page for split
		Page* newLeaf;
		PageId newLeafPageNum;
		bufMgr->allocPage(file, newLeafPageNum, newLeaf);
		leaf* newLeafNode = reinterpret_cast<leaf*> (newLeaf);
		newLeafNode->slot =0;


		//where to start copying from
		int startCopy =  leafOccupancy/2;

		//memcopy
		for(int i = startCopy; i < orgLeaf->slot; i++){
			insertLeafData(newLeaf, (void*)&(orgLeaf->keyArray[i]), (orgLeaf->ridArray[i]));
			newLeafNode->slot = newLeafNode->slot + 1;
		}

		orgLeaf->slot = startCopy;

		//whcih node to insert upon?
		if(*((T*)key) < newLeafNode->keyArray[0]){
			insertLeafData(curr, key, rid);
			orgLeaf->slot++;
		}
		else{
			insertLeafData(newLeaf, key, rid);	
			newLeafNode->slot++;

		}

		//connect pointers
		newLeafNode->rightSibPageNo = orgLeaf->rightSibPageNo;
		orgLeaf->rightSibPageNo = newLeafPageNum;

		//create new non leaf node to  point to new leaf node
		PageId nonleaf_pageid;
		Page* nonleaf_page;
		bufMgr->allocPage(file, nonleaf_pageid, nonleaf_page);
		node* first_nonleafPage = reinterpret_cast<node*> (nonleaf_page);
		first_nonleafPage->level = 1;
		first_nonleafPage->slot = 0;

		insertNodeData(nonleaf_page, (void*)&(newLeafNode->keyArray[0]), newLeafPageNum );
		first_nonleafPage->slot = first_nonleafPage->slot + 1;

		//unpin leafs
		bufMgr->unPinPage(file, newLeafPageNum, true);
		bufMgr->unPinPage(file, leafNum, true);

		//meta
		Page* metapage;
		bufMgr->readPage(file, headerPageNum, metapage);
		IndexMetaInfo* metaPtr = (IndexMetaInfo*) metapage;

		//check if root was a leaf 
		if(metaPtr->rootLeaf){
			metaPtr->rootLeaf = false;
			rootPageNum = nonleaf_pageid;
			metaPtr->rootPageNo = nonleaf_pageid;
			bufMgr->unPinPage(file, headerPageNum, true);
			first_nonleafPage->pageNoArray[0] = leafNum;
			bufMgr->unPinPage(file, nonleaf_pageid, true);


		}else{
			bufMgr->unPinPage(file, headerPageNum, false);
			bufMgr->unPinPage(file, nonleaf_pageid, true);

			insertNonLeaf<T, leaf, node>(nonleaf_pageid, first_nonleafPage->level);
		}

	}



// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

	const void BTreeIndex::startScan(const void* lowValParm,
		const Operator lowOpParm,
		const void* highValParm,
		const Operator highOpParm)
	{

					scanExecuting = true;
			lowOp = lowOpParm;
			highOp = highOpParm;

		
		if((lowOp!=LT && lowOp!=LTE && lowOp!=GTE && lowOp!=GT) ||(highOp!=LT && highOp!=LTE && highOp!=GTE && highOp!=GT)){
	
			throw BadOpcodesException();	
		}	

		Page* meta_page;
		bufMgr->readPage(file, headerPageNum, meta_page);
		IndexMetaInfo* metaptr = (IndexMetaInfo*) meta_page;


		currentPageNum = headerPageNum;
		bufMgr->readPage(file, currentPageNum, currentPageData);


		switch(attributeType){

			case INTEGER:{

			//cast 
				lowValInt = *((int*)lowValParm);
				highValInt = *((int*) highValParm);
				NonLeafNodeInt *itr = (NonLeafNodeInt *)currentPageData;
				
				while(itr->level != 1){

					int i = 0;

					while(itr->keyArray[i] < lowValInt && i<nodeOccupancy && itr->pageNoArray[i+1] !=0){
						i++;
					}

					PageId nextNum = itr->pageNoArray[i];

				//update itr
					bufMgr->readPage(file, nextNum, currentPageData);
					bufMgr->unPinPage(file, currentPageNum, false);
					currentPageNum = nextNum;

					itr = (NonLeafNodeInt*) currentPageData;


				}

			//pointint to leafs
				int j = 0;
				while(itr->keyArray[j] > lowValInt && itr->pageNoArray[j + 1] != 0 && j < nodeOccupancy){
					j++;
				}

				PageId nextNum = itr->pageNoArray[j];

			//update itr
				bufMgr->readPage(file, nextNum, currentPageData);
				bufMgr->unPinPage(file, currentPageNum, false);
				currentPageNum = nextNum;

				itr = (NonLeafNodeInt*) currentPageData;

			//set up scan
				nextEntry = 0;

			}
			break;
			case DOUBLE:{

			//cast 
				lowValDouble = *((double*)lowValParm);
				highValDouble = *((double*) highValParm);
				NonLeafNodeDouble *itr = (NonLeafNodeDouble *)currentPageData;
				

				while(itr->level != 1){

					int i = 0;

					while(itr->keyArray[i] < lowValDouble && i<nodeOccupancy && itr->pageNoArray[i+1] !=0){
						i++;
					}

					PageId nextNum = itr->pageNoArray[i];

				//update itr
					bufMgr->readPage(file, nextNum, currentPageData);
					bufMgr->unPinPage(file, currentPageNum, false);
					currentPageNum = nextNum;

					itr = (NonLeafNodeDouble*) currentPageData;


				}

			//pointint to leafs
				int j = 0;
				while(itr->keyArray[j] > lowValDouble && itr->pageNoArray[j + 1] != 0 && j < nodeOccupancy){
					j++;
				}

				PageId nextNum = itr->pageNoArray[j];

			//update itr
				bufMgr->readPage(file, nextNum, currentPageData);
				bufMgr->unPinPage(file, currentPageNum, false);
				currentPageNum = nextNum;

				itr = (NonLeafNodeDouble*) currentPageData;

			//set up scan
				nextEntry = 0;

			}
			break;
			case STRING:{
			//cast 
				lowValString =  std::string((char *)lowValParm).substr(0,STRINGSIZE);;
				highValString = std::string((char *) highValParm).substr(0,STRINGSIZE);;
				NonLeafNodeString *itr = (NonLeafNodeString*)currentPageData;
				
				while(itr->level != 1){

					int i = 0;

					while(itr->keyArray[i] < lowValString && i<nodeOccupancy && itr->pageNoArray[i+1] !=0){
						i++;
					}

					PageId nextNum = itr->pageNoArray[i];

				//update itr
					bufMgr->readPage(file, nextNum, currentPageData);
					bufMgr->unPinPage(file, currentPageNum, false);
					currentPageNum = nextNum;

					itr = (NonLeafNodeString*) currentPageData;


				}

			//pointint to leafs
				int j = 0;
				while(itr->keyArray[j] > lowValString && itr->pageNoArray[j + 1] != 0 && j < nodeOccupancy){
					j++;
				}

				PageId nextNum = itr->pageNoArray[j];

			//update itr
				bufMgr->readPage(file, nextNum, currentPageData);
				bufMgr->unPinPage(file, currentPageNum, false);
				currentPageNum = nextNum;

				itr = (NonLeafNodeString*) currentPageData;

			//set up scan
				nextEntry = 0;

			}
			break;		
		}

	}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

	const void BTreeIndex::scanNext(RecordId& outRid) 
	{

		if(scanExecuting == false){
			throw ScanNotInitializedException();
		}
		bufMgr->readPage(file, currentPageNum, currentPageData);


		switch(attributeType){
			case INTEGER:	{

				bool done = false;

				while(!done){

					LeafNodeInt *curr = (LeafNodeInt *)currentPageData;

					//new page
					if( (nextEntry >= nodeOccupancy) || (curr->ridArray[nextEntry].page_number == 0) ){

						//update curr
						PageId nextNum = curr->rightSibPageNo;

						//scan complete
						if(nextNum ==0){
							throw IndexScanCompletedException();
						}else{
							currentPageNum = nextNum;

						}

						bufMgr->unPinPage(file, currentPageNum, false);
						bufMgr->readPage(file, currentPageNum, currentPageData);

						//new page
						nextEntry = 0;

					}

					else if(lowOp == GT || lowOp == GTE){
						if(lowOp == GT){
							if(curr->keyArray[nextEntry] > lowValInt){
								nextEntry++;
							}

						}else{
							if(curr->keyArray[nextEntry] <= lowValInt){
								nextEntry++;
							}
						}
					}

					else if(highOp == LT || highOp == LTE){
						if(highOp == LT){
							if(curr->keyArray[nextEntry] < highValInt){
								throw IndexScanCompletedException();
							}
						}else{
							if(curr->keyArray[nextEntry] <= highValInt){
								throw IndexScanCompletedException();
							}
						}
					}

					else{

						outRid = curr->ridArray[nextEntry];
						nextEntry++;
						done = true;
					}

				}
			}
			break;
			case DOUBLE:	{

				bool done = false;

				while(!done){

					LeafNodeDouble *curr = (LeafNodeDouble *)currentPageData;

					//new page
					if( (nextEntry >= nodeOccupancy) || (curr->ridArray[nextEntry].page_number == 0) ){

						//update curr
						PageId nextNum = curr->rightSibPageNo;

						//scan complete
						if(nextNum ==0){
							throw IndexScanCompletedException();
						}else{
							currentPageNum = nextNum;

						}

						bufMgr->unPinPage(file, currentPageNum, false);
						bufMgr->readPage(file, currentPageNum, currentPageData);

						//new page
						nextEntry = 0;

					}

					else if(lowOp == GT || lowOp == GTE){
						if(lowOp == GT){
							if(curr->keyArray[nextEntry] > lowValDouble){
								nextEntry++;
							}

						}else{
							if(curr->keyArray[nextEntry] <= lowValDouble){
								nextEntry++;
							}
						}
					}

					else if(highOp == LT || highOp == LTE){
						if(highOp == LT){
							if(curr->keyArray[nextEntry] < highValDouble){
								throw IndexScanCompletedException();
							}
						}else{
							if(curr->keyArray[nextEntry] <= highValDouble){
								throw IndexScanCompletedException();
							}
						}
					}

					else{

						outRid = curr->ridArray[nextEntry];
						nextEntry++;
						done = true;
					}

				}

				

			}
			break;
			case STRING:	
			{

				bool done = false;

				while(!done){

					LeafNodeString *curr = (LeafNodeString *)currentPageData;

					//new page
					if( (nextEntry >= nodeOccupancy) || (curr->ridArray[nextEntry].page_number == 0) ){

						//update curr
						PageId nextNum = curr->rightSibPageNo;

						//scan complete
						if(nextNum ==0){
							throw IndexScanCompletedException();
						}else{
							currentPageNum = nextNum;

						}

						bufMgr->unPinPage(file, currentPageNum, false);
						bufMgr->readPage(file, currentPageNum, currentPageData);

						//new page
						nextEntry = 0;

					}

					else if(lowOp == GT || lowOp == GTE){
						if(lowOp == GT){
							if(curr->keyArray[nextEntry] > lowValString){
								nextEntry++;
							}

						}else{
							if(curr->keyArray[nextEntry] <= lowValString){
								nextEntry++;
							}
						}
					}

					else if(highOp == LT || highOp == LTE){
						if(highOp == LT){
							if(curr->keyArray[nextEntry] < highValString){
								throw IndexScanCompletedException();
							}
						}else{
							if(curr->keyArray[nextEntry] <= highValString){
								throw IndexScanCompletedException();
							}
						}
					}

					else{

						outRid = curr->ridArray[nextEntry];
						nextEntry++;
						done = true;
					}

				}

				
			}
			break;
		}


	}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
	const void BTreeIndex::endScan() 
	{
		if(scanExecuting == false){
			throw ScanNotInitializedException();
		}
		scanExecuting = false;
		if(currentPageNum != 0){
			bufMgr->unPinPage(file, currentPageNum, false);
		}

	}

}
