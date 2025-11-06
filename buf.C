#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{
	Status status = OK;
	int i = 0;
	while (i < numBufs) {
		BufDesc currFrame = bufTable[clockHand];
		bool pinned = currFrame.pinCnt > 0;
		if (!currFrame.valid) {
			frame = currFrame.frameNo;			
			return status;
		}

		if (currFrame.refbit || currFrame.pinned) {
			currFrame.refbit = false;	
			advanceClock();
			i++;
			continue;
		}

		//must be valid, not pinned, refbit not set
		if (currFrame.dirty) {
			//write to disk
			status = currFrame.file->writePage(currFrame.pageNo, &bufPool[currFrame.frameNo]);
			currFrame.dirty = false;
			hashTable->remove(currFrame.file, currFrame.pageNo);
		} 
		frame = currFrame.frameNo;			
		return status;

		//advanceClock();
		//i++;
	}

	//only pinned frames
	status = BUFFEREXCEEDED;
	return status;
}

/*
 * Attempts to read a page into page
 * If the page is already in buffer, assign the frame in buffer to page
 * Otherwise, read the page into buffer and then assign allocated frame to page
 *
 * Input are the file and the page number that we want to read in
 * The frame containing the page will be set to page upon successful read
 */ 
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
	Status status = OK;
	int frameNo = 0;
	status = hashTable->lookup(file, PageNo, frameNo);
	// Case 1: Page is not in the buffer pool.	
	if (status == HASHNOTFOUND){
		status = allocBuf(frameNo); 
		// Error occured in allocBuf
		if (status != OK){
		       return status;
		}else{
		       // read page on file into buffer
		       status = file->readPage(PageNo, &bufPool[frameNo]);
		       if (status != OK) {
			       return status;
		       }
		       status = hashTable->insert(file, PageNo, frameNo);	
		       // Error occured in inserting to hashtable
		       if (status != OK){
			       return status;
		       }
		       bufTable[frameNo].Set(file, PageNo);
		       page = &bufPool[frameNo];
		       return OK;
		}		
	}
	// Case 2: Page is in the buffer pool
	else if (status == OK){
		bufTable[frameNo].refbit = true;
		bufTable[frameNo].pinCnt += 1;
		page = &bufPool[frameNo];
	}	
	return status;
}

/* 
 * Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, sets the dirty bit.  
 * Returns OK if no errors occurred, HASHNOTFOUND if the page is not in the
 * buffer pool hash table, PAGENOTPINNED if the pin count is already 0.
*/

const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) 
{
	Status status = OK;
	int frameNo = 0;
	status = hashTable->lookup(file, PageNo, frameNo);
	if (status == OK){
		int pinCount = bufTable[frameNo].pinCnt;
		if (pinCount == 0){
			return PAGENOTPINNED;
		}
		else{
			bufTable[frameNo].pinCnt -= 1;
			if(dirty) bufTable[frameNo].dirty = true;
		}
	}
	// Return the appropriate status depending on hash lookup 
	return status;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{







}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


