package Demo;

import java.nio.ByteBuffer;

import PTCFramework.ConsumerIterator;
import StorageManager.Storage;
import Tuple.Tuple;

public class SortMergeIters{
	byte[] inputBufferOne = null;
	byte[] inputBufferTwo = null;
	byte[] outputBuffer = null;
	
	int inBuffOneindex;
	int inBuffTwoindex;
	
	int numBuffer;
	
	int finalindex;
	
	int noOfPageOne;
	int noOfPageTwo;
	
	Tuple t = new Tuple();
	
	Storage s;
	ConsumerIterator<byte []> consiter;
	
	public SortMergeIters(int startpage, int numPages, int numBuffer) throws Exception{
		//createIters(1024*3,)
		System.out.println("Merge Phase");
		s = new Storage();
        s.loadStorage("myDiskMine");
        
        this.numBuffer = numBuffer;
        
        finalindex = startpage + numPages;
        createIters(numBuffer, startpage,numPages);
	}
	
	public void createIters(int noOfpages, int startpage, int numPages) throws Exception{
		
		int iter = noOfpages * 2;
		int count = 1;
		
		while(iter <= numPages + 1){
			consiter = new PutTupleInRelationIterator(35,"myDiskMine");
			consiter.open();
			for(int i=0; i<numPages; i=i+iter){
				inputBufferOne = null;
				inputBufferTwo = null;
				
				callInBuffOne(startpage,noOfpages);
				
				if(i+noOfpages < numPages){
					callInBuffTwo(startpage+noOfpages,noOfpages);
				}
				
				merge();
				startpage = startpage + 2 * noOfpages;
			}
			
			startpage = finalindex;
			noOfpages = noOfpages * 2;
			finalindex = finalindex + numPages;
			iter = iter * 2;
			count++;
			System.out.println("Processing Pass - "+count+" no. of Pages read - "+numPages+" no. of Pages written - "+numPages);
		} 
	}
	
	public boolean isBuffer(byte[] buffer){
		if(buffer==null){
			return false;
		}
		return true;
	}

	public void merge() throws Exception {
		
		if(isBuffer(inputBufferOne) && isBuffer(inputBufferTwo)){
			
			int bufferCountOne;
			int bufferCountTwo;
			
			int readBytesOne = 8;
			int readBytesTwo = 8;
			
			byte[] inBuffTupOne = new byte[4];
			byte[] inBuffTupTwo = new byte[4];
			byte[] outputBuffer = new byte[35];
			
			for(int i=0; i<4; i++){
				inBuffTupOne[i] = inputBufferOne[i];
				inBuffTupTwo[i] = inputBufferTwo[i];
			}
			
			bufferCountOne = ByteBuffer.wrap(inBuffTupOne).getInt();
			bufferCountTwo = ByteBuffer.wrap(inBuffTupTwo).getInt();
			
			inBuffTupOne = new byte[35];
			inBuffTupTwo = new byte[35];
			
			while(true){
				if(readBytesOne >= (bufferCountOne * 35)+8){
					inputBufferOne = null;
					callInBuffOne(inBuffOneindex,noOfPageOne);
					if(isBuffer(inputBufferOne)){
						readBytesOne = 8;
						for(int i=0; i<4; i++){
							inBuffTupOne[i] = inputBufferOne[i];
						}
						bufferCountOne = ByteBuffer.wrap(inBuffTupOne).getInt();
					}
					else{
						mergeOne(inputBufferTwo,readBytesTwo,2);
						break;
					}
				}
				
				if(readBytesTwo >= (bufferCountTwo * 35)+8){
					inputBufferTwo = null;
					callInBuffTwo(inBuffTwoindex,noOfPageTwo);
					if(isBuffer(inputBufferTwo)){
						readBytesTwo = 8;
						for(int i=0; i<4; i++){
							inBuffTupTwo[i] = inputBufferTwo[i];
						}
						bufferCountTwo = ByteBuffer.wrap(inBuffTupTwo).getInt();
					}
					else{
						mergeOne(inputBufferOne,readBytesOne,1);
						break;
					}
				}
				
				for(int l=0; l<35; l++){
					inBuffTupOne[l] = inputBufferOne[readBytesOne+l];
					inBuffTupTwo[l] = inputBufferTwo[readBytesTwo+l];
				}
				
				
				int keyOnekeyTwo = t.compare(inBuffTupOne, inBuffTupTwo);
				
				if(keyOnekeyTwo == -1 || keyOnekeyTwo == 0){
					for(int l=0 ; l<35; l++){
						outputBuffer[l] = inputBufferOne[readBytesOne+l];
					}
					writeBuffer(outputBuffer);
					readBytesOne = readBytesOne+35;
				}
				else{
					for(int l=0; l<35; l++){
						outputBuffer[l] = inputBufferTwo[readBytesTwo+l];
					}
					writeBuffer(outputBuffer);
					readBytesTwo = readBytesTwo+35;
				}
			}
			
		}
		else if(isBuffer(inputBufferOne)){
			mergeOne(inputBufferOne,8,1);
		}
		
	}
	
	public int getLastSortPage(int numPages){
		int page = 0;
		page = (int) s.getLastAllocated();
		page = page + 1;
		page = page - numPages;
		return page;
	}
	
	public void mergeOne(byte[] buff, int readBytes, int item) throws Exception{
		int bufferCount;
		byte[] inBuffTuple = new byte[4];
		byte[] outputBuffer = new byte[35];
		
		for(int i=0; i<4; i++){
			inBuffTuple[i] = buff[i];
		}
		
		bufferCount = ByteBuffer.wrap(inBuffTuple).getInt();
		
		while(true){
			for(int l=0; l<35; l++){
				outputBuffer[l] = buff[readBytes+l];
			}
			writeBuffer(outputBuffer);
			readBytes = readBytes+35;
			
			if(readBytes >= (bufferCount * 35)+8){
				if(item==1){
					inputBufferOne = null;
					callInBuffOne(inBuffOneindex,noOfPageOne);
					if(isBuffer(inputBufferOne)){
						buff = inputBufferOne;
						readBytes = 8;
						for(int i=0; i<4; i++){
							inBuffTuple[i] = buff[i];
						}
						bufferCount = ByteBuffer.wrap(inBuffTuple).getInt();
					}
					else{
						break;
					}
				}
				else if(item==2){
					inputBufferTwo = null;
					callInBuffTwo(inBuffTwoindex,noOfPageTwo);
					if(isBuffer(inputBufferTwo)){
						buff = inputBufferTwo;
						readBytes = 8;
						for(int i=0; i<4; i++){
							inBuffTuple[i] = buff[i];
						}
						bufferCount = ByteBuffer.wrap(inBuffTuple).getInt();
					}
					else{
						break;
					}
				}
			}
		}
	}

	public void writeBuffer(byte[] buff) throws Exception {
		consiter.next(buff);
	}

	public void callInBuffTwo(int i, int noOfpages) throws Exception {
		inputBufferTwo = new byte[1024];
		inBuffTwoindex = i;
		noOfPageTwo = noOfpages;
		
		noOfPageTwo = noOfpages;
		if(noOfPageTwo > 0 && inBuffTwoindex < finalindex){
			   s.ReadPage(inBuffTwoindex,inputBufferTwo );
		}
		else{
			inputBufferTwo = null;
		}
		inBuffTwoindex++;
		noOfPageTwo--;
	}

	public void callInBuffOne(int startpage, int noOfpages) throws Exception {
		inputBufferOne = new byte[1024];
		inBuffOneindex = startpage;
		noOfPageOne = noOfpages;

		if(noOfPageOne > 0 && inBuffOneindex < finalindex){
			    s.ReadPage(inBuffOneindex,inputBufferOne );
		}
		else{
			inputBufferOne = null;
		}
		inBuffOneindex++;
		noOfPageOne--;
		
	}
	
	
	
   
}