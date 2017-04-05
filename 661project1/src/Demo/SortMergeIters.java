package Demo;

import java.nio.ByteBuffer;

import PTCFramework.ConsumerIterator;
import StorageManager.Storage;

public class SortMergeIters{
	byte[] inputBufferOne = null;
	byte[] inputBufferTwo = null;
	byte[] outputBuffer = null;
	
	int inBuffOneindex;
	int inBuffTwoindex;
	
	int finalindex;
	
	int noOfPageOne;
	int noOfPageTwo;
	
	Storage s;
	ConsumerIterator<byte []> consiter = new PutTupleInRelationIterator(35,"myDiskMine");
	
	public SortMergeIters(int startpage, int numPages) throws Exception{
		//createIters(1024*3,)
		s = new Storage();
        s.LoadStorage("myDiskMine");
        consiter.open();
        
        finalindex = 33;
        createIters(1024*3,22,11);
	}
	
	public void createIters(int pageSize, int startpage, int numPages) throws Exception{
		int noOfpages = pageSize/1024;
		
	/*	int iter = 6;
		
		while(iter <= numPages){
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
			
			noOfpages = noOfpages * 2;
			iter = iter + (numPages % iter);
		} */
		
		for(int i=0; i<numPages; i=i+6){
			inputBufferOne = null;
			inputBufferTwo = null;
			
			callInBuffOne(startpage,noOfpages);
			
			if(i+noOfpages < numPages){
				callInBuffTwo(startpage+noOfpages,noOfpages);
			}
			
			merge();
			startpage = startpage + 2 * noOfpages;
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
				
				for(int l=0; l<4; l++){
					inBuffTupOne[l] = inputBufferOne[readBytesOne+l];
					inBuffTupTwo[l] = inputBufferTwo[readBytesTwo+l];
				}
				
				int keyOne = ByteBuffer.wrap(inBuffTupOne).getInt();
				int keyTwo = ByteBuffer.wrap(inBuffTupTwo).getInt();
				
				if(keyOne < keyTwo){
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
		
		System.out.println("inside CallInBuffTwo - " + inBuffTwoindex + " " + noOfPageTwo);
		
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
		
		System.out.println("inside CallInBuffONe - " + inBuffOneindex + " " + noOfPageOne);
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