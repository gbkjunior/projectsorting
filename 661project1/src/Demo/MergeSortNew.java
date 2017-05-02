package Demo;

import java.nio.ByteBuffer;

import PTCFramework.ConsumerIterator;
import StorageManager.Storage;
import Tuple.Tuple;


public class MergeSortNew{
	
	byte[] runBufferOne = null;
	byte[] runBufferTwo = null;
	byte[] runBufferThree = null;
	
	Tuple t = new Tuple();
	
	Storage s;
	ConsumerIterator<byte []> consiter = new PutTupleInRelationIterator(35,"myDiskMine");
	
	public MergeSortNew(int startpage, int numPages) throws Exception{
        s = new Storage();
        s.LoadStorage("myDiskMine");
        consiter.open();
        
        createRuns(1024,startpage,numPages);
        
        startpage = getLastSortPage(numPages);
        SortMergeIters iter = new SortMergeIters(startpage,numPages,3);

	}
	
	public void createRuns(int pageSize, int startpage, int numPages) throws Exception{
		int noOfPages = pageSize/1024;
		
		for(int i=0; i<numPages; i=i+3){
			runBufferOne = null;
			runBufferTwo = null;
			runBufferThree = null;
			callRunBufferOne(startpage,noOfPages);
			
			if(i+1 < numPages){
				callRunBufferTwo(startpage+noOfPages,noOfPages);
			}
			
			if(i+2 < numPages){
				callRunBufferThree(startpage+noOfPages+noOfPages,noOfPages);
			}
			
			merge();
			startpage = startpage + 3 * noOfPages;
		}
	}
	
	public void callRunBufferOne(int startIndex, int numOfPages) throws Exception{
		runBufferOne = new byte[1024];
		if(numOfPages > 0){
			s.ReadPage(startIndex, runBufferOne);
		}
	}
	
	public void callRunBufferTwo(int startIndex, int numOfPages) throws Exception{
		runBufferTwo = new byte[1024];
		if(numOfPages > 0){
			s.ReadPage(startIndex, runBufferTwo);
		}
	}
	
	public void callRunBufferThree(int startIndex, int numOfPages) throws Exception{
		runBufferThree = new byte[1024];
		if(numOfPages > 0){
			s.ReadPage(startIndex, runBufferThree);
		}
	}
	
	public boolean isRunBufferOne(){
		if(runBufferOne==null){
			return false;
		}
		return true;
	}
	
	public boolean isRunBufferTwo(){
		if(runBufferTwo==null){
			return false;
		}
		return true;
	}
	
	public boolean isRunBufferThree(){
		if(runBufferThree==null){
			return false;
		}
		return true;
	}
	
	public void merge() throws Exception{
		
		if(isRunBufferOne() && isRunBufferTwo() && isRunBufferThree()){
			int readBytesOne = 8;
			int readBytesTwo = 8;
			int readBytesThree = 8;
			
			int bufferOneCount;
			int bufferTwoCount;
			int bufferThreeCount;
			
			byte[] inBuffTupleOne = new byte[4];
			byte[] inBuffTupleTwo = new byte[4];
			byte[] inBuffTupleThree = new byte[4];
			byte[] outputBuffer = new byte[35];
			
			for(int l=0; l<4; l++){
				inBuffTupleOne[l] = runBufferOne[l];
				inBuffTupleTwo[l] = runBufferTwo[l];
				inBuffTupleThree[l] = runBufferThree[l];
			}
			
			bufferOneCount = ByteBuffer.wrap(inBuffTupleOne).getInt();
			bufferTwoCount = ByteBuffer.wrap(inBuffTupleTwo).getInt();
			bufferThreeCount = ByteBuffer.wrap(inBuffTupleThree).getInt();
			
			/*for(int l=0; l<4; l++){
				inBuffTupleOne[l] = runBufferOne[readBytesOne+l];
				inBuffTupleTwo[l] = runBufferTwo[readBytesTwo+l];
				inBuffTupleThree[l] = runBufferThree[readBytesThree+l];
			} */
			
			inBuffTupleOne = new byte[35];
			inBuffTupleTwo = new byte[35];
			inBuffTupleThree = new byte[35];
			
			for(int l=0; l<35; l++){
				inBuffTupleOne[l] = runBufferOne[readBytesOne+l];
				inBuffTupleTwo[l] = runBufferTwo[readBytesTwo+l];
				inBuffTupleThree[l] = runBufferThree[readBytesThree+l];
			} 
			
			while(true){
				/*int keyOne = ByteBuffer.wrap(inBuffTupleOne).getInt();
				int keyTwo = ByteBuffer.wrap(inBuffTupleTwo).getInt();
				int keyThree = ByteBuffer.wrap(inBuffTupleThree).getInt();  */
				
				int keyOnekeyTwo = t.compare(inBuffTupleOne, inBuffTupleTwo);
				int keyOnekeyThree = t.compare(inBuffTupleOne, inBuffTupleThree);
				int keyTwokeyOne = t.compare(inBuffTupleTwo, inBuffTupleOne);
				int keyTwokeyThree = t.compare(inBuffTupleTwo, inBuffTupleThree);
				
				if((keyOnekeyTwo == -1 || keyOnekeyTwo == 0) && (keyOnekeyThree == -1 || keyOnekeyThree == 0)){
					for(int l=0; l<35; l++){
						outputBuffer[l] = runBufferOne[readBytesOne+l];
					}
					writeBuffer(outputBuffer);
					readBytesOne = readBytesOne+35;
				}
				else{
					if((keyTwokeyOne == -1 || keyTwokeyOne == 0) && (keyTwokeyThree == -1 || keyTwokeyThree == 0)){
						for(int l=0; l<35; l++){
							outputBuffer[l] = runBufferTwo[readBytesTwo+l];
						}
						writeBuffer(outputBuffer);
						readBytesTwo = readBytesTwo+35;
					}
					else{
						for(int l=0; l<35; l++){
							outputBuffer[l] = runBufferThree[readBytesThree+l];
						}
						writeBuffer(outputBuffer);
						readBytesThree = readBytesThree+35;
					}
				}
				
				if(readBytesOne >= (bufferOneCount * 35)+8){
					
						mergeTwo(runBufferTwo, runBufferThree, readBytesTwo, readBytesThree);
						break;
				}
				
				if(readBytesTwo >= (bufferTwoCount * 35)+8){
					
						mergeTwo(runBufferOne, runBufferThree, readBytesOne, readBytesThree);
						break;
				}
				
				if(readBytesThree >= (bufferThreeCount * 35)+8){
					
						mergeTwo(runBufferOne, runBufferTwo, readBytesOne, readBytesTwo);
						break;
				}
				
				for(int l=0; l<35; l++){
					inBuffTupleOne[l] = runBufferOne[readBytesOne+l];
					inBuffTupleTwo[l] = runBufferTwo[readBytesTwo+l];
					inBuffTupleThree[l] = runBufferThree[readBytesThree+l];
				}
				
			}
		}
		else if(isRunBufferOne() && isRunBufferTwo()){
			mergeTwo(runBufferOne,runBufferTwo,8,8);
		}
		else if(isRunBufferOne()){
			mergeOne(runBufferOne,8);
		}
	}
	
	public void mergeTwo(byte[] buffOne, byte[] buffTwo, int readBytesOne, int readBytesTwo) throws Exception{
		int bufferCountOne;
		int bufferCountTwo;
		
		byte[] inBuffTupOne = new byte[4];
		byte[] inBuffTupTwo = new byte[4];
		byte[] outputBuffer = new byte[35];
		
		for(int i=0; i<4; i++){
			inBuffTupOne[i] = buffOne[i];
			inBuffTupTwo[i] = buffTwo[i];
		}
		
		bufferCountOne = ByteBuffer.wrap(inBuffTupOne).getInt();
		bufferCountTwo = ByteBuffer.wrap(inBuffTupTwo).getInt();
		
		inBuffTupOne = new byte[35];
		inBuffTupTwo = new byte[35];
		
		while(true){
			if(readBytesOne >= (bufferCountOne * 35)+8){
				mergeOne(buffTwo,readBytesTwo);
				break;
			}
			
			if(readBytesTwo >= (bufferCountTwo * 35)+8){
				mergeOne(buffOne,readBytesOne);
				break;
			}
			
			for(int l=0; l<35; l++){
				inBuffTupOne[l] = buffOne[readBytesOne+l];
				inBuffTupTwo[l] = buffTwo[readBytesTwo+l];
			}
			
			/*int keyOne = ByteBuffer.wrap(inBuffTupOne).getInt();
			int keyTwo = ByteBuffer.wrap(inBuffTupTwo).getInt(); */
			
			int keyOnekeyTwo = t.compare(inBuffTupOne, inBuffTupTwo);
			
			if(keyOnekeyTwo == -1 || keyOnekeyTwo == 0){
				for(int l=0 ; l<35; l++){
					outputBuffer[l] = buffOne[readBytesOne+l];
				}
				writeBuffer(outputBuffer);
				readBytesOne = readBytesOne+35;
			}
			else{
				for(int l=0; l<35; l++){
					outputBuffer[l] = buffTwo[readBytesTwo+l];
				}
				writeBuffer(outputBuffer);
				readBytesTwo = readBytesTwo+35;
			}
		}
	}
	
	public void mergeOne(byte[] buff, int readBytes) throws Exception{
		int bufferCount;
		byte[] inBuffTuple = new byte[4];
		byte[] outputBuffer = new byte[35];
		
		for(int i=0; i<4; i++){
			inBuffTuple[i] = buff[i];
		}
		
		bufferCount = ByteBuffer.wrap(inBuffTuple).getInt();
		
		inBuffTuple = new byte[35];
		
		while(readBytes < (bufferCount * 35)+8){
			for(int l=0; l<35; l++){
				outputBuffer[l] = buff[readBytes+l];
			}
			writeBuffer(outputBuffer);
			readBytes = readBytes+35;
		}
	}
	
	public int getLastSortPage(int numPages){
		int page = 0;
		page = (int) s.getLastAllocated();
		page = page + 1;
		page = page - numPages;
		return page;
	}
	
	public void writeBuffer(byte[] buff) throws Exception{
		consiter.next(buff);
	}
	
}