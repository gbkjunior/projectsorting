package Demo;

import java.nio.ByteBuffer;

import PTCFramework.ConsumerIterator;
import StorageManager.Storage;

public class MergeSortProcess{
	
	int iteration;
	byte[] inputBufferOne;
	byte[] inputBufferTwo;
	byte[] lastbuffer = new byte[1024];
	byte[] outputBuffer = new byte[35];
	int startpage;
	int numPages;
	Storage s;
	ConsumerIterator<byte []> consiter = new PutTupleInRelationIterator(35,"myDiskMine");
	
	public MergeSortProcess(int iteration, int startpage, int numPages) throws Exception{
		this.iteration = iteration;
		this.startpage = startpage;
		this.numPages = numPages;
		
		consiter.open();
		
		s = new Storage();
		s.LoadStorage("myDiskMine");
		//System.out.println(s.getLastAllocated());
		long lastindex = s.getLastAllocated();
		System.out.println(lastindex + " - " + startpage);
		
		int currentbufferSize = 29 * 35;
		int iterations = iteration/2;
		int lastiter = iteration%2;
		iterations = iterations + lastiter;
		
		int first = numPages/2;
		int bal = numPages%2;
		
		System.out.println(iterations + " " + lastiter);
		
		if(bal==1){
			byte[] val = new byte[4];
			s.ReadPage(startpage+numPages-1, lastbuffer);
			for(int i=0; i<4; i++){
				val[i] = lastbuffer[i];
			}
			lastbuffer = new byte[ByteBuffer.wrap(val).getInt()*35];
			readPageintoBuffer(startpage+numPages-1,lastbuffer,0);
		}
		
		byte[] tembuffer;
		int iter = 0;
		
		while(iterations > 1){
			iterations--;
			callMerge(numPages,startpage,currentbufferSize);
			numPages = numPages/2;
			startpage = (int) lastindex + 1;
			lastindex = s.getLastAllocated();
			currentbufferSize = currentbufferSize * 2;
			System.out.println("while startpage - " + startpage);
			System.out.println(currentbufferSize);
			System.out.println(lastindex); 
			
		/*	callMerge(numPages,startpage,currentbufferSize);
			first = first/2;
			bal = first%2;
			
			iter++;
			
			if(bal==1){
				tembuffer = new byte[currentbufferSize * 2];
				startpage = (int) (lastindex - iter);
			} */
		}
		
	if(iterations==1 && lastiter==0){
			callMerge(numPages,startpage,currentbufferSize);
		}
		else if(iterations==1 && lastiter==1){
			inputBufferOne = new byte[currentbufferSize];
			inputBufferTwo = lastbuffer;
			int inOffset = 0;
			for(int i=0; i<2; i++){
				inOffset = readPageintoBuffer(startpage, inputBufferOne, inOffset);
				startpage++;
			}
			merge(); 
		}   
	}
	
	public int readPageintoBuffer(int startOffest, byte[] buffer, int bufferOffset) throws Exception{
		byte[] tempbuffer = new byte[1024];
		s.ReadPage(startOffest, tempbuffer);
		
		byte[] bufCount = new byte[4];
		for(int i=0; i<4; i++){
			bufCount[i] = tempbuffer[i];
		}
		
		int count = ByteBuffer.wrap(bufCount).getInt();
		
		System.out.println(count + " - " + bufferOffset);
		count = count * 35 + 8;
		
		System.out.println(count + " - " + bufferOffset);
		
		for(int i=8; i<count; i++){
			buffer[bufferOffset] = tempbuffer[i];
			bufferOffset++;
		}
		
		System.out.println(bufferOffset);
		
		return bufferOffset;
	}
	
	public void callMerge(int numPages, int startpage, int currentBufferSize) throws Exception{
		inputBufferOne = new byte[currentBufferSize];
		inputBufferTwo = new byte[currentBufferSize];
		int inOffSetOne = 0;
		int inOffSetTwo = 0;
		
		System.out.println("inside callMerge " + startpage + " " + numPages);
		
		for(int i=0; i<numPages-1; i=i+2){
			
			while(inOffSetOne<inputBufferOne.length){
			  inOffSetOne =  readPageintoBuffer(startpage,inputBufferOne,inOffSetOne);
			  System.out.println(inOffSetOne);
			  startpage++;
			}
			if(i+1 < numPages)
				while(inOffSetTwo < inputBufferTwo.length){
					inOffSetTwo = readPageintoBuffer(startpage,inputBufferTwo,inOffSetTwo);
					startpage++;
				}
			
			System.out.println(startpage);
			
			inOffSetOne = 0;
			inOffSetTwo = 0;
			
			merge();
		} 
	}
	
	public void merge() throws Exception{
		
		System.out.println("inside merge");
		
		System.out.println(inputBufferOne.length);
		System.out.println(inputBufferTwo.length);
		
		int readBytesOne = 0;
		int readBytesTwo = 0;
		
		byte[] inBuffTupleOne = new byte[35];
		byte[] inBuffTupleTwo = new byte[35];
		
		for(int l=0; l<4; l++){
			inBuffTupleOne[l] = inputBufferOne[readBytesOne+l];
			inBuffTupleTwo[l] = inputBufferTwo[readBytesTwo+l];
		}
		
		while(true){
		//	System.out.println("inTuOne - " + ByteBuffer.wrap(inBuffTupleOne).getInt() + " - " + readBytesOne);
		//	System.out.println("inTuTwo - " + ByteBuffer.wrap(inBuffTupleTwo).getInt() + " - " + readBytesTwo);
		   
			if(ByteBuffer.wrap(inBuffTupleOne).getInt() < ByteBuffer.wrap(inBuffTupleTwo).getInt()){
				for(int l=0; l<35; l++){
					outputBuffer[l] = inputBufferOne[readBytesOne+l];
				}
				readBytesOne = readBytesOne+35;
			}
			else{
				for(int l=0; l<35; l++){
					outputBuffer[l] = inputBufferTwo[readBytesTwo+l];
				}
				readBytesTwo = readBytesTwo+35;
			}
			
			consiter.next(outputBuffer);
			
			if(readBytesOne > inputBufferOne.length-1){
				while(readBytesTwo < inputBufferTwo.length){
					for(int l=0; l<35; l++){
						outputBuffer[l] = inputBufferTwo[readBytesTwo+l];
					}
					readBytesTwo = readBytesTwo+35;
					consiter.next(outputBuffer);
				}
				break;
			}
			
			if(readBytesTwo > inputBufferTwo.length-1){
				while(readBytesOne < inputBufferOne.length){
					for(int l=0; l<35; l++){
						outputBuffer[l] = inputBufferOne[readBytesOne+l];
					}
					readBytesOne = readBytesOne+35;
					consiter.next(outputBuffer);
				}
				break;
			}
			
			for(int l=0; l<4; l++){
				inBuffTupleOne[l] = inputBufferOne[readBytesOne+l];
				inBuffTupleTwo[l] = inputBufferTwo[readBytesTwo+l];
			}
		}
		
	}
	
}