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
		
		boolean haslastbuffer = false;
		
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
		
		System.out.println(first + " " + bal);
		
		if(bal==1){
			byte[] val = new byte[4];
			s.ReadPage(startpage+numPages-1, lastbuffer);
			for(int i=0; i<4; i++){
				val[i] = lastbuffer[i];
			}
			lastbuffer = new byte[ByteBuffer.wrap(val).getInt()*35];
			readPageintoBuffer(startpage+numPages-1,lastbuffer,0);
			haslastbuffer = true;
		}
		
		byte[] tembuffer;
		int iter = 0;
		
		while(first!=0){
			/*iterations--;
			callMerge(numPages,startpage,currentbufferSize);
			numPages = numPages/2;
			startpage = (int) lastindex + 1;
			lastindex = s.getLastAllocated();
			currentbufferSize = currentbufferSize * 2;
			System.out.println("while startpage - " + startpage);
			System.out.println(currentbufferSize);
			System.out.println(lastindex);  */
			
		//	System.out.println(first + " " + bal);
			
			callMerge(first,startpage,currentbufferSize);
			currentbufferSize = currentbufferSize * 2;
			startpage = (int) lastindex + 1;
			lastindex = s.getLastAllocated();
			
			System.out.println(startpage + " - " + lastindex);
			bal = first%2;
			first = first/2;
			
			if(first == 0){
				break;
			}
			
			
			System.out.println(first + " - " + bal);
			
			iter++;
			
			if(bal==1 && haslastbuffer == true){
				System.out.println("inside me first case " );
				tembuffer = new byte[currentbufferSize];
				int inOff = 0;
				int pos = (int)lastindex + 1 - (iter*2);
				
				System.out.println(pos);
				
				while(inOff < tembuffer.length){
					inOff = readPageintoBuffer(pos,tembuffer,inOff);
					pos++;
				}
				
				int finalSize = tembuffer.length + lastbuffer.length;
				inputBufferOne = tembuffer;
				inputBufferTwo = lastbuffer;
				
				merge();
				
				pos = (int)lastindex + 1;
				
				
				lastbuffer = new byte[finalSize];
				inOff = 0;
				
				if(inOff == 0){
				System.out.println("printme - "+ pos + " " + lastbuffer.length);
				while(inOff < lastbuffer.length){
					inOff = readPageintoBuffer(pos,lastbuffer,inOff);
					pos++;
				}
				break;
				}
				
				
				
				haslastbuffer = true;
				
			} 
			else if(bal==1 && haslastbuffer == false){
				lastbuffer = new byte[currentbufferSize];
				int pos = (int) lastindex + 1 - (iter*2);
				int inOff = 0;
				
				while(inOff < lastbuffer.length){
					inOff = readPageintoBuffer(pos,lastbuffer,inOff);
					pos++;
				}
				haslastbuffer = true;
			}
		}
		
	if(first==0 && haslastbuffer==true){
		System.out.println("inside first==0 loop " + currentbufferSize);
			inputBufferOne = new byte[currentbufferSize];
			inputBufferTwo = lastbuffer;
			int inOffset = 0;
			while(inOffset < inputBufferOne.length){
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
		
		//System.out.println(count + " - " + bufferOffset);
		count = count * 35 + 8;
		
		//System.out.println(count + " - " + bufferOffset);
		
		for(int i=8; i<count; i++){
			buffer[bufferOffset] = tempbuffer[i];
			bufferOffset++;
		}
		
		System.out.println("inside read page - " + bufferOffset);
		
		return bufferOffset;
	}
	
	public int getLastSortPage(){
		int page = 0;
		page = (int) s.getLastAllocated();
		page = page + 1;
		page = page - numPages;
		return page;
	}
	
	public void callMerge(int numPages, int startpage, int currentBufferSize) throws Exception{
		inputBufferOne = new byte[currentBufferSize];
		inputBufferTwo = new byte[currentBufferSize];
		int inOffSetOne = 0;
		int inOffSetTwo = 0;
		
		System.out.println("inside callMerge " + startpage + " " + numPages);
		
		for(int i=0; i<numPages; i++){
			
			System.out.println("inside callMerge for loop " + startpage + " " + numPages + " " + i);
			
			while(inOffSetOne<inputBufferOne.length){
			  inOffSetOne =  readPageintoBuffer(startpage,inputBufferOne,inOffSetOne);
			 // System.out.println(inOffSetOne);
			  startpage++;
			}
				while(inOffSetTwo < inputBufferTwo.length){
					inOffSetTwo = readPageintoBuffer(startpage,inputBufferTwo,inOffSetTwo);
					startpage++;
				}
			
			//System.out.println(startpage);
			
			inOffSetOne = 0;
			inOffSetTwo = 0;
			
			merge();
			
			System.out.println("end of callmerge for loop");
		} 
		
		System.out.println("end of callmerge");
	}
	
	public void merge() throws Exception{
		
		//System.out.println("inside merge");
		
		//System.out.println(inputBufferOne.length);
		//System.out.println(inputBufferTwo.length);
		
		int readBytesOne = 0;
		int readBytesTwo = 0;
		
		byte[] inBuffTupleOne = new byte[4];
		byte[] inBuffTupleTwo = new byte[4];
		
		for(int l=0; l<4; l++){
			inBuffTupleOne[l] = inputBufferOne[readBytesOne+l];
			inBuffTupleTwo[l] = inputBufferTwo[readBytesTwo+l];
		}
		
		while(true){
			System.out.println("inTuOne - " + ByteBuffer.wrap(inBuffTupleOne).getInt() + " - " + readBytesOne);
			System.out.println("inTuTwo - " + ByteBuffer.wrap(inBuffTupleTwo).getInt() + " - " + readBytesTwo);
		   
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
		
		System.out.println("End of merge");
		
	}
	
}