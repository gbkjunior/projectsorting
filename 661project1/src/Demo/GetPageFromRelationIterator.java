package Demo;

import java.nio.ByteBuffer;

import PTCFramework.ProducerIterator;
import StorageManager.Storage;
import Tuple.Tuple;

public class GetPageFromRelationIterator implements ProducerIterator<byte []>{
	String filename;
	int currentpage;
	int nextpage;
	Storage storage;
	Tuple tuple;
	private int pagesize;
	public byte [] openBuffer;
	byte[][] bufferMatrix;
	public int tupleCount = 0;
	public byte [][] tupleCountBuffer;
	public byte [][] nextPageBuffer;
	public int nextBufferNum=0;
	public int bytesRead;
	int numPages;
	int numBuffers = TestPages.availableBuffers;
	
	
	public GetPageFromRelationIterator(String filename, int currentpage) throws Exception{
		this.filename = filename;
		this.nextpage = currentpage;
		this.numPages = TestPages.numPages;
		storage = new Storage(this.filename);
		tuple = new Tuple();
	}

	@Override
	public void openFile()
	{
		
	}
	
	public void checkNextPage()
	{
		if(nextpage != -1)
		{
			currentpage = nextpage;
			open2();
			
		}
	}
	public boolean hasNext1()
	{
		try{
			if(tupleCount <=0){
				checkNextPage();
				
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		if(tupleCount>0)
		{
			return true;
		}
		else return false;

	}
	public boolean hasNext() {
		if(nextpage!=-1){
			currentpage = nextpage;
			return true;
		}
		return false;
	}

	@Override
	public byte[] next() {
		byte[] buffer = new byte[Storage.pageSize];
		try {
			storage.ReadPage(currentpage, buffer);
			byte[] temp = new byte[4];
			for(int i=0; i<4; i++){
				temp[i] = buffer[i+4];
			}
			nextpage = ByteBuffer.wrap(temp).getInt();
			System.out.println("Next Page:" + nextpage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return buffer;
	}

	@Override
	public void open() throws Exception {
		
		System.out.println("storage page size returned: " + storage.getPageSize());
		storage.getPageSize();
	}
	
	public void open1() {
		this.pagesize = Storage.pageSize;
		this.openBuffer = new byte[this.pagesize];
		try{
			storage.ReadPage(currentpage, this.openBuffer);
			byte[] tupleCountBuffer = new byte[4];
			for(int i=0; i<4; i++){
				tupleCountBuffer[i] = openBuffer[i];
			}
			this.tupleCount = ByteBuffer.wrap(tupleCountBuffer).getInt();
			System.out.println(tupleCount);
			this.bytesRead = 8;
			byte [] nextPageBuffer = new byte[4];
			for(int i=0; i<4;i++)
			{
				nextPageBuffer[i] = openBuffer[i+4];
			}
			nextpage = ByteBuffer.wrap(nextPageBuffer).getInt();
			System.out.println(nextpage);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
	}
	
	/* open for creating runs using 3 buffers */
	
	public void open2() {
		this.pagesize = Storage.pageSize;
		bufferMatrix = new byte [numBuffers][];
		
		nextPageBuffer = new byte [numBuffers][];
		tupleCountBuffer = new byte[numBuffers][];
		    
		//this.openBuffer = new byte[this.pagesize];
		try{
			
			    for(int i=0; i<numBuffers; i++){
			    	if(nextpage != -1)
			    	{
			    		
			    	
				    bufferMatrix[i] = new byte[Storage.pageSize]; 
				    storage.ReadPage(currentpage+i, bufferMatrix[i]);
				    byte[] tupleCountBuffer1 = new byte[4];
					for(int k=0; k<4; k++){
						tupleCountBuffer1[k] = bufferMatrix[i][k];
					}
					this.tupleCount = ByteBuffer.wrap(tupleCountBuffer1).getInt();
					tupleCountBuffer[i] = new byte[1]; 
					tupleCountBuffer[i][0] = (byte) tupleCount;
					System.out.println(tupleCountBuffer[i][0]);
					this.bytesRead = 8;
					byte [] nextPageBuffer1 = new byte[4];
					for(int k=0; k<4;k++)
					{
						nextPageBuffer1[k] = bufferMatrix[i][k+4];
					}
					this.nextpage = ByteBuffer.wrap(nextPageBuffer1).getInt();
					nextPageBuffer[i] = new byte[1];
					nextPageBuffer[i][0] = (byte) nextpage;
					System.out.println(nextPageBuffer[i][0]);
					
			    	}
			    }
			/*
			//storage.ReadPage(currentpage, this.openBuffer);
			byte[] tupleCountBuffer = new byte[4];
			for(int i=0; i<4; i++){
				tupleCountBuffer[i] = openBuffer[i];
			}
			this.tupleCount = ByteBuffer.wrap(tupleCountBuffer).getInt();
			System.out.println(tupleCount);
			this.bytesRead = 8;
			byte [] nextPageBuffer = new byte[4];
			for(int i=0; i<4;i++)
			{
				nextPageBuffer[i] = openBuffer[i+4];
			}
			nextpage = ByteBuffer.wrap(nextPageBuffer).getInt();
			System.out.println(nextpage);*/
			    
			
			}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
	}
	
	public byte[] next1() throws Exception
	{
		byte[] val = new byte[tuple.getLength()];
		System.out.println("num pages in next: " + numPages);
		int i=0;
		for(int j=0;j<tuple.getLength();j++)
		{
			if(nextBufferNum < numPages)
			{
			val[j]= this.bufferMatrix[nextBufferNum][this.bytesRead+j];
			}
		}
		this.bytesRead = this.bytesRead + tuple.getLength();
		tupleCount--;
		nextBufferNum++;
		/*byte [] getNextPage = new byte[4];
		for(int i=0;i<4;i++)
		{
			getNextPage[i]= this.openBuffer[i+4];
		}
		nextpage = ByteBuffer.wrap(getNextPage).getInt();
		System.out.println(nextpage);
		
		
				
		
			
		
			storage.ReadPage(nextpage, this.openBuffer);
				this.bytesRead = 8;
				for(int j=0;j<tuple.getLength() && this.bytesRead < this.openBuffer.length;j++)
				{
						val[j]= this.openBuffer[this.bytesRead+j];
				}
				this.bytesRead = this.bytesRead + tuple.getLength();
			
			
			
			
		
			
		
*/
		return val;
	}
	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte [] getNextRecord() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}