package Demo;

import java.nio.ByteBuffer;

import PTCFramework.ConsumerIterator;
import StorageManager.Storage;

public class PutTupleInRelationIterator implements ConsumerIterator<byte []>{
	Storage storage;
	long currentpage;
	int byteswrittentopage;
	int tuplelength;
	String fileName;
	int pageSize;
	long initialFirstPage = 0;
	byte [][] bufferMatrix;
	
	public PutTupleInRelationIterator(int tuplelength,String fileName) throws Exception {
		this.tuplelength= tuplelength;
		this.fileName=fileName;
		storage = new Storage(this.fileName);
	}
	public void initializePage() throws Exception {
		pageSize = Storage.pageSize;
		currentpage = -1;
		byteswrittentopage = 8;
	}
	public void open() throws Exception{
		//storage= new Storage();
		//storage.loadStorage(fileName);
		pageSize=Storage.pageSize;

		currentpage=-1;
		byteswrittentopage=8;
		
		
	}
	
	/* 
	 * Writing open function for initializing 3 buffers for creating runs.
	 * */
	
	public void open1()
	{
		bufferMatrix = new byte[TestPages.availableBuffers][Storage.pageSize];
		
	}
	
	public long getInitialFirstPage(){
		return initialFirstPage;
	}
	
	public int getNumAllocated(){
		return storage.getNumAllocated();
	}
	
	public void close(){
	}
	
	public boolean hasNext(){
		return true;
	}
	
	public byte [] next(){
		return null;
	}
	public void next(byte [] tuple) throws Exception{
		byte[] buffer= new byte[pageSize];
		
		if(currentpage==-1){
			currentpage=storage.AllocatePage();
			//Set the next pointer of the page to -1
			byte [] nextPage=ByteBuffer.allocate(4).putInt(-1).array();
			for(int i=0;i<4;i++){
				buffer[4+i]= nextPage[i];
			}
			storage.WritePage(currentpage, buffer);
			System.out.println("The first page of this relation is: "+currentpage);
			initialFirstPage = currentpage;
		}
		
		buffer= new byte[pageSize];
		
		//If the new tuple if written will make the current page overflow, allocate a new page.
		if(byteswrittentopage+this.tuplelength>=pageSize){
			storage.ReadPage(currentpage, buffer);
			long prevpage=currentpage;
			currentpage=storage.AllocatePage();
			Integer count= (byteswrittentopage-8)/tuplelength;
			
			//write the number of tuples and the page number of the next allocated page to the first 
			//8 bytes of the page and write the page to storage.
			byte [] counttuples=ByteBuffer.allocate(4).putInt(count).array();
			for(int i=0;i<4;i++){
				buffer[i]= counttuples[i];
			}
			
			
			byte[] nextPage=ByteBuffer.allocate(4).putInt((int) currentpage).array();
			for(int i=4;i<8;i++){
				buffer[i]= (byte) nextPage[i-4];
			}
			storage.WritePage(prevpage, buffer);
			
			
			//Initialize the buffer for the new page created
			buffer= new byte[pageSize];
			for(int i=0;i<8;i++){
				buffer[i]=0;
			}
			byteswrittentopage=8;
			
			//Write the tuple to the buffer
			for(int i=8;i<8+tuplelength;i++){
				buffer[i]=tuple[i-8];
			}
			
			
			count= (byteswrittentopage-8)/tuplelength;
			
			//write the number of tuples written on the current page to the buffer.
			counttuples=ByteBuffer.allocate(4).putInt(count).array();
			for(int i=0;i<4;i++){
				buffer[i]= counttuples[i];
			}
			
			//Set the next pointer of the page to -1
			nextPage=ByteBuffer.allocate(4).putInt(-1).array();
			for(int i=0;i<4;i++){
				buffer[4+i]= nextPage[i];
			}
			
			
			byteswrittentopage+=tuplelength;
			
			//Write the buffer to the storage.
			storage.WritePage(currentpage, buffer);
			
			
		} else {//Else if the page can accommodate the tuple to be written
			buffer= new byte[pageSize];
			storage.ReadPage(currentpage, buffer);
			for(int i=0;i<this.tuplelength;i++){
				buffer[byteswrittentopage++]=tuple[i];
			}
			
			Integer count= (byteswrittentopage-8)/tuplelength;
			ByteBuffer b = ByteBuffer.allocate(4);
			b.putInt(count);
			byte[] result = b.array();
			for(int i=0;i<4;i++){
				buffer[i]=result[i];
			}
			
			storage.WritePage(currentpage, buffer);
		}
	}

	@Override
	public void remove() {
		
	}
	@Override
	
	// method to put data as bytes into storage
	
	public void putTupleInStorage(byte[] tuple) throws Exception {
		// TODO Auto-generated method stub
		byte[] buffer= new byte[pageSize];
		
		if(currentpage==-1){
			currentpage=storage.AllocatePage();
			//Set the next pointer of the page to -1
			byte [] nextPage=ByteBuffer.allocate(4).putInt(-1).array();
			for(int i=0;i<4;i++){
				buffer[4+i]= nextPage[i];
			}
			storage.WritePage(currentpage, buffer);
			System.out.println("The first page of this relation is: "+currentpage);
			initialFirstPage = currentpage;
		}
		
		buffer= new byte[pageSize];
		
		//If the new tuple if written will make the current page overflow, allocate a new page.
		if(byteswrittentopage+this.tuplelength>=pageSize){
			storage.ReadPage(currentpage, buffer);
			long prevpage=currentpage;
			currentpage=storage.AllocatePage();
			Integer count= (byteswrittentopage-8)/tuplelength;
			
			//write the number of tuples and the page number of the next allocated page to the first 
			//8 bytes of the page and write the page to storage.
			byte [] counttuples=ByteBuffer.allocate(4).putInt(count).array();
			for(int i=0;i<4;i++){
				buffer[i]= counttuples[i];
			}
			
			
			byte[] nextPage=ByteBuffer.allocate(4).putInt((int) currentpage).array();
			for(int i=4;i<8;i++){
				buffer[i]= (byte) nextPage[i-4];
			}
			storage.WritePage(prevpage, buffer);
			
			
			//Initialize the buffer for the new page created
			buffer= new byte[pageSize];
			for(int i=0;i<8;i++){
				buffer[i]=0;
			}
			byteswrittentopage=8;
			
			//Write the tuple to the buffer
			for(int i=8;i<8+tuplelength;i++){
				buffer[i]=tuple[i-8];
			}
			
			
			count= (byteswrittentopage-8)/tuplelength;
			
			//write the number of tuples written on the current page to the buffer.
			counttuples=ByteBuffer.allocate(4).putInt(count).array();
			for(int i=0;i<4;i++){
				buffer[i]= counttuples[i];
			}
			
			//Set the next pointer of the page to -1
			nextPage=ByteBuffer.allocate(4).putInt(-1).array();
			for(int i=0;i<4;i++){
				buffer[4+i]= nextPage[i];
			}
			
			
			byteswrittentopage+=tuplelength;
			
			//Write the buffer to the storage.
			storage.WritePage(currentpage, buffer);
			
			
		} else {//Else if the page can accommodate the tuple to be written
			buffer= new byte[pageSize];
			storage.ReadPage(currentpage, buffer);
			for(int i=0;i<this.tuplelength;i++){
				buffer[byteswrittentopage++]=tuple[i];
			}
			
			Integer count= (byteswrittentopage-8)/tuplelength;
			ByteBuffer b = ByteBuffer.allocate(4);
			b.putInt(count);
			byte[] result = b.array();
			for(int i=0;i<4;i++){
				buffer[i]=result[i];
			}
			
			storage.WritePage(currentpage, buffer);
		}

	}
}