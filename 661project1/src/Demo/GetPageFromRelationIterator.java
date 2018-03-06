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
	public int tupleCount = 0;
	public int bytesRead;
	public GetPageFromRelationIterator(String filename, int currentpage) throws Exception{
		this.filename = filename;
		this.nextpage = currentpage;
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
			open1();
			
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
	public byte[] next1() throws Exception
	{
		byte[] val = new byte[tuple.getLength()];
		
		
		for(int j=0;j<tuple.getLength();j++)
		{
			val[j]= this.openBuffer[this.bytesRead+j];
		}
		this.bytesRead = this.bytesRead + tuple.getLength();
		tupleCount--;
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