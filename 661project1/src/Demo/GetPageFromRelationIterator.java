package Demo;

import java.nio.ByteBuffer;

import PTCFramework.ProducerIterator;
import StorageManager.Storage;

public class GetPageFromRelationIterator implements ProducerIterator<byte []>{
	String filename;
	int currentpage;
	int nextpage;
	Storage storage;
	public GetPageFromRelationIterator(String filename, int currentpage) throws Exception{
		this.filename = filename;
		this.nextpage = currentpage;
		storage = new Storage(this.filename);
	}

	@Override
	public void openFile()
	{
		
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
	
	/*public byte [] open() throws Exception {
		this.pagesize = s.pageSize;
		byte[] openBuffer = new byte[this.pagesize];
	}*/

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	
}