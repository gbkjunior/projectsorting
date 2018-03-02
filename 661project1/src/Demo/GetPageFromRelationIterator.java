package Demo;

import java.nio.ByteBuffer;

import PTCFramework.ProducerIterator;
import StorageManager.Storage;

public class GetPageFromRelationIterator implements ProducerIterator<byte []>{
	String filename;
	int currentpage;
	int nextpage;
	Storage s;
	private int pagesize;
	
	public GetPageFromRelationIterator(String filename, int currentpage){
		this.filename = filename;
		this.nextpage = currentpage;
	}

	@Override
	public boolean hasNext() {
		if(nextpage!=-1){
			currentpage = nextpage;
			return true;
		}
		return false;
	}

	@Override
	public byte[] next() {
		byte[] buffer = new byte[pagesize];
		try {
			s.ReadPage(currentpage, buffer);
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
		s = new Storage();
		s.LoadStorage(filename);
		this.pagesize = s.pageSize;
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