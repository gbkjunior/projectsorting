package Demo;

import java.nio.ByteBuffer;

import PTCFramework.ProducerIterator;
import StorageManager.Storage;

public class GetTupleFromRelationIterator implements ProducerIterator<byte []>{
	String filename;
	int tuplelength;
	int currentpage;
	int count = 0;
	int nextPage;
	int numbytesread;
	int pagesize;
	Storage s;
	byte[] mainbuffer;
	
	public GetTupleFromRelationIterator(String filename, int tuplelength, int currentpage){
		this.filename = filename;
		this.tuplelength = tuplelength;
		this.nextPage = currentpage;
	}
	
	public void open() throws Exception{
		s = new Storage();
		s.LoadStorage(filename);
		this.pagesize = s.pageSize;
	}
	
	public void checkNextPage() throws Exception{
		if(nextPage!=-1){
			currentpage = nextPage;
			byte[] buffer = new byte[pagesize];
			s.ReadPage(currentpage, buffer);
			mainbuffer = buffer;
			numbytesread = 8;
			
			int size = 4;
			byte[] countval = new byte[size];
			for(int i=0; i<4; i++){
				countval[i] = buffer[i];
			}
			count = ByteBuffer.wrap(countval).getInt();
			for(int i=0; i<4; i++){
				countval[i] = buffer[i+4];
			}
			nextPage = ByteBuffer.wrap(countval).getInt();
		}
	}
	
	public boolean hasNext(){
		if(count<=0){
			try {
				checkNextPage();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(count>0){
				return true;
			}
			else{
				return false;
			}
		}
		return true;
	}
	
	public void prinnext(){
		System.out.println(count);
		count--;
	}
	
	public byte[] next(){
		byte[] res = new byte[tuplelength];
		for(int i=0; i<tuplelength; i++){
			res[i] = mainbuffer[numbytesread+i];
		}
		numbytesread+=tuplelength;
		count--;
		return res;
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		
	}
	
}