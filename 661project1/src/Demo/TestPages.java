package Demo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import PTCFramework.ConsumerIterator;
import PTCFramework.PTCFramework;
import PTCFramework.ProducerIterator;
import StorageManager.Storage;

public class TestPages{
	public static void main(String[] args) throws Exception{
		Storage s1 = new Storage();
		s1.CreateStorage("myDiskMine", 1024, 1024*100);
		ProducerIterator<byte []> textFileProducerIterator= new TextFileScanIterator();
		ConsumerIterator<byte []> relationConsumerIterator = new PutTupleInRelationIterator(35,"myDiskMine");
		PTCFramework<byte[],byte[]> fileToRelationFramework= new TextFileToRelationPTC(textFileProducerIterator, relationConsumerIterator);
		fileToRelationFramework.run();
		
		int numPages = relationConsumerIterator.getNumAllocated();
		
		GetPageFromRelationIterator getpagefromrelationiter = new GetPageFromRelationIterator("myDiskMine",0);
		getpagefromrelationiter.open();
		
		relationConsumerIterator = new PutTupleInRelationIterator(35,"myDiskMine");
		relationConsumerIterator.open();
		
		while(getpagefromrelationiter.hasNext()){
			List<Bytenode> byteList = new ArrayList<Bytenode>();
			byte[] page = getpagefromrelationiter.next();
			byte[] getcount = new byte[4];
			for(int i=0; i<4; i++){
				getcount[i] = page[i];
			}
			int count = ByteBuffer.wrap(getcount).getInt();
			int bytesread = 8;
			
			for(int i=0 ; i<count; i++){
				byte[] key = new byte[4];
				byte[] val = new byte[31];
				
				for(int j=0; j<4; j++){
					key[j] = page[bytesread+j];
				}
				
				for(int j=0; j<31; j++){
					val[j] = page[bytesread+4+j];
				}
				bytesread = bytesread + 35;
				Bytenode bytenode = new Bytenode(key,val);
				byteList.add(bytenode);
			}
			
			byteList.sort(new Comparator<Bytenode>(){

				@Override
				public int compare(Bytenode o1, Bytenode o2) {
					int keyo1 = ByteBuffer.wrap(o1.key).getInt();
					int keyo2 = ByteBuffer.wrap(o2.key).getInt();
					
					if(keyo1 > keyo2)
						return 1;
					else if(keyo1 < keyo2)
						 return -1;
					return 0;
				}
				
			});
			
			for(Bytenode e : byteList){
				byte[] fill = new byte[35];
				
				for(int i=0; i<4; i++){
					fill[i] = e.key[i];
				}
				
				for(int i=0; i<31; i++){
					fill[i+4] = e.val[i];
				}
				
				relationConsumerIterator.next(fill);
			}
		}
		System.out.println(numPages);
		//MergeSortProcess proc = new MergeSortProcess(numPages,numPages,numPages);
		
		MergeSortNew proc = new MergeSortNew(numPages,numPages);
		
		GetTupleFromRelationIterator iter = new GetTupleFromRelationIterator("myDiskMine",35,proc.getLastSortPage(numPages));
		iter.open();
		while(iter.hasNext()){
			byte [] tuple = iter.next();
			System.out.println(new String(toInt(tuple, 0)+", "+new String(tuple).substring(4, 27)+", "+ new String(tuple).substring(27,31)+", "+ toInt(tuple, 31)));
		}  
	}
	
	private static int toInt(byte[] bytes, int offset) {
		  int ret = 0;
		  for (int i=0; i<4; i++) {
		    ret <<= 8;
		    ret |= (int)bytes[offset+i] & 0xFF;
		  }
		  return ret;
		}
}

class Bytenode{
	byte[] key;
	byte[] val;
	
	public Bytenode(byte[] key, byte[] val){
		this.key = key;
		this.val = val;
	}
}