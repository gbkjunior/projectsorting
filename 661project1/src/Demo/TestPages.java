package Demo;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import PTCFramework.ConsumerIterator;
import PTCFramework.PTCFramework;
import PTCFramework.ProducerIterator;
import StorageManager.Storage;
import Tuple.Tuple;

public class TestPages{
	public static final String outputTextPath = "resources/output.txt";
	public static final int availableBuffers = 3;
	
	
	public static void main(String[] args) throws Exception{		
		
		long startTime = System.nanoTime();
		
		Storage s1 = new Storage();
		
		Tuple t = new Tuple();
		s1.createStorage("myDiskMine", 1024, 1024*2500);
		String storageName = s1.getFileName();
		
		
		ProducerIterator<byte []> textFileProducerIterator= new TextFileScanIterator();
		ConsumerIterator<byte []> relationConsumerIterator = new PutTupleInRelationIterator(t.getLength(),storageName);
		PTCFramework<byte[],byte[]> fileToRelationFramework= new TextFileToRelationPTC(textFileProducerIterator, relationConsumerIterator);
		
		
		fileToRelationFramework.run();
		
		
		
		int numPages = relationConsumerIterator.getNumAllocated();
		//System.out.println("num pages:" + numPages );
		GetPageFromRelationIterator getpagefromrelationiter = new GetPageFromRelationIterator(storageName,0);
		getpagefromrelationiter.open();
		
		relationConsumerIterator = new PutTupleInRelationIterator(t.getLength(),storageName);
		relationConsumerIterator.open();
		
		while(getpagefromrelationiter.hasNext()){
			List<Bytenode> byteList = new ArrayList<Bytenode>();
			byte[] page = getpagefromrelationiter.next();
			byte[] getcount = new byte[4];
			for(int i=0; i<4; i++){
				getcount[i] = page[i];
			}
			int count = ByteBuffer.wrap(getcount).getInt();
			System.out.println("count :" + count);
			int bytesread = 8;
			
			for(int i=0 ; i<count; i++){
				byte[] val = new byte[35];
				
				for(int j=0; j<t.getLength(); j++){
					val[j] = page[bytesread+j];
				}
				
				byte[] key = val;
				
				bytesread = bytesread + t.getLength();
				Bytenode bytenode = new Bytenode(key,val);
				byteList.add(bytenode);
			}
			
			byteList.sort(new Comparator<Bytenode>(){

				@Override
				public int compare(Bytenode o1, Bytenode o2) {
					byte[] keyo1 = o1.key;
					byte[] keyo2 = o2.key;
					System.out.println(t.compare(keyo1,keyo2));
					return t.compare(keyo1, keyo2);
				}
				
			});
			
			for(Bytenode e : byteList){
				byte[] fill = e.val;
				relationConsumerIterator.next(fill);
			} 
		} 
		System.out.println("Number of pages before passing to create runs : " + numPages);
		CreateRuns proc = new CreateRuns(availableBuffers,numPages,numPages);
		
		GetTupleFromRelationIterator iter = new GetTupleFromRelationIterator(storageName,t.getLength(), proc.getLastSortPage(numPages));
		iter.open();
		PrintStream out = new PrintStream(new FileOutputStream(outputTextPath));
		System.setOut(out);
		while(iter.hasNext()){
			byte [] tuple = iter.next();
			out.println(new String(toInt(tuple, 0)+", "+new String(tuple).substring(4, 27)+", "+ new String(tuple).substring(27,31)+", "+ toInt(tuple, 31)));
		}  
		
		long endTime = System.nanoTime();
		System.out.println("Took "+(endTime - startTime) + " ns"); 
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