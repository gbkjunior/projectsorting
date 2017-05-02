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
	public static void main(String[] args) throws Exception{
		
		long startTime = System.nanoTime();
		//code
		 
		
		Storage s1 = new Storage();
		s1.CreateStorage("myDiskMine", 1024, 1024*2500);
		ProducerIterator<byte []> textFileProducerIterator= new TextFileScanIterator();
		ConsumerIterator<byte []> relationConsumerIterator = new PutTupleInRelationIterator(35,"myDiskMine");
		PTCFramework<byte[],byte[]> fileToRelationFramework= new TextFileToRelationPTC(textFileProducerIterator, relationConsumerIterator);
		fileToRelationFramework.run();
		s1.printStats();
		
		Tuple t = new Tuple();
		
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
				//byte[] key = new byte[4];
				byte[] val = new byte[35];
				
				/*for(int j=0; j<4; j++){
					key[j] = page[bytesread+j];
				} */
				
				for(int j=0; j<35; j++){
					val[j] = page[bytesread+j];
				}
				
				byte[] key = val;
				
				bytesread = bytesread + 35;
				Bytenode bytenode = new Bytenode(key,val);
				byteList.add(bytenode);
			}
			
			byteList.sort(new Comparator<Bytenode>(){

				@Override
				public int compare(Bytenode o1, Bytenode o2) {
					byte[] keyo1 = o1.key;
					byte[] keyo2 = o2.key;
					
					//System.out.println(Arrays.toString(keyo1) + " - " + keyo1.length + " || " + Arrays.toString(keyo2) + " - " + keyo2.length); 
					
					return t.compare(keyo1, keyo2);
				}
				
			});
			
			for(Bytenode e : byteList){
				byte[] fill = e.val;
				relationConsumerIterator.next(fill);
			} 
		} 
		
		System.out.println(relationConsumerIterator.getNumAllocated());
		
		s1.printStats();
		
		CreateRuns proc = new CreateRuns(5,numPages,numPages);
		
		GetTupleFromRelationIterator iter = new GetTupleFromRelationIterator("myDiskMine",35, proc.getLastSortPage(numPages));
		//GetTupleFromRelationIterator iter = new GetTupleFromRelationIterator("myDiskMine",35, 330);
		iter.open();
		PrintStream out = new PrintStream(new FileOutputStream("/Users/geethanjalijeevanatham/Desktop/output.txt"));
		System.setOut(out);
		while(iter.hasNext()){
			byte [] tuple = iter.next();
			out.println(new String(toInt(tuple, 0)+", "+new String(tuple).substring(4, 27)+", "+ new String(tuple).substring(27,31)+", "+ toInt(tuple, 31)));
			//System.out.println(new String(toInt(tuple, 0)+", "+new String(tuple).substring(4, 27)+", "+ new String(tuple).substring(27,31)+", "+ toInt(tuple, 31)));
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