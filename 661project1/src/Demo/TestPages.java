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
	public static final int availableBuffers = 1;
	public static Storage s1 ;
	public static int numPages;
	public static void main(String[] args) throws Exception{		
		
		long startTime = System.nanoTime();
		
		 // initializing storage variable to access its methods
		
		Tuple t = new Tuple(); // initializing tuple variable to access its methods
		s1 = new Storage();
		s1.createStorage("myDiskMine", 1024, 1024*2500);  // creating a storage by giving a name, defining page size and file size as its parameters. 
		
		String storageName = s1.getFileName(); // returns the name of storage that has been created.
		
		s1.loadStorage(s1.getFileName()); // loads the storage on to the disc 
		
		ProducerIterator<byte []> textFileProducerIterator= new TextFileScanIterator();
		ConsumerIterator<byte []> relationConsumerIterator = new PutTupleInRelationIterator(t.getLength(),storageName);
		PTCFramework<byte[],byte[]> fileToRelationFramework= new TextFileToRelationPTC(textFileProducerIterator, relationConsumerIterator);
		
		
		fileToRelationFramework.run();
		
		
		/* after the run function is executed, we have loaded the data into the storage. Now we have to create the runs using three buffers
		 * and write the sorted data back onto the disc.
		 * 
		 *  */
		
		
		
		 numPages = relationConsumerIterator.getNumAllocated();
		//System.out.println("num pages:" + numPages );
		GetPageFromRelationIterator getpagefromrelationiter = new GetPageFromRelationIterator(storageName,s1.getStartPage());
		getpagefromrelationiter.open();
		
		
		relationConsumerIterator = new PutTupleInRelationIterator(t.getLength(),storageName);
		relationConsumerIterator.open();
		
		// implementing the actual open iterator
		getpagefromrelationiter.open1(); //use this in the actual implementation
		
		while(getpagefromrelationiter.hasNext1())
		{
			List<Bytenode> byteList = new ArrayList<Bytenode>();
			for(int i=0;i<getpagefromrelationiter.noOfTuples;i++)
			{
				byte [] tupleData = getpagefromrelationiter.next1();
				byte [] key = tupleData;
				
				Bytenode byteNode = new Bytenode(key,tupleData);
				//String sam = new String(tupleData);
				//System.out.println(sam);
				byteList.add(byteNode);
			
			}
				
			
						
			byteList.sort(new Comparator<Bytenode>(){

				@Override
				public int compare(Bytenode o1, Bytenode o2) {
					byte[] keyo1 = o1.key;
					byte[] keyo2 = o2.key;
					//System.out.println(t.compare(keyo1,keyo2));
					return t.compare(keyo1, keyo2);
				}
				
			});
			
			/*for(Bytenode e : byteList)
			{
				byte[] sample = e.val;
				String getText = new String(sample);
				//Integer getInt = new Integer(sample);
				System.out.println(getText);
			}*/
			//byte[] bigArray = new byte[1024];
			//ByteBuffer target = ByteBuffer.wrap(bigArray);
			byte [] fill = new byte[Storage.pageSize];
			int bytesread = 8;
			for(Bytenode e : byteList){
				
				System.arraycopy(e.val, 0, fill, bytesread, e.val.length);
				//target.put(e.val);
				bytesread = bytesread + e.val.length;
				

			} 
			
			
			//target.get(fill);
			//System.out.println(fill);
			getpagefromrelationiter.writeDataInStorage(fill);
			//relationConsumerIterator.putTupleInStorage(fill);
		} 
	
		/* 
		 * The above getpagefromrelationiterator.hasNext() performs the following :
		 * 
		 * It opens the iterator to initialize one buffer , reads the contents of the first page on to the buffer, sorts them internally 
		 * writes it back onto the disc tuple by tuple (should be changed to writing the whole buffer onto the page) pass target onto puttupleinstorage func
		 * 
		 * hasnext checks for next page and does the same process for the next page
		 * */
		

		
		// the while loop below will sort records in a given buffer
		
	/*	while(getpagefromrelationiter.hasNext()){
			List<Bytenode> byteList = new ArrayList<Bytenode>();
			byte[] page = getpagefromrelationiter.next();
			
			int count = getpagefromrelationiter.getTupleCount(page);
			System.out.println("count :" + count);
			int bytesread = 8;
			
			for(int i=0 ; i<count; i++){
				byte[] val = new byte[t.getLength()];
				
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
					//System.out.println(t.compare(keyo1,keyo2));
					return t.compare(keyo1, keyo2);
				}
				
			});
			
			for(Bytenode e : byteList)
			{
				byte[] sample = e.val;
				String getText = new String(sample);
				//Integer getInt = new Integer(sample);
				System.out.println(getText);
			}
			for(Bytenode e : byteList){
				byte[] fill = e.val;
				relationConsumerIterator.putTupleInStorage(fill);
			} 
		}
		
*/		
		System.out.println("Number of pages before passing to create runs : " + numPages);
		// testing create runs before sorting it initially
		
		CreateRuns proc = new CreateRuns(availableBuffers,s1.getStartPage(),numPages);
		System.out.println("get last sort page :" + proc.getLastSortPage(numPages));
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