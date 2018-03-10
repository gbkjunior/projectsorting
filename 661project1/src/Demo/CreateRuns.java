package Demo;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import PTCFramework.ConsumerIterator;
import PTCFramework.ProducerIterator;
import StorageManager.Storage;
import Tuple.Tuple;

public class CreateRuns{
	int numBuffers;
	int startpage;
	static int numPages;
	byte [][] byteMatrix;
	int lastindex;
	
	Tuple t = new Tuple();
	//Storage s = new Storage();
	
	ConsumerIterator<byte []> consiter = new PutTupleInRelationIterator(t.getLength(),TestPages.s1.getFileName());
	GetPageFromRelationIterator proiter = new GetPageFromRelationIterator(TestPages.s1.getFileName(),TestPages.s1.getStartPage());
	public CreateRuns(int numBuffers, int startpage, int numPages) throws Exception{
		this.numBuffers = numBuffers;
		CreateRuns.numPages = numPages;
		this.startpage = startpage;
		//s.loadStorage(s.getFileName());
		consiter.open();
		
		System.out.println("First Step - Create Runs - Available Buffers : "+numBuffers);
		System.out.println("Processing Pass - 1 "+"no: of Pages read - "+numPages+" no of Pages written - "+numPages);
		System.out.println();
		
		proiter.open2();
		run();
		SortMergeIters iter = new SortMergeIters(getLastSortPage(numPages),numPages,numBuffers);
		
	}
	
	public byte [] sortTuplesInBuffer(byte [] bufferMatrix) throws Exception
	{
		List<Bytenode> byteList = new ArrayList<Bytenode>();
		
			byte [] getcount = new byte[4];
			for(int i=0; i<4; i++){
				getcount[i] = bufferMatrix[i];
			}
			int count = ByteBuffer.wrap(getcount).getInt();
			System.out.println("count :" + count);
			int bytesread = 8;
			
			for(int i=0 ; i<count; i++){
				byte[] val = new byte[t.getLength()];
				
				val = proiter.next1();
				
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
			
			/*byte[] b = new byte [byteList.size()];
			b = byteList.get(0).key;
			String s = new String(b);
			System.out.println(s);
			System.out.println(byteList.size());
			*/
			
			byte [] putSortedBuffer = new byte[Storage.pageSize];
			bytesread = 8;
			for(int i=0;i<8;i++)
			{
				putSortedBuffer[i] = bufferMatrix[i];
			}
			for(int j=0;j<byteList.size();j++)
			{
				for(int i=0;i<byteList.size();i++)
				{
					//Bytenode sortedList = (Bytenode) byteList.get(i);
					putSortedBuffer[bytesread+i] = byteList.get(j).val[i];
				}
				bytesread = bytesread + t.getLength();
			}
			
			return putSortedBuffer;
			//Bytenode putSortedBuffer = byteList.get(0);
			
	}
	
	public void run() throws Exception{
		byteMatrix = new byte[numBuffers][];
		
		for(int j=0; j<numPages;){
		    /*for(int i=0; i<numBuffers; i++){
			    byteMatrix[i] = new byte[Storage.pageSize];
		    }
		*/
		for(int i=0; i<numBuffers; i++){
			if(j<numPages){
				//while(proiter.hasNext1())
				
				proiter.bufferMatrix[i] = sortTuplesInBuffer(proiter.bufferMatrix[i]);
				
				/*TestPages.s1.ReadPage(startpage+j, byteMatrix[i]);
				byteMatrix[i] = sortTuplesInBuffer(byteMatrix[i]);*/
				j=j+1;
			}
			else{
				lastindex = i-1;
				break;
			}
			
			lastindex = i;
		}
		
		
		merge(lastindex);
		
		}
	}
	
	public void printByteContent(byte[] content){
		byte[] count = new byte[4];
		for(int i=0;i<4;i++){
			count[i] = content[i];
		}
		
		int countVal = ByteBuffer.wrap(count).getInt();
		System.out.println(countVal);
		int index = 8;
		
		byte[] tuple = new byte[35];
		
		while(true){
			
			if(index >= ((countVal * 35) + 8)){
				break;
			}
			
			for(int i=0; i<35; i++){
				tuple[i] = content[index+i];
			}
			
			System.out.println(new String(toInt(tuple, 0)+", "+new String(tuple).substring(4, 27)+", "+ new String(tuple).substring(27,31)+", "+ toInt(tuple, 31)));
			
			index = index + 35;
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
	
	public void merge(int lastindex) throws Exception{
		
		int[] currentindex = new int[lastindex+1];
		int[] finalindex = new int[lastindex+1];
		byte[] count = new byte[4];
		int countval;
		List<TupleNode> tupleList;
		
		for(int i=0; i<currentindex.length; i++){
			currentindex[i] = 8;
		}
		
		for(int j=0; j<=lastindex; j++){
			for(int i=0; i<4; i++){
				count[i] = proiter.bufferMatrix[j][i];
			}
			
			countval = ByteBuffer.wrap(count).getInt();
			finalindex[j] = (countval * 35) + 8;
		}
		
		while(true){
			tupleList = null;
			tupleList = new ArrayList<TupleNode>();
			
			for(int j=0; j<=lastindex; j++){
				if(currentindex[j] < finalindex[j]){
					byte[] tuple = new byte[35];
					for(int l=0; l<35; l++){
						tuple[l] = proiter.bufferMatrix[j][currentindex[j]+l];
					}
					
					TupleNode tuplenode = new TupleNode(tuple, j);
					tupleList.add(tuplenode);
				}
			}
			
			tupleList.sort(new Comparator<TupleNode>(){

				@Override
				public int compare(TupleNode o1, TupleNode o2) {
					
					return t.compare(o1.tuple, o2.tuple);
				}
				
			});
			
			TupleNode tem = (TupleNode) tupleList.get(0);
			
			
			/*for(int j=0;j<=lastindex;j++)
			{
				if(currentindex[j]<currentindex[j]+35 && currentindex[j]<finalindex[j])
				{
					for(int l=0;l<35;l++)
					{
						proiter.bufferMatrix[j][currentindex[j]+l] = tem.tuple[l];
						currentindex[j] = currentindex[j]+l;
					}
				}
			}*/
			consiter.next(tem.tuple);
			currentindex[tem.buffernum] = currentindex[tem.buffernum]+35;
			
			int finalVal = 0;
			for(int i=0; i<=lastindex; i++){
				if(currentindex[i] >= finalindex[i]){
					finalVal++;
				}
			}
			
			if(finalVal == lastindex + 1){
				break;
			}
			
		}
		
	}
	
	public int getLastSortPage(int numPages){
		int page = 0;
		page = (int) TestPages.s1.getLastAllocated();
		page = page + 1;
		page = page - numPages;
		return page;
	}
	
	class TupleNode{
		byte[] tuple;
		int buffernum;
		
		public TupleNode(byte[] tuple, int buffernum){
			this.tuple = tuple;
			this.buffernum = buffernum;
		}
	}
	
}