package Demo;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import PTCFramework.ConsumerIterator;
import StorageManager.Storage;
import Tuple.Tuple;

public class CreateRuns{
	int numBuffers;
	int startpage;
	int numPages;
	byte[][] byteMatrix;
	int lastindex;
	
	Tuple t = new Tuple();
	Storage s = new Storage();
	ConsumerIterator<byte []> consiter = new PutTupleInRelationIterator(35,"myDiskMine");
	
	public CreateRuns(int numBuffers, int startpage, int numPages) throws Exception{
		this.numBuffers = numBuffers;
		this.numPages = numPages;
		this.startpage = startpage;
		s.LoadStorage("myDiskMine");
		consiter.open();
		
		System.out.println("First Step - Create Runs - Available Buffers : "+numBuffers);
		System.out.println("Processing Pass - 1 "+"no: of Pages read - "+numPages+" no of Pages written - "+numPages);
		System.out.println();
		
		run();
		SortMergeIters iter = new SortMergeIters(getLastSortPage(numPages),numPages,numBuffers);
		
	}
	
	public void run() throws Exception{
		byteMatrix = new byte[numBuffers][];
		
		for(int j=0; j<numPages;){
		    for(int i=0; i<numBuffers; i++){
			    byteMatrix[i] = new byte[1024];
		    }
		
		for(int i=0; i<numBuffers; i++){
			if(j<numPages){
				s.ReadPage(startpage+j, byteMatrix[i]);
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
				count[i] = byteMatrix[j][i];
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
						tuple[l] = byteMatrix[j][currentindex[j]+l];
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
		page = (int) s.getLastAllocated();
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