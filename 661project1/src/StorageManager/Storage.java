package StorageManager;
import java.io.RandomAccessFile;

public class Storage{
	private static String fileName;
	private static long fileSize;
	public RandomAccessFile file;
	public static int pageSize;
	private int bitMapSize;
	public int numPages;
	private int numAllocated;
	private int numDeallocated;
	private int numRead;
	private int numWritten;
	private static long lastAllocatedPage;
	
	public String getFileName()
	{
		System.out.println(Storage.fileName);
		return Storage.fileName;
	}
	
	public int getPageSize()
	{
		return Storage.pageSize;
	}
	
	public Storage()
	{
		
	}
	public Storage(String fileName) throws Exception
	{
		Storage.fileName = fileName;
		loadStorage(fileName);
	}
	public void createStorage(String fileName,int pageSize, int fileSize) throws Exception{
		Storage.fileName=fileName;
		Storage.fileSize= fileSize;
		Storage.pageSize=pageSize;
		this.numPages=(int) (Storage.fileSize/Storage.pageSize);
		
		this.bitMapSize =(int) Math.ceil(this.numPages/8.0);

		if(this.bitMapSize%16!=0){
			this.bitMapSize = (this.bitMapSize/16+1)*16;
		}
		//Allocating 16 extra bytes in the beginning for storage of parameters such as pagesize.
		this.bitMapSize=this.bitMapSize+16;
		
		System.out.println("bitmap size: "+ this.bitMapSize);
		this.file= new RandomAccessFile(Storage.fileName, "rw");
		file.seek(0);
		//Write the pagesize to the first 4 bytes in the file.
		file.writeInt(pageSize);
		
		//Write number of pages to the next 4 bytes in the file
		file.seek(4);
		file.writeInt(this.numPages);
		System.out.println("num pages:" + this.numPages);
		file.seek(0);
		
		Storage.fileSize=Storage.fileSize+this.bitMapSize;
		file.setLength(fileSize);
		file.seek(16);
		//Writing 0s to the randomaccessfile so that we physically claim the memory required for the storage.
		//first writing for the bitmap
		for(int i=16;i<this.bitMapSize;i++){
			this.file.write((byte) 0);
		}
		//Writing the file contents will 0s
		for(int i=this.bitMapSize;i<Storage.fileSize;i++){
			file.write((byte) 0);
		}
	}
	
	public void loadStorage(String fileName) throws Exception{
		this.file= new RandomAccessFile(fileName, "rw");
		
		Storage.fileSize=file.length();
		System.out.println("fileSize:" + Storage.fileSize );
		//Read bytes 4 to 7 which we used to store the number of pages
		file.seek(4);
		this.numPages= file.readInt();
		System.out.println("numpages:" + this.numPages);
		Storage.fileName=fileName;
		
		//Read the first 4 bytes of the file which we used to store the page size while creating the storage.
		file.seek(0);
		Storage.pageSize = file.readInt();
		System.out.println("this.pagesize" + Storage.pageSize);
		
		this.bitMapSize =(int) Math.ceil(this.numPages/8.0);

		if(this.bitMapSize%16!=0){
			this.bitMapSize = (this.bitMapSize/16+1)*16;
		}
		this.bitMapSize=this.bitMapSize+16;
		//System.out.println("load storage bitmap size:" + this.bitMapSize);
		
		this.numAllocated=0;
		this.numDeallocated=0;
		this.numRead=0;
		this.numWritten=0;
	}
	
	public void UnloadStorage() {
		this.file=null;
	}
	
	
	public void ReadPage(long n, byte [] buffer) throws Exception{
		//Go to the offset.
		long offset= n*Storage.pageSize+this.bitMapSize;
		file.seek(offset);
		
		//read the page in buffer.
		file.read(buffer);
		this.numRead++;
	}
	
	
	public void WritePage(long n, byte[] buffer) throws Exception{
		//Go to the required offset
		long offset= n*Storage.pageSize+this.bitMapSize;
		file.seek(offset);
		
		//Write the buffer to the file.
		file.write(buffer);
		this.numWritten++;
	}
	
	//This function changes a bit in a byte and returns the int value of the new byte.
	private int WriteBitInAByte(int offset, int byteRead, int bitToBeWritten){
		String binaryString= String.format("%8s", Integer.toBinaryString(byteRead & 0xFF)).replace(' ', '0');
		binaryString = binaryString.substring(0,offset)+bitToBeWritten+binaryString.substring(offset+1);
		int byteWrite= Integer.parseInt(binaryString,2);
		return byteWrite;
	}
	
	public long getLastAllocated(){
		return lastAllocatedPage;
	}
	
	public int getNumAllocated(){
		return numAllocated;
	}
	
	
	public long AllocatePage() throws Exception{
		file.seek(16);
		//We use bits to keep track of allocated pages. The RandomAccessFile supports only byte operations.
		//Thus, to allocate, we pick up bytes from the RandomAccessFile and then look in the bits in the byte to see 
		//if any of them is 0 or not.
		for(long i=16;i<this.bitMapSize;i++){
			int byteread;
			byteread=file.read();
			//If the byte which is read has all 1's, then all the pages are allocated. Don't look in that byte.
			if(byteread<255){
				file.seek(i);
				
				//Convert the byte into a binary string.
				String binaryString= String.format("%8s", Integer.toBinaryString(byteread & 0xFF)).replace(' ', '0');
				//Look in the string to find the first 0 bit and set it to 1. Return that page number
				for(int j=0;j<8;j++){
					if(binaryString.charAt(j)=='0'){
						binaryString=binaryString.substring(0,j)+"1"+binaryString.substring(j+1);
						file.write(Integer.parseInt(binaryString,2));
						numAllocated++;
						
						//Return the page number only if the number of pages is more than the page we are returning.
						if((i-16)*8+j<this.numPages){
							lastAllocatedPage = ((i-16)*8+j);
							return ((i-16)*8+j);
						}
						else {

							System.out.println("Error in allocating a page");
							return -1;
						}
						
					}
				}
			}
		}
		System.out.println("Error in allocating a page");
		return -1;
	}
	
	
	//To deallocate a page n, we pick up the n/8th byte from the RandomAccessFile and then change the corresponding bit in that byte to 0.
	public void DeAllocatePage(long n) throws Exception{
		file.seek(n/8);
		int byteRead= file.read();
		int byteToBeWritten = WriteBitInAByte((int) (n%8), byteRead, 0);
		file.seek(n/8);
		file.write(byteToBeWritten);
		numDeallocated++;
	}
	
	public void printStats(){
		System.out.println("Number of pages Read:"+numRead + " "+ "; Written:"+numWritten+" "+"; Allocated: "+numAllocated+" "+"; Deallocated: "+numDeallocated);
	}
	
}
