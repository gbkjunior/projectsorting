package Demo;

import java.io.File;
import java.util.Scanner;

import PTCFramework.*;

public class TextFileScanIterator implements ProducerIterator<byte []>{
	File file;
	Scanner in;
	int count=0;
	public static final String textFilePath = "resources/Emp_sample.txt";
	@Override
	public boolean hasNext() {
		return in.hasNextLine();
	}

	@Override
	public byte [] next() {
		count++;
		//System.out.println("next returns :"+in.nextLine());
		return in.nextLine().getBytes();
		
		
	}
	
	@Override
	public void remove() {
		
	}

	@Override
	public void openFile() throws Exception {
		file= new File(textFilePath);
		in=new Scanner(file);
	}
	
	@Override
	public void open() throws Exception {
	
	}

	@Override
	public void close() throws Exception {
		in.close();
	}

	public byte [] getNextRecord() {
		// TODO Auto-generated method stub
		count++;
		//System.out.println("next returns :"+in.nextLine());
		return in.nextLine().getBytes();
	}
	
}
