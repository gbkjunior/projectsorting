package Demo;

public class PrintRelation3{
	public static void main(String args[]) throws Exception{
		System.out.println("The tuples after loading file to Relation are: ");
		GetTupleFromRelationIterator getTupleFromRelationIterator= new GetTupleFromRelationIterator("myDisk1", 27, 21);
		getTupleFromRelationIterator.open();
		while(getTupleFromRelationIterator.hasNext()){
			byte [] tuple = getTupleFromRelationIterator.next();
			System.out.println(new String(tuple).substring(0, 23)+", "+toInt(tuple, 23));
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