package Demo;

import PTCFramework.ConsumerIterator;
import PTCFramework.PTCFramework;
import PTCFramework.ProducerIterator;

public class ProcessRelation2{
public static void main(String[] args) throws Exception{
		
		ProducerIterator<byte []> relationProducerIterator= new GetTupleFromRelationIterator("myDisk1", 31, 11);
		ConsumerIterator<byte []> consumerIterator= new PutTupleInRelationIterator(27,"myDisk1");
		PTCFramework<byte[], byte[]>relationToRelationFramework= new RelationToRelationPTC1(relationProducerIterator, consumerIterator);
		
		relationToRelationFramework.run();
		
	}
}