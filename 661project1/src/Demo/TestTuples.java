package Demo;

import PTCFramework.ConsumerIterator;
import PTCFramework.PTCFramework;
import PTCFramework.ProducerIterator;
import StorageManager.Storage;

public class TestTuples {
	public static void main(String[] args) throws Exception {
		Storage s1 = new Storage();
		s1.CreateStorage("myDisk1", 1024, 1024 * 100);
		ProducerIterator<byte[]> textFileProducerIterator = new TextFileScanIterator();
		ConsumerIterator<byte[]> relationConsumerIterator = new PutTupleInRelationIterator(35, "myDisk1");
		PTCFramework<byte[], byte[]> fileToRelationFramework = new TextFileToRelationPTC(textFileProducerIterator,
				relationConsumerIterator);
		fileToRelationFramework.run();
		GetTupleFromRelationIterator getTupleFromRelationIterator = new GetTupleFromRelationIterator("myDisk1", 35, 0);
		getTupleFromRelationIterator.open();
		// System.out.println(getTupleFromRelationIterator.hasNext());
		while (getTupleFromRelationIterator.hasNext()) {
			byte[] tuple = getTupleFromRelationIterator.next();
			System.out.println(new String(toInt(tuple, 0) + ", " + new String(tuple).substring(4, 27) + ", "
					+ new String(tuple).substring(27, 31) + ", " + toInt(tuple, 31)));
		}

		Thread.sleep(5000);

		ProducerIterator<byte[]> relationProducerIterator = new GetTupleFromRelationIterator("myDisk1", 35, 0);
		ConsumerIterator<byte[]> consumerIterator = new PutTupleInRelationIterator(31, "myDisk1");
		PTCFramework<byte[], byte[]> relationToRelationFramework = new RelationToRelationPTC(relationProducerIterator,
				consumerIterator);

		relationToRelationFramework.run();

		Thread.sleep(3000);
		System.out.println("The tuples after loading file to Relation are: ");
		getTupleFromRelationIterator = new GetTupleFromRelationIterator("myDisk1", 31, 11);
		getTupleFromRelationIterator.open();
		while (getTupleFromRelationIterator.hasNext()) {
			byte[] tuple = getTupleFromRelationIterator.next();
			System.out.println(new String(tuple).substring(0, 23) + ", " + new String(tuple).substring(23, 27) + ", "
					+ toInt(tuple, 27));
		}

		Thread.sleep(3000);

		relationProducerIterator = new GetTupleFromRelationIterator("myDisk1", 31, 11);
		consumerIterator = new PutTupleInRelationIterator(27, "myDisk1");
		relationToRelationFramework = new RelationToRelationPTC1(relationProducerIterator, consumerIterator);

		relationToRelationFramework.run();

		Thread.sleep(3000);

		System.out.println("The tuples after loading file to Relation are: ");
		getTupleFromRelationIterator = new GetTupleFromRelationIterator("myDisk1", 27, 21);
		getTupleFromRelationIterator.open();
		while (getTupleFromRelationIterator.hasNext()) {
			byte[] tuple = getTupleFromRelationIterator.next();
			System.out.println(new String(tuple).substring(0, 23) + ", " + toInt(tuple, 23));
		}
	}

	private static int toInt(byte[] bytes, int offset) {
		int ret = 0;
		for (int i = 0; i < 4; i++) {
			ret <<= 8;
			ret |= (int) bytes[offset + i] & 0xFF;
		}
		return ret;
	}

}