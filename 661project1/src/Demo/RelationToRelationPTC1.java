package Demo;

import java.nio.ByteBuffer;

import PTCFramework.ConsumerIterator;
import PTCFramework.PTCFramework;
import PTCFramework.ProducerIterator;

public class RelationToRelationPTC1 extends PTCFramework<byte [], byte []> {

	public RelationToRelationPTC1(ProducerIterator<byte []> pIterator,ConsumerIterator<byte []> cIterator) {
		super(pIterator, cIterator);
	}
	
	public void run(){
		try{
			this.producerIterator.open();
			this.consumerIterator.open();
			while(this.producerIterator.hasNext()){
				byte [] producerElement= producerIterator.next();
				byte [] salary = new byte[4];
				
				//read the salary bytes
				for(int i=0; i<4; i++){
					salary[i] = producerElement[27+i];
				}
				int salaryval = ByteBuffer.wrap(salary).getInt();
				
				//check if salary>=5000, only then do transformation
				if(salaryval>=50000){
					byte [] bytes= new byte[27];
					
					//read the name bytes
					for(int i=0;i<23;i++){
						bytes[i]=producerElement[i];
					}
					
					//read the salary bytes
					for(int i=0; i<4; i++){
						bytes[23+i] = producerElement[27+i];
					}
					
					//Send the transformed tuple to the Consumer Iterator
					consumerIterator.next(bytes);
				}
			}
			this.producerIterator.close();
			this.consumerIterator.close();
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	
}