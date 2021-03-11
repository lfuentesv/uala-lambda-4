package cl.lfuentes.lambda.lambda4;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;

public class SnsFunctionHandler implements RequestHandler<SNSEvent, String> {

	private DynamoDB dynamoDb;
    private String DYNAMODB_TABLE_NAME = "ContactoLFV";
	private Regions REGION = Regions.US_EAST_1;
    

    private void initDynamoDbClient() {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(REGION).build();

		this.dynamoDb = new DynamoDB(client);
	}
    
    
	
    @Override
    public String handleRequest(SNSEvent event, Context context) {
        context.getLogger().log("Received event: " + event);
        
        String message = "oops";
        String id = event.getRecords().get(0).getSNS().getMessage();
       
        this.initDynamoDbClient();
    	
        Table table = dynamoDb.getTable(DYNAMODB_TABLE_NAME);     
        
//    	UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("id", id)
//    			.withUpdateExpression("set #s = PROCESSED").withValueMap(new ValueMap().withString("#s", "status"))
//    			.withReturnValues(ReturnValue.UPDATED_NEW);
    	
    	
    	Map<String, String> expressionAttributeNames = new HashMap<String, String>();
    	expressionAttributeNames.put("#P", "status");
    	
    	Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
    	expressionAttributeValues.put(":val1", "PROCESSED");
    	
    	try {
            System.out.println("Actualizando item: "+ id);
//            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            
            
//            UpdateItemOutcome outcome =  table.updateItem(
//            	    "id",          // key attribute name
//            	    id,           // key attribute value
//            	    "set #P = :val1", // UpdateExpression
//            	    expressionAttributeNames,
//            	    expressionAttributeValues);
            
            System.out.println("Nueva expression2");
            UpdateItemOutcome outcome = table.updateItem(new PrimaryKey("id",id), new AttributeUpdate("status").put("PROCESSED") );

            message= "Actualizacion exitosa";
            System.out.println(message);
            

        }
        catch (Exception e) {
        	message = "No se pudo actualizar item: " + id+" ex: "+e.getMessage();
            System.err.println(message);
            System.err.println(e.getMessage());
            
        }
    	
    	
        return message;
    }
}
