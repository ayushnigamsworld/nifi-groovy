import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.commons.io.IOUtils
import org.apache.nifi.processor.io.StreamCallback

import java.nio.charset.StandardCharsets

def flowFile = session.get();
if (flowFile == null) {
    return;
}

def newPurchaseOrderItemArr = [];
flowFile = session.write(flowFile, { inputStream, outputStream ->

    def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    def inJson = new JsonSlurper().parseText(content)
    def purchaseOrderItemArr = inJson.purchaseOrderItem;  
    
  	
      for (purchaseOrderItem in purchaseOrderItemArr) {
        if (purchaseOrderItem.actionDescription.equals("ADD")) {
          newPurchaseOrderItemArr.add(purchaseOrderItem);

        } else if (purchaseOrderItem.actionDescription.equals("CHANGE")  && !purchaseOrderItem.FieldChange.equals("null")) {
          newPurchaseOrderItemArr.add(purchaseOrderItem);
        }
      }
    inJson.purchaseOrderItem = newPurchaseOrderItemArr;
    
  	def builder = new JsonBuilder(inJson)  	
    outputStream.write(builder.toPrettyString().getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

if (newPurchaseOrderItemArr.size() == 0) {
  flowFile = session.putAttribute(flowFile, 'sendToHttp', "false")
} else {
  flowFile = session.putAttribute(flowFile, 'sendToHttp', "true")
}

session.transfer(flowFile, REL_SUCCESS)
