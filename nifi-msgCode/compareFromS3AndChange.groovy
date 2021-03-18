// Comparing from existing file on S3 for Change

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.commons.io.IOUtils
import org.apache.nifi.processor.io.StreamCallback

import java.nio.charset.StandardCharsets

def flowFile = session.get()
if(!flowFile) return

def jsonSlurper = new JsonSlurper()
def incomingRequest = flowFile.getAttribute("originalRequest.0")
def incomingJSON = jsonSlurper.parseText(incomingRequest)

flowFile = session.write(flowFile, { inputStream, outputStream ->

    def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    def s3Json = new JsonSlurper().parseText(content)
    
    def purchaseOrderItemArr = incomingJSON.purchaseOrderItem;
    def s3POMap = [:];
  	def s3PurchaseOrderItemArr = s3Json.purchaseOrderItem;
  	
  	for (s3PurchaseOrder in s3PurchaseOrderItemArr) {
      s3POMap[s3PurchaseOrder.lineItemNumber] = s3PurchaseOrder;
    }
    
    
  	for (purchaseOrder in purchaseOrderItemArr) {
      if (purchaseOrder.actionDescription.equalsIgnoreCase("Change")) {
        def s3PO = s3POMap[purchaseOrder.lineItemNumber];
        if (s3PO != null) {
          if ( purchaseOrder.goodsAtConsolidatorDate != s3PO.goodsAtConsolidatorDate ) {
            purchaseOrder.FieldChange = "GAC";
          } else if (purchaseOrder.acceptanceDate != s3PO.acceptanceDate ) {
            purchaseOrder.FieldChange = "Acceptance";
          } else {
            purchaseOrder.FieldChange = "null";
          }
        }
      }
    }
    
  	def builder = new JsonBuilder(incomingJSON)  	
    outputStream.write(builder.toPrettyString().getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

flowFile = session.putAttribute(flowFile, 'filterForHttp', 'true')
session.transfer(flowFile, REL_SUCCESS)