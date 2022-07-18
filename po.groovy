flowFile = session.write(flowFile, { inputStream, outputStream ->
 
    def inJson = new JsonSlurper().parseText(IOUtils.toString(inputStream, StandardCharsets.UTF_8))
    def origpurchaseOrder = inJson.purchaseOrder
    def purchaseOrder = origpurchaseOrder.getClass().newInstance(origpurchaseOrder)
    def purchaseOrderItems = purchaseOrder.purchaseOrderHeader.purchaseOrderItem
 
    def resultJson = []
    for (purchaseOrderItem in purchaseOrderItems) {
        if (purchaseOrderItem) {
            def newPurchaseOrder = purchaseOrder
            newPurchaseOrder.purchaseOrderHeader.purchaseOrderItem = []
            newPurchaseOrder.purchaseOrderHeader.purchaseOrderItem.add(newpoItem)
            resultJson.add(newPurchaseOrder)
        }
    }
    outputStream.write( new JsonBuilder(resultJson).toPrettyString().getBytes(StandardCharsets.UTF_8))
} as StreamCallback)
 
//if (purchaseOrderItems.size() == 0) {
  //flowFile = session.putAttribute(flowFile, 'emptyPOItem', 'true')
//} else {
  //flowFile = session.putAttribute(flowFile, 'emptyPOItem', 'false')
//}
 
session.transfer(flowFile, REL_SUCCESS)
