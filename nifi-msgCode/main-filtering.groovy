import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.commons.io.IOUtils
import org.apache.nifi.processor.io.StreamCallback
import org.apache.groovy.json.internal.*;

import java.nio.charset.StandardCharsets

def flowFile = session.get();
if (flowFile == null) {
    return;
}

flowFile = session.write(flowFile, { inputStream, outputStream ->

    def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    def inJson = new JsonSlurper().parseText(content)
    def partnerArr = inJson.partners;
    def vendorNo = "";
    def tradingCompanyRelevant = "N";
  	for (partner in partnerArr) {      
      if (partner.vendorPartnerTypeCode.equals("VN")) {
        
        vendorNo = partner.vendorNumber;        
      }
      if (partner.vendorPartnerTypeCode.equals("IP") && partner.vendorNumber != null && partner.vendorNumber.length() != 0) {        
        tradingCompanyRelevant = "Y";
      }
  	}
    inJson.purchaseOrderHeader["vendorNumber"] = vendorNo;
    inJson.purchaseOrderHeader.TradingCompanyRelevant =  tradingCompanyRelevant;
  
  
    def organizationArr = inJson.organizations;
    def purchaser = "";
    for (organization in organizationArr) {
      if (organization.organizationType.equals("PURCHASER_ORG")) {
        purchaser = organization.PURCHASER_ORG;
        break;
      }
    }
    inJson.purchaseOrderHeader["PURCHASER_ORG"] = purchaser;
  
    def headerRefArr = inJson.headerRef;
    def referenceT = false;
    def referenceNo = ""
  	for (headerRef in headerRefArr) {
      if (headerRef.referenceType != null && headerRef.referenceType.equals("COUNTRY_REGION_PURCHASE_ORDER")) {
       	referenceT = true;
        referenceNo = headerRef.referenceNumber;
        break;
      }  
    }
  
    // headerReference ZP29  
  
   	inJson.put("headerReference", new LazyMap())
   	inJson.headerReference << [CountryRegionPONumber: ""]  
  
    if (referenceT && inJson.purchaseOrderHeader.documentTypeCode != null && 
        inJson.purchaseOrderHeader.documentTypeCode.equals("ZP29")) {
        inJson.headerReference.CountryRegionPONumber = referenceNo;
    }
    
  
    // removing unwanted keys
  
    inJson.remove("partners");
    inJson.remove("organizations");
    inJson.remove("headerRef");
   
    // Converting back to JSON
  	def builder = new JsonBuilder(inJson)  	
    outputStream.write(builder.toPrettyString().getBytes(StandardCharsets.UTF_8))
  
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
