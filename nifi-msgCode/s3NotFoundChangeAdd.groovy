import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.commons.io.IOUtils
import org.apache.nifi.processor.io.StreamCallback

import java.nio.charset.StandardCharsets

def flowFile = session.get();
if (flowFile == null) {
    return;
}

flowFile = session.write(flowFile, { inputStream, outputStream ->

    def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    def inJson = new JsonSlurper().parseText(content)
    inJson.EventControl.msgActnDesc = "ADD";      
    
  	def builder = new JsonBuilder(inJson);  	
    outputStream.write(builder.toPrettyString().getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

flowFile = session.putAttribute(flowFile, 'filterForHttp', 'false')

session.transfer(flowFile, REL_SUCCESS)
