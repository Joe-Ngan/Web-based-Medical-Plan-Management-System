package edu.northeastern.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

@Component
public class JsonUtils {

    private static final Logger logger = LoggerFactory.getLogger(JsonUtils.class);

    public static JsonNode stringToNode(String str) {
        if (str == null){
            return null;
        }
        if(str.isEmpty()){
            str = "{}";
        }
        try {
            return JsonLoader.fromString(str);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean validateWithJsonSchema(JsonNode requestBodyJson, String path) {
        try{
            URL url = JsonUtils.class.getResource(path);
            String urlString = String.valueOf(url.toURI());
            JsonSchemaFactory jsonSchemaFactory = JsonSchemaFactory.byDefault();
            JsonSchema schema = jsonSchemaFactory.getJsonSchema(urlString);
            return schema.validate(requestBodyJson).isSuccess();
        }catch (URISyntaxException ex){
            logger.error("Invalid URI detected when validate request body with path:"+path);
            ex.printStackTrace();
        } catch (ProcessingException ex){
            logger.error("Failed to parse Json Schema from:" + path);
            ex.printStackTrace();
        }
        return false;
    }
}
