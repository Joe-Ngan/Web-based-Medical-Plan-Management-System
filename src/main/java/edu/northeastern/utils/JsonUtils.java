package edu.northeastern.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;

import java.io.IOException;

public class JsonUtils {
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
}
