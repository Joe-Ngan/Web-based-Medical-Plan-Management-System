package edu.northeastern.utils;

import edu.northeastern.Service.PlanService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Component
public class ETagUtils {

    private static final Logger logger = LoggerFactory.getLogger(ETagUtils.class);

    public boolean verifyEtag(HttpServletRequest request, String value) {
        String ifMatch = request.getHeader("If-Match");
        if(ifMatch==null)return false;
        String etag = generateEtag(value);
        if(!etag.equals(ifMatch))return false;
        return true;
    }

    public String generateEtag(String body) {
        try{
            byte[] hashBody = MessageDigest.getInstance("MD5").digest(body.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexSb = new StringBuilder();
            for (byte b : hashBody) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1){
                    hexSb.append('0');
                }
                hexSb.append(hex);
            }
            return hexSb.toString();
        }catch (NoSuchAlgorithmException ex){
            logger.error("Error occurred when generating eTag. "+ ex.getMessage());
            ex.printStackTrace();
            return null;
        }
    }
}
