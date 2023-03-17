package edu.northeastern.utils;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.crypto.DefaultJwtSignatureValidator;
import org.springframework.stereotype.Component;

import javax.crypto.spec.SecretKeySpec;
import java.net.URL;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.Date;

import static io.jsonwebtoken.SignatureAlgorithm.HS256;


@Component
public class JwtUtils {
    private static final String ISSUER = "https://accounts.google.com";
    private static final String AUDIENCE = "861514686898-4qhjfr1alpk0tvhi1j6dsdot2u77tv4j.apps.googleusercontent.com";
    private static final String CERT_URL = "https://www.googleapis.com/oauth2/v3/certs";

    public static boolean verify(String tokenHeader) {
        if(tokenHeader==null)return false;
        try {
            // Parse the JWT token: bearer xxxxxx
            String jwt = tokenHeader.substring(7);
            DecodedJWT decodedJWT = JWT.decode(jwt);

            JwkProvider provider = new UrlJwkProvider(new URL(CERT_URL));
            RSAPublicKey publicKey = (RSAPublicKey) provider.get(decodedJWT.getKeyId()).getPublicKey();
            Algorithm algorithm = Algorithm.RSA256(publicKey, null);
            JWTVerifier verifier = JWT.require(algorithm)
                            .withIssuer(decodedJWT.getIssuer())
                            .build();
            // 1. Verify the signature
            verifier.verify(decodedJWT);
            // 2. Verify the issuer and audience claims
            if (!decodedJWT.getIssuer().equals(ISSUER) || !decodedJWT.getAudience().get(0).equals(AUDIENCE)) {
                System.out.println("wrong issuer or audience");
                return false;
            }
            // 3. Verify that the token has not expired
            if (decodedJWT.getExpiresAt().before(new Date())) {
                System.out.println("expired date");
                return false;
            }
            return true;
        } catch (Exception e) {// An error occurred while parsing or verifying the token
            System.out.println("JWT signature validation failed");
            return false;
        }
    }
}
