package edu.northeastern.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LinkedService implements Serializable {
    @JsonProperty("_org")
    private String org;
    private String objectId;
    private String objectType;
    private String name;
}
