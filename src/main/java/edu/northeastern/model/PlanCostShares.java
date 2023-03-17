package edu.northeastern.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PlanCostShares implements Serializable {
    private int deductible;
    @JsonProperty("_org")
    private String org;
    private int copay;
    private String objectId;
    private String objectType;
}
