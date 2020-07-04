package com.mphrx.datalake.resources;


import java.util.Map;

public class Address {
    //private
}

class AddressObject {
    private Map city;
    public Map getCity() {
        return city;
    }

    public void setCity(Map city) {
        this.city = city;
    }
}

class  StringType {
    private String value;
    public String getValue() {
        return value;
    }
    public void setValue(String value) {
        this.value = value;
    }
}
