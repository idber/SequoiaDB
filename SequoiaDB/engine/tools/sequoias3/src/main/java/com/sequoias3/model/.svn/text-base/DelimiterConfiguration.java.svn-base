package com.sequoias3.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

@JacksonXmlRootElement(localName ="DelimiterConfiguration")
public class DelimiterConfiguration {
    @JsonProperty("Delimiter")
    private String delimiter;
    @JsonProperty("Status")
    private String status;

    public DelimiterConfiguration(){}

    public DelimiterConfiguration(String delimiter, String status){
        this.delimiter = delimiter;
        this.status    = status;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }
}
