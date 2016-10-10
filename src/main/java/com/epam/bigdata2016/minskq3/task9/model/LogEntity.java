package com.epam.bigdata2016.minskq3.task9.model;

import java.io.Serializable;

public class LogEntity implements Serializable{

    private String line;

    public LogEntity() {
    }

    public LogEntity(String line) {
        this.line = line;
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }
}
