package org.example.kafkastreamsdemo.model;

public class OutputMessage {

    private String name;
    private String greet;

    public OutputMessage() {
    }

    public OutputMessage(String name, String greet) {
        this.name = name;
        this.greet = greet;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGreet() {
        return greet;
    }

    public void setGreet(String greet) {
        this.greet = greet;
    }

    @Override
    public String toString() {
        return "OutputMessage{name='" + name + "', greet='" + greet + "'}";
    }
}
