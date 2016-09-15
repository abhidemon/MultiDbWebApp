package main.java.controllers;

import main.java.beans.Message;

/**
 * Created by abhishek.singh on 14/09/16.
 */
public class GetMessage {

    public static Message getMessage(){
        return new Message(12l,"qwert");
    }

}
