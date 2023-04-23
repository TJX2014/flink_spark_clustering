package org.service;

import org.apache.hive.beeline.BeeLine;

import java.io.IOException;

public class MyBeeline {

    public static void main(String[] args) throws IOException {
        String[] beelineArgs = new String[]{"-u", "jdbc:hive2://localhost:10000"};
        BeeLine.mainWithInputRedirection(beelineArgs, System.in);
    }
}
