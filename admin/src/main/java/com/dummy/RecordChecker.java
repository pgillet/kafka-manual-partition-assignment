package com.dummy;

import org.unix4j.Unix4j;
import org.unix4j.line.Line;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class RecordChecker {

    public static void main(String[] args) throws IOException {

        File file = new File(args[0]);
        int totalError = 0;
        for (int i = 0; i < 1000; i++) {
            String pattern = String.format("key = %d,", i, i);

            List<Line> lines = Unix4j.grep(pattern, file).toLineList();

            System.out.println(String.format("Record %d: found %d matches", i, lines.size()));

            if(lines.size() != 1) {
                totalError++;
                System.out.println("Error here!");
            }
        }

        System.out.println(String.format("%d missing or duplicate records", totalError));


    }


}
