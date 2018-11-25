package com.company;

public class Main {

    public static void main(String[] args) {
        String connection = "jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/oracle";
        String username = "sakazi";
        String password = "abcd";
        Assignment ass = new Assignment(connection,username,password);
        String filePath ="C:\\Users\\Idosakazi\\IdeaProjects\\Ex3\\src\\films.csv";
        //
        // ass.fileToDataBase(filePath);
        //ass.calculateSimilarity();
        //ass.printSimilarityItems((long)0);
    }
}
