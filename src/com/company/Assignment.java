package com.company;
import oracle.jdbc.OracleTypes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Assignment {
    private String connection;
    private String username;
    private String password;
    private Connection connectionDB;
    private Map<String,Integer> movieDictionary;

    // 1.2.1 Constructor
    public Assignment(String connection, String username, String password) {
        this.connection = connection;
        this.username = username;
        this.password = password;
        this.connectionDB = null;
        connectDB();
    }

    private boolean connectDB(){

        try{
            if(this.connectionDB == null){
                Class.forName("oracle.jdbc.driver.OracleDriver");
                this.connectionDB = DriverManager.getConnection(this.connection,this.username,this.password);
                return true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    private boolean disconnect(){
        try {
            this.connectionDB.close();
            return true;
        }catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    // 1.2.2  file to data base function

    public void fileToDataBase(String filePath){
        String lineFile = "";
        this.movieDictionary  = new LinkedHashMap<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            while((lineFile = br.readLine())!= null ){
                String[] movieInfo = lineFile.split(",");
                this.movieDictionary.put(movieInfo[0],Integer.parseInt(movieInfo[1]));
            }
            addMoviesToDB();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean addMoviesToDB(){
        connectDB();
        PreparedStatement ps = null;
        for (Map.Entry<String,Integer> movie : this.movieDictionary.entrySet()){
            try{
                //check if the query work you need to delete it!!!
                //String query = "INSERT INTO MediaItems(TITLE,PROD_YEAR) VALUES(?,?)";
                ps = connectionDB.prepareStatement("INSERT INTO MediaItems(TITLE,PROD_YEAR) VALUES(?,?)");
                ps.setString(1,movie.getKey());
                ps.setInt(2,movie.getValue());
                ps.executeUpdate();
                connectionDB.commit();
            }catch (SQLException sqle){
                sqle.printStackTrace();
            }finally {
                try{
                    if(ps != null){
                        ps.close();
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        if(connectionDB != null)
            disconnect();

        return true;
    }

    // 1.2.3 calculate similarity function

    public boolean calculateSimilarity(){

        List<Integer> midList = getAllMID();
        Integer maximalDis = getMaximalDis();

        //check the for if it work

        for (int i = 0; i <midList.size()-1 ; i++) {
            for (int j = i+1; j < midList.size() ; j++) {
                Double similarityCalc =  getSimilarityCalc(i,j,maximalDis);
                addSimToDB(i,j,similarityCalc);
            }
        }
        return true;
    }

    private List<Integer> getAllMID(){
        connectDB();
        List<Integer> midList = new ArrayList<>();
        PreparedStatement ps = null;
        try{
            //check if the query work you need to delete it!!!
            //String query = "SELECT MID from MEDIAITEMS";
            ps = connectionDB.prepareStatement("SELECT MID from MEDIAITEMS");
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                //check if this work !!!!
                midList.add(Math.toIntExact(rs.getLong("MID"))) ;
            }
            rs.close();
            connectionDB.commit();
        }catch (SQLException sqle){
            sqle.printStackTrace();
        }finally {
            try{
                if (ps != null)
                    ps.close();
                disconnect();
            }catch (SQLException sqle1){
                sqle1.printStackTrace();
            }
        }
        return midList;

    }

    private Integer getMaximalDis(){
        connectDB();

        Integer maximalDis = null;
        CallableStatement cs = null;
        try{
            // Need to check this query !!!
            cs = connectionDB.prepareCall("{? = call MAXIMALDISTANCE()}");
            cs.registerOutParameter(1,OracleTypes.NUMBER);
            cs.execute();
            maximalDis = cs.getInt(1);
        }catch (SQLException sqle){
            sqle.printStackTrace();
        }finally {
            try{
                if (cs != null)
                    cs.close();
                disconnect();
            }catch (SQLException sqle1){
                sqle1.printStackTrace();
            }
        }
        return maximalDis;
    }

    private Double getSimilarityCalc(int mid1, int mid2, Integer maximalDis){
        connectDB();
        Double similarityCalc = null ;
        CallableStatement cs = null;
        try{
            // Need to check this query !!!
            cs = connectionDB.prepareCall("{? = call SIMCALCULATION(?,?,?)}");
            cs.setLong(4,maximalDis);
            cs.setLong(3,mid2);
            cs.setLong(2,mid1);
            cs.registerOutParameter(1,OracleTypes.FLOAT);
            cs.execute();
            similarityCalc = cs.getDouble(1);
        }catch (SQLException sqle){
            sqle.printStackTrace();
        }finally {
            try{
                if (cs != null)
                    cs.close();
                disconnect();
            }catch (SQLException sqle1){
                sqle1.printStackTrace();
            }
        }
        return similarityCalc;
    }

    private void addSimToDB(int mid1,int mid2,Double similarityCalc){
        connectDB();
        PreparedStatement ps = null;
        try{
            //check if the query work you need to delete it!!!
            //String query = "INSERT INTO SIMILARITY VALUES(?,?,?)";
            ps = connectionDB.prepareStatement("INSERT INTO SIMILARITY VALUES(?,?,?)");
            ps.setDouble(3,similarityCalc);
            ps.setLong(1,mid1);
            ps.setLong(2,mid2);
            ps.executeUpdate();
            connectionDB.commit();
        }catch (SQLException sqle){
            sqle.printStackTrace();
        }finally {
            try{
                if(ps != null)
                    ps.close();
                disconnect();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    //1.2.4 print  similar items
   public void printSimilarityItems(Long mid){
        Map<String,Double> simMidMap = getSimMidMap(mid);
       for(Map.Entry<String,Double> otherMid : simMidMap.entrySet()){
           if(otherMid.getValue()>=0.3)
               System.out.println("Title: " + otherMid.getKey() + " Similarity Value: " + otherMid.getValue() +"\n");
       }
   }

    private Map<String,Double> getSimMidMap(Long mid) {
        connectDB();
        Map<String,Double> simMidMap = new LinkedHashMap<>();
        PreparedStatement ps = null;
        try{
            //check if query works
            ps = connectionDB.prepareStatement("SELECT MEDIAITEMS.TITLE AS TITLE, SIMILARITY.MID2, SIMILARITY.SIMILARITY AS SIM FROM SIMILARITY INNER JOIN MEDIAITEMS on SIMILARITY.MID2 = MEDIAITEMS.MID WHERE MID1=? ORDER BY SIMILARITY ASC");
            ps.setLong(1,mid);
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                simMidMap.put(rs.getString("TITLE"),rs.getDouble("SIM"));
            }
            rs.close();
        }catch (SQLException sqle){
            sqle.printStackTrace();
        }finally {
            try{
                if(ps != null)
                    ps.close();
                disconnect();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return simMidMap;
    }

}
