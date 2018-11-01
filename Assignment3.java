/*
Owen Sheets
CSCI 330
Assignment 3

Project connects to sheetso and johnson330 databases
Reads info from johnson330 and does calculation to determine industry to invest in
Inputs this data into sheetso database table "Performance"

GIVE ME AN 'A' Cody ;D
(just joking around)
*/

import java.util.Properties;
import java.util.Scanner;
import java.io.FileInputStream;
import java.sql.*;
import java.util.*;
import java.text.DecimalFormat;

class Assignment3{
  static Connection conn = null;
  static Connection connOwen = null;

  public static void main(String[] args) throws Exception {
    //Connect to sheetso Database
    String paramsFileOwen = "WriterParams.txt";
    if(args.length >= 1){
      paramsFileOwen = args[0];
    }
    Properties connectpropsOwen = new Properties();
    connectpropsOwen.load(new FileInputStream(paramsFileOwen));
    try{
      Class.forName("com.mysql.jdbc.Driver");
      String dburlOwen = connectpropsOwen.getProperty("dburl");
      String usernameOwen = connectpropsOwen.getProperty("user");
      connOwen = DriverManager.getConnection(dburlOwen, connectpropsOwen);
      System.out.printf("Database connection %s %s established. %n", dburlOwen, usernameOwen);

      PreparedStatement dropTable = connOwen.prepareStatement("drop table if exists Performance;");
      dropTable.executeUpdate();

      PreparedStatement createTable = connOwen.prepareStatement("create table Performance(Industry char(30), Ticker char(6), StartDate char(10), EndDate char(10), TickerReturn char(12), IndustryReturn char(12));");
      createTable.executeUpdate();

    }catch(SQLException ex){
      System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
      ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
    }

    //Connect to johnson330 Database
    String paramsFile = "ConnectionParameters.txt";
    if (args.length >= 1) {
      paramsFile = args[0];
    }
    Properties connectprops = new Properties();
    connectprops.load(new FileInputStream(paramsFile));

    try{
      Class.forName("com.mysql.jdbc.Driver");
      String dburl = connectprops.getProperty("dburl");
      String username = connectprops.getProperty("user");
      conn = DriverManager.getConnection(dburl, connectprops);
      System.out.printf("Database connection %s %s established. %n", dburl, username);

      ArrayList<String> industryGroup = new ArrayList<String>();
      ResultSet rs5 = showIndustryGroups();  // Getting all of the industries and saving them
      while(rs5.next()){
        industryGroup.add(rs5.getString("Industry"));
      }

      for(int index = 0; index < industryGroup.size(); index++){ // Loop to go through each industry
        //Arrays to hold all required info about industries
        ArrayList<String> tickers = new ArrayList<String>();
        ArrayList<String> transDates = new ArrayList<String>();
        ArrayList<String> interval = new ArrayList<String>();
        ArrayList<Double> result = new ArrayList<Double>();
        ArrayList<Double> tickerReturn = new ArrayList<Double>();
        ArrayList<Double> industryReturn = new ArrayList<Double>();
        ArrayList<String> tickerNames = new ArrayList<String>();
        ArrayList<String> startDates = new ArrayList<String>();
        ArrayList<String> endDates = new ArrayList<String>();

        String industryTemp = industryGroup.get(index);

        ResultSet rs1 = showIndustryRange(industryTemp);   // Finds range of date for given industry
        rs1.next();

        String min = rs1.getString("max");
        String max = rs1.getString("min");

        ResultSet rs2 = showIndustryDates(industryTemp, min, max);    // Gets data for given dates
        while(rs2.next()){
          tickers.add(rs2.getString("Ticker"));
        }

        // Calculate intervals for given industry
        if(tickers.size() > 1){
          ResultSet rs3 = showIndustryDay(tickers.get(0), min, max);
          while(rs3.next()){
            transDates.add(rs3.getString("TransDate"));
          }
          int index2 = 0;
          int count= 0;
          while(index2 < transDates.size()){
            if((index2 + 60) < transDates.size()){
              interval.add(transDates.get(index2));
              count++;
            }
            index2 = index2 + 60;
          }
          interval.add(transDates.get(index2-61));

          for(int i = 0; i < interval.size() - 1; i++){ // Do analysis on industry
            for(int j = 0; j < tickers.size(); j++){
              result.add(stockSplit(tickers.get(j), min, interval.get(i), interval.get(i+1)));
              tickerReturn.add(stockSplit(tickers.get(j), min, interval.get(i), interval.get(i+1)));
              tickerNames.add(tickers.get(j));
              startDates.add(interval.get(i));
              if((result.size()%(tickers.size())) == 0){
                double industry = 0.0;
                for(int k = 0; k < result.size(); k ++){
                  industry = industry + result.get(k);
                }
                for(int l = 0; l < result.size(); l ++){
                  double temp = 0.0;
                  temp = industry - result.get(l);
                  temp = temp + (tickers.size()-1);
                  temp = temp / (tickers.size()-1);
                  temp = temp - 1;
                  industryReturn.add(temp);
                }
                result.clear();
              }
            }
          }
          for(int i = 0; i < interval.size() -1; i++){
            for(int j = 0; j < tickers.size(); j++){
              ResultSet rs4 = showIndustryDaySorted(tickers.get(j), interval.get(i), interval.get(i+1));
              ArrayList<String> tradingDays = new ArrayList<String>();
              while(rs4.next()){
                tradingDays.add(rs4.getString("TransDate"));
              }
              endDates.add(tradingDays.get(tradingDays.size()-2));
            }
          }

          System.out.printf("%.7f %.7f %n", tickerReturn.get(0), industryReturn.get(0));
          for(int i = 0; i < tickerReturn.size(); i++){  //Insert adjusted data into database
            PreparedStatement tableInsert = connOwen.prepareStatement("insert into Performance(Industry, Ticker, StartDate, EndDate, TickerReturn, IndustryReturn) values(?,?,?,?,?,?);");
            tableInsert.setString(1, industryGroup.get(index));
            tableInsert.setString(2, tickerNames.get(i));
            tableInsert.setString(3, startDates.get(i));
            tableInsert.setString(4, endDates.get(i));
            DecimalFormat df = new DecimalFormat("#.#######");
            tableInsert.setString(5, df.format(tickerReturn.get(i)));
            tableInsert.setString(6, df.format(industryReturn.get(i)));
            tableInsert.executeUpdate();
          }
        }
      }
    }catch(SQLException ex){
      System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
      ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
    }
  }

  static double stockSplit(String ticker, String minimum, String Sdate, String Sdate1) throws SQLException {
    //Arrays to hold all of the required info
    ArrayList<String> transactions = new ArrayList<String>();
    ArrayList<Double> openPrice = new ArrayList<Double>();
    ArrayList<Double> openAdjusted = new ArrayList<Double>();
    ArrayList<Double> closePrice = new ArrayList<Double>();
    ArrayList<Double> closeAdjusted = new ArrayList<Double>();

    ResultSet rs = showTickerSales(ticker, minimum, Sdate1);
    int index = 0;
    Double divisor = 1.0;
    while (rs.next()) {
      //insert rows from result set into arraylists
      transactions.add(rs.getString(2));
      openPrice.add(rs.getDouble(3));
      openAdjusted.add(rs.getDouble(3));
      closePrice.add(rs.getDouble(4));
      closeAdjusted.add(rs.getDouble(4));

      if(index > 0){ // Stock split has occured
        if(rs.getDouble(4) != openPrice.get(index-1)){
          if(Math.abs(rs.getDouble(4)/openPrice.get(index-1) - 2.0) < 0.20){
            divisor = divisor * 2.0;
          }else if(Math.abs(rs.getDouble(4)/openPrice.get(index-1) - 3.0) < 0.30){
            divisor = divisor * 3.0;
          }else if(Math.abs(rs.getDouble(4)/openPrice.get(index-1) - 1.5) < 0.15){
            divisor = divisor * 1.5;
          }
        }
        if(divisor > 1.0){
          openAdjusted.set(index, rs.getDouble(3)/divisor);
          closeAdjusted.set(index, rs.getDouble(4)/divisor);
        }
      }
      index++;
    }
    double open = 0;
    double close = 0;
    for(int i =  transactions.size() - 1; i >= 0; i --){
      if(transactions.get(i).equals(Sdate)){
        open = openAdjusted.get(i);
      }
      else if(transactions.get(i).equals(Sdate1)){
        close = closeAdjusted.get(i+1);
      }
    }
    double result = 0.0;
    result = (close/open) - 1;
    return result;
  }

  static ResultSet showTickerSales(String ticker, String minimum, String Sdate1) throws SQLException { // Query to find a range of sales data associated with ticker
    ResultSet rs;
    PreparedStatement pstmt = conn.prepareStatement("select Ticker, TransDate, OpenPrice, ClosePrice " +
    " from PriceVolume " +
    " where Ticker = ? and TransDate >= ? and TransDate <= ? " +
    " order by TransDate DESC");
    pstmt.setString(1, ticker);
    pstmt.setString(2, minimum);
    pstmt.setString(3, Sdate1);

    rs = pstmt.executeQuery();
    return rs;
  }

  static ResultSet showIndustryGroups() throws SQLException{ // Query to show all industrys
    ResultSet rs;
    PreparedStatement pstmt = conn.prepareStatement("select Industry " +
    " from Company " +
    "  group by Industry " +
    " order by Industry;");

    rs = pstmt.executeQuery();
    return rs;
  }

  static ResultSet showIndustryRange(String industry) throws SQLException{ //Gives range of dates for a given industry
    ResultSet rs;
    PreparedStatement pstmt = conn.prepareStatement(
    " select max(minTransDate) as max, min(maxTransDate) as min " +
    " from (select Ticker, min(TransDate) as minTransDate, max(TransDate) as maxTransDate, count(distinct TransDate) as TradingDays " +
    " from Company natural join PriceVolume " +
    " where Industry = ? " +
    " group by Ticker " +
    " having TradingDays >= 150 " +
    " order by Ticker) as alias;");
    pstmt.setString(1, industry);

    rs = pstmt.executeQuery();
    return rs;
  }

  static ResultSet showIndustryDates(String industry, String min, String max) throws SQLException{ // Gts the number of trading days in an interval
    ResultSet rs;
    PreparedStatement pstmt = conn.prepareStatement(
    " select Ticker, count(distinct TransDate) as TradingDays " +
    " from Company natural join PriceVolume " +
    " where Industry = ? and TransDate >= ? and TransDate <= ? " +
    " group by Ticker " +
    " having TradingDays >= 150 " +
    " order by Ticker;");
    pstmt.setString(1, industry);
    pstmt.setString(2, min);
    pstmt.setString(3, max);

    rs = pstmt.executeQuery();
    return rs;
  }

  static ResultSet showIndustryDay(String ticker, String min, String max) throws SQLException{ //Query to get transactions between range
    ResultSet rs;
    PreparedStatement pstmt = conn.prepareStatement(
    " select TransDate " +
    " from PriceVolume " +
    " where Ticker = ? and TransDate >= ? and TransDate <= ?");
    pstmt.setString(1, ticker);
    pstmt.setString(2, min);
    pstmt.setString(3, max);

    rs = pstmt.executeQuery();
    return rs;
  }

  static ResultSet showIndustryDaySorted(String ticker, String min, String max) throws SQLException{ // Query to get SORTED transactions in range
    ResultSet rs;
    PreparedStatement pstmt = conn.prepareStatement(
    "select TransDate " +
    " from PriceVolume " +
    " where Ticker = ? and TransDate >= ? and TransDate <= ? " +
    " order by TransDate;");
    pstmt.setString(1, ticker);
    pstmt.setString(2, min);
    pstmt.setString(3, max);

    rs = pstmt.executeQuery();
    return rs;
  }
}
