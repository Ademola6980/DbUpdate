
package com.adroit.data.injector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
//import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

public class DatabaseUpdate{
	
	static String TABLENAME ="ic4pro_product5";
	static String RECORDID ="PR0012";
	static String RECORD="ic4pro_001|Compliance_Hub|20202508";
	
	final static Logger logger = Logger.getLogger(DatabaseUpdate.class);
	
//     private static final String DB_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
       public static void main(String[] argv) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
//    	   if(argv.length != 0)
//           TABLENAME = argv[0];
//             if(argv.length != 1)
//    	   COLUMN = argv[1];
////          if(argv.length != 2)   	 
//    	   RECORD = argv[2];
//    	   
          //Pass the required arguments
		updateRecordToTable(TABLENAME,RECORDID, RECORD);

        }
       

        private static boolean updateRecordToTable(String tableName, String column, String record) throws ClassNotFoundException, InstantiationException, IllegalAccessException, FileNotFoundException, IOException {

              Connection dbConnection = null;
              PreparedStatement preparedStatement = null;
              
              //String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
              
              Date today = new Date();
              SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd-MM-yy:HH:mm");
              String date = DATE_FORMAT.format(today);
               

          try {
                   //dbConnection = getDBConnection();
                   File file = new File("C:\\DBConnection\\dbConnection.txt");
                   
                 BufferedReader br = new BufferedReader(new FileReader(file)); 
                 String sCurrentLine;
                 String bdconnection= null;
         
                    sCurrentLine = br.readLine();
                       System.out.println(sCurrentLine);
                       bdconnection=sCurrentLine;
                    
     
                   //
                    //dbDriver
                      File file2 = new File("C:\\DBConnection\\dbDriver.txt");
                   
                 BufferedReader br2 = new BufferedReader(new FileReader(file2)); 
                 String sCurrentLine2;
                 String bdDriver= null;
         
                    sCurrentLine2 = br2.readLine();
                       System.out.println(sCurrentLine2);
                       bdDriver=sCurrentLine2;
                   

//        String connectionUrl = "jdbc:sqlserver://localhost:1433;databaseName=Radar;user=adroit;password=Money123";

        String connectionUrl = bdconnection;

            // Load SQL Server JDBC driver and establish connection.
            System.out.print("Connecting to SQL Server ... ");
          Class.forName(bdDriver);
                dbConnection = DriverManager.getConnection(connectionUrl); 
            
                System.out.println("Done.");

                
                
            
      
                   
                   
//                   //
//                   for (int l = 0; l < record.split("|").length; l++) {
//                                      
//                                     System.out.println("Record after splitting:"+record.split("]")[l]+ ",");
//                                 }
                   
                   
                   
                   
                   //Checks if table exists
                   if(Table.tableExist(dbConnection, tableName))
        		   {
                               
                               String sql = "SELECT * FROM "+tableName+" WHERE record_id='"+RECORDID+"'";
                                Statement statement = dbConnection.createStatement();
                                ResultSet result = statement.executeQuery(sql);
                                if(result.next())
                                {
                                 ResultSetMetaData md = result.getMetaData();
                                int col = md.getColumnCount();
                                System.out.println("Number of Column : "+ col);
                                System.out.println("Columns Name: ");
                                StringBuffer sBuffer = new StringBuffer("UPDATE " + tableName );
                                 sBuffer.append(" SET");
                                for (int i = 1; i <= col; i++){
                                String col_name = md.getColumnName(i);
                                sBuffer.append(""+col_name+""+ "=?,");
                                         
                                     }
                                     sBuffer.append("record_id=?");
                                    sBuffer.append("WHERE record_id=?");
                                     String singleString = sBuffer.toString();
                                    PreparedStatement state = dbConnection.prepareStatement(singleString);
                                     for(int k=1;k<record.split("[|]").length;k++)
                                     {
                                       state.setString(k, record.split("[|]")[k]);
                                     
                                     }
                                     int rowsUpdated = state.executeUpdate();
                                        if (rowsUpdated > 0) {
                                            System.out.println("An existing record was updated successfully!");
                                        }

                                       

                                }
                               else
                                {
                	   StringBuffer sBuffer = new StringBuffer("INSERT INTO " + tableName );
                	   ArrayList<String> columnNames = Table.tokenizeValue(column);   
                	   ArrayList<String> records = Table.tokenizeValue(record);
                	   
                	   int i = 1;
                	   int j = 1;
                	   int k = 1;
//	                  	 for (String temp : columnNames) 
//	                  	 {
//	                  		 if (i < columnNames.size())
//	                  		 {
//	                  			 sBuffer.append(temp+ ", ");
//	                  		 }
//	                  		 else
//	                  		 {
//	                  			 sBuffer.append(temp+ ") VALUES (");
//	                  		 }
//	                  		 i++;
//	               		}
//	                  	 for (int l = 0; l < record.split("|").length; l++) {
//                                      sBuffer.append(record.split("|")[l]+ ",");
//                                     
//                                 }
                                    
                                    sBuffer.append(" VALUES (");
                                  for (String id : record.split("[|]")) {
                                         System.out.println("Now spliting:"+id);
                                       sBuffer.append("'"+id+"'"+ ",");
                                   System.out.println("Then spliting:"+"'"+id+"'"+ ",");
                                       
                                   }
                                  sBuffer.append("'"+RECORDID+"' "+ ")");
                                  System.out.println("Final:"+"'id'"+ ")");
                                  //sBuffer.append(")");
                                  
                                  
	                  	 
//	                  	 for (@SuppressWarnings("unused") String temp : records) 
//	                  	 {
//	                  		 if (j < records.size())
//	                  		 {
//	                  			 sBuffer.append("?"+ ",");
//	                  		 }
//	                  		 else
//	                  		 {
//	                  			 sBuffer.append("?"+ ")");
//	                  		 }
//	                  		 j++;
//	                  		 
//	                  		
//	               		}
	                  	 //System.out.println(sBuffer.toString()); 
	                  	preparedStatement = dbConnection.prepareStatement(sBuffer.toString());
	                  	
//	                  	 for (String temp : records) 
//	                  	 {
//	                  		preparedStatement.setString(k, temp);
//	                  		k++;
//	                  	 }
	                  	preparedStatement.executeUpdate();
                                }
	                   return true;
        		   }
                   else//Create Table
                   {
                	 StringBuffer sBuffer = new StringBuffer("CREATE TABLE " + tableName + " (" +
                			 " [ID] [int] primary key IDENTITY(1,1) NOT NULL, ");
                	 ArrayList<String> tokenizedValues = Table.tokenizeValue(column);
                	 int i = 1;
//                	 for (String temp : tokenizedValues) 
//                	 {
//                		 if (i < tokenizedValues.size())
//                		 {
//                			 sBuffer.append(temp+ " VARCHAR(255), ");
//                		 }
//                		 else
//                		 {
//                			 sBuffer.append(temp+ " VARCHAR(255)) ");
//                		 }
//                		 i++;
//             		}
//                         
//                         
//                                for (String id : record.split("[|]")) {
//                                         System.out.println("Now spliting:"+id);
//                                       sBuffer.append("'"+id+"'"+ ",");
//                                   System.out.println("Then spliting:"+"'"+id+"'"+ ",");
//                                   
//                                  
//                                       
//                                   }
                                for(int k=0;k<record.split("[|]").length;k++)
                                {
                                      sBuffer.append("NEW_FLD"+k+"" + " VARCHAR(255), ");
                                }
                                 sBuffer.append("record_id"+ " VARCHAR(255)) ");
                	 
                	 String sql = sBuffer.toString();
                	 createTable(dbConnection, sql);
                	// updateRecordToTable(TABLENAME, COLUMN, RECORD);
                        updateRecordToTable(TABLENAME,RECORDID, RECORD);
     
                   }

          } catch (SQLException e) {

                 //System.out.println( e.getMessage());
                 logger.error("ERROR", e);
                 return false;

               } finally {

                 if (preparedStatement != null) {
                          try {
							preparedStatement.close();
						} catch (SQLException e) {
							
							//e.printStackTrace();
							 logger.error("ERROR", e);
							 return false;
						}
                 }

                 if (dbConnection != null) {
                               try {
								dbConnection.close();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								//e.printStackTrace();
								logger.error("ERROR", e);
								return false;
							}
                   }

           }
		return true;

   }

   private static Connection getDBConnection() {

               Connection dbConnection = null;

             
                try {
                	
                   dbConnection = java.sql.DriverManager.getConnection(ReadProperty.loadTAFJProperty("database.properties"));
                	
                	return dbConnection;

              } catch (SQLException e) {

                     //System.out.println(e.getMessage());
            	  logger.error("ERROR", e);

           }

           return dbConnection;

  }
   
	 private static void createTable(Connection conn, String sql)
	 {
		 Statement stmt;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	      
	 }

}

