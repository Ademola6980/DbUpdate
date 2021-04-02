package com.adroit.data.injector;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class Table {
	
	
	public static boolean tableExist(Connection conn, String tableName) throws SQLException 
	{
	    boolean tExists = false;
	    try (ResultSet rs = conn.getMetaData().getTables(null, null, tableName, null)) 
	    {
	        while (rs.next()) 
	        { 
	            String tName = rs.getString("TABLE_NAME");
	            if (tName != null && tName.equals(tableName))
	            {
	                tExists = true;
	                break;
	            }
	        }
	    }
	    return tExists;
	}
	
	
	public static ArrayList<String> tokenizeValue(String value)
	{
            System.out.println("value tokenizer:"+value);
		ArrayList<String> list = new ArrayList<String>();
		
		StringTokenizer str = new StringTokenizer(value, "|");

		while (str.hasMoreElements()) 
		{
			list.add((String) str.nextElement());
		}
                   System.out.println("value Listr:"+value);
		return list;
	}
	
}
