//package SQLCertify;

import java.sql.*;
import java.util.*;
//import java.io.*;

public class ConnectionTest {

	public Connection connection() {
		String jdbcClassName = "com.vertica.jdbc.Driver";
		Connection conn = null;

		try {
			Class.forName(jdbcClassName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		//Properties properties = new Properties();
		//try {
		//	properties.load(new FileInputStream("readintest.txt"));
		//} catch (Exception e) {
		//	e.printStackTrace();
		//}

		Properties myProp = new Properties();
		myProp.put("user", "group6");
		myProp.put("password", "123456");

		try {
			conn=DriverManager.getConnection(
					"jdbc:vertica://129.7.243.246:5433/group6", myProp);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return conn;
	}
}

/*



CREATE TABLE TestR(
K1 int,
K2 int,
A int,
B int
); 

CREATE TABLE TestS
(K1 int, K2 int, A int,B int); 


CREATE TABLE TestT
(K1 int,K2 int,A int)



*/
