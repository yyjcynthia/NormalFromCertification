//import CommandLineParse;

//import java.beans.Statement;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class CertifyNF {

	static Connection conn = null;
	static java.sql.Statement stmt;
	private static ResultSet result;
	private static ResultSet result1;
	private static String[] sql = new String[3000];
	private static int count = 0;
	private static String[] combineNonKeys;
	private static String[] combineKeys;
	public static List<String> itemlist;
	static ItemCombination combination = null;
	public static String keystogether;

	CertifyNF() {
	}

	public static void main(String[] args) throws IOException, SQLException {

		CommandLineParse commandparse = new CommandLineParse();
		String filename = commandparse.GetFileName(args);

		// parse the file name. Show exception when command line invalid.
		Scanner scanner = new Scanner(new FileInputStream(filename));

		// verify if file exist. Show exception when no such file.
		try {
			scanner = new Scanner(new FileInputStream(filename));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		ConnectionTest connect_verify = new ConnectionTest();

		// when connection to vertica sucessful:
		conn = connect_verify.connection();

		if (conn != null) {
			try {
				stmt = conn.createStatement();
			} catch (SQLException e) {
				e.printStackTrace();
			}

			
			createNFTable();

			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();

				if (line.length() == 0)
					break;

				// parse the schema line
				Map<String, List<String>> schemaMap = schemaParse(line);

				// store the table name, key, nonkey.
				int nonKey_size = schemaMap.get("Nonkeys").size();
				int key_size = schemaMap.get("Keys").size();
				String[] nonKeys = new String[nonKey_size];
				String[] candidateKey = new String[key_size];
				String table = schemaMap.get("Table").get(0).toString();

				// store candidate key together.
				keystogether = new String();

				for (int i = 0; i < key_size; i++) {
					candidateKey[i] = schemaMap.get("Keys").get(i).toString();
					if (keystogether.length() == 0)
						keystogether = keystogether + candidateKey[i];

					else
						keystogether = keystogether + "," + candidateKey[i];

				}

				for (int i = 0; i < nonKey_size; i++) {
					schemaMap.get("Nonkeys").get(i).toString();
					nonKeys[i] = schemaMap.get("Nonkeys").get(i).toString();
				}

				// storing sql.
				sql[count] = "insert into NF (Tables,Form) VALUES ('" + table
						+ "', '3NF');";

				stmt.executeUpdate(sql[count]);
				count++;
				sql[count] = "insert into NF (Tables,Form) VALUES ('" + table
						+ "', 'BCNF');";

				stmt.executeUpdate(sql[count]);
				count++;

				combination = new ItemCombination();

				// store subset keys composition
				List<List<String>> res = combination
						.StringCombination(candidateKey);
				int index = 0;
				combineKeys = new String[res.size()];

				while (index < res.size()) {

					{
						combineKeys[index] = res.get(index).toString();
						combineKeys[index] = combineKeys[index].substring(1,
								combineKeys[index].length() - 1);
						//System.out.println(combineKeys[index]);
						index++;
					}
				}

				// store subset nonkey composition
				List<List<String>> res1 = combination
						.StringCombination(nonKeys);
				int index1 = 0;
				combineNonKeys = new String[res1.size()];

				while (index1 < res1.size()) {
					{
						combineNonKeys[index1] = res1.get(index1).toString();
						combineNonKeys[index1] = combineNonKeys[index1]
								.substring(1,
										combineNonKeys[index1].length() - 1);
						index1++;
					}

				}

				// 1NF Certify: check if the candidate key is clean
				boolean nf_one = true;
				System.out.println("Start to verify table" + table);
				System.out.println("Verifying the 1NF:");
				nf_one = firstNFCertify(key_size, candidateKey, table,
						keystogether);

				if (nf_one == true) {

					// 2NF certify: check wrong: if any attribute(nonKeys)
					// partially
					// dependent
					// on any subset of Key combine(combineKeys)
					boolean nf_two = true;

					String nf_2 = "2NF";
					if (key_size > 1) {
						System.out.println("Verifying the 2NF:");

						nf_two = normalFormCertify(nonKey_size,
								combineKeys.length - 1, nonKeys, combineKeys,
								table, nf_2);
					}

					if (nf_two == true) {
						// 3NF certify: check wrong: if any attribute(nonKeys)
						// partially
						// dependent
						// on any subset of attributes(combineNonKeys)
						System.out.println("Verifying the 3NF:");

						boolean nf_three = true;
						String nf_3 = "3NF";
						nf_three = normalFormCertify(nonKey_size,
								combineNonKeys.length, nonKeys, combineNonKeys,
								table, nf_3);
						if (nf_three == true) {

							// BCNF certify: check wrong: if any
							// key(candidateKey) partially
							// dependent on any subset of
							// attributes(combineNonKeys)
							System.out.println("Verifying the BCNF:");

							String nf_4 = "BCNF";
							boolean nf_bcnf = true;

							nf_bcnf = normalFormCertify(candidateKey.length,
									combineNonKeys.length, candidateKey,
									combineNonKeys, table, nf_4);
							if (nf_bcnf == true) {
								sql[count] = "update NF set " + "Y_N = 'Y', "
										+ "Reason = '" + "'"
										+ " WHERE Tables = '" + table
										+ "' and Form = '3NF'";

								stmt.executeUpdate(sql[count]);
								count++;

								sql[count] = "update NF set " + "Y_N = 'Y', "
										+ "Reason = '" + "'"
										+ " WHERE Tables = '" + table
										+ "' and Form = 'BCNF'";
								//System.out.print(sql[count]);
								count++;

							}
						}
					}
				}				
			}
			

			System.out.println("Tables" + "\t" + "Form" + "\t" + "Y_N"
					+ "\t" + "Reason");
			
			sql[count] = "select * from NF;";
			result1 = stmt.executeQuery(sql[count]);
			count++;
			
			System.out.println("");
			while (result1.next()){
				String c1 = result1.getString("Tables");
				String c2 = result1.getString("Form");
				String c3 = result1.getString("Y_N");
				String c4 = result1.getString("Reason");
				System.out.println(c1 + "\t" + c2 + "\t"+ c3 +"\t" +c4 + "\t");
			}
			
			sql[count] = "select * from NF;";
			//System.out.print(sql[count]);
			result = stmt.executeQuery(sql[count]);
			count++;
			
			PrintWriter writerscript = new PrintWriter("NF.txt", "UTF-8");
			
			writerscript.println("Tables" + "\t" + "Form" + "\t" + "Y_N"
					+ "\t" + "Reason");

			while (result.next()) {
				String c1 = result.getString("Tables");
				String c2 = result.getString("Form");
				String c3 = result.getString("Y_N");
				String c4 = result.getString("Reason");
				writerscript.println(c1 + "\t" + c2 + "\t"+ c3 +"\t" +c4 + "\t");
			}
			writerscript.close();

			sql[count] = "drop table NF";
			System.out.println(sql[count]);
			stmt.executeUpdate(sql[count]);
			count++;
			
			PrintWriter writer = new PrintWriter("script.sql", "UTF-8");

			for (int i = 0; sql[i] != null; i++) {
				writer.println(sql[i]);
			}
			
			writer.close();
			scanner.close();
			conn.close();
			stmt.close();
		}
	}

	private static boolean normalFormCertify(int nonKey_size, int key_size,
			String[] nonKeys, String[] keys, String table, String nf_)
			throws SQLException {
		int value;
		int value1;

		for (int i = 0; i < key_size; i++) {

			sql[count] = "select count(*) from (select " + keys[i] + " from "
					+ table + " group by " + keys[i] + ") as T1;";

			//System.out.println(sql[count]);
			result1 = stmt.executeQuery(sql[count]);
			result1.next();
			value = result1.getInt(1);
			//System.out.println("value" +value);
			count++;

			for (int j = 0; j < nonKey_size; j++) {

				//System.out.println(" key " + keys[i] + " nonkey " +nonKeys[j]);
				if (!keys[i].equals(nonKeys[j])){
						if(!keys[i].contains(nonKeys[j]))
						{
							//System.out.println("check validation" + keys[i] + " "+nonKeys[j]);
				
				sql[count] = "select count(*) from (select " + keys[i] + ","
						+ nonKeys[j] + " from " + table + " group by "
						+ keys[i] + "," + nonKeys[j] + ") as T2;";
				//System.out.println(sql[count]);
				result1 = stmt.executeQuery(sql[count]);
				result1.next();
				value1 = result1.getInt(1);
				//System.out.println("value1"+value);
				count++;

				if (value == value1 && keys[i] != nonKeys[j]) {
					if (nf_ == "2NF")
						return secondNFUpdate(nonKeys, keys, table, i, j);
					if (nf_ == "3NF")

						return threeNFUpdate(nonKeys, keys, table, i, j);
					if (nf_ == "BCNF") {
						bcNFUpdate(nonKeys, keys, table, i, j);

						return false;
					}}}

				}
			}
		}

		return true;
	}

	private static void bcNFUpdate(String[] nonKeys, String[] keys,
			String table, int i, int j) throws SQLException {
		sql[count] = "update NF set " + "Y_N = 'Y', " + "Reason = '" + "'"
				+ " WHERE Tables = '" + table + "' and Form = '3NF';";

		stmt.executeUpdate(sql[count]);
		count++;

		sql[count] = "update NF set " + "Y_N = 'N', " + "Reason = '" + keys[i]
				+ "->" + nonKeys[j] + "'" + " WHERE Tables = '" + table
				+ "' and Form = 'BCNF';";
		
		stmt.executeUpdate(sql[count]);
		count++;
	}

	private static boolean threeNFUpdate(String[] nonKeys, String[] keys,
			String table, int i, int j) throws SQLException {

		sql[count] = "update NF set " + "Y_N = 'N', " + "Reason = '"
				+ keys[i] + "->" + nonKeys[j] + "'"
				+ " WHERE Tables = '" + table + "' and Form = '3NF';";

		stmt.executeUpdate(sql[count]);
		count++;

		sql[count] = "update NF set " + "Y_N = 'N', " + "Reason = '"
				+ "not 3nf " + "'" + " WHERE Tables = '" + table
				+ "' and Form = 'BCNF';";
		
		stmt.executeUpdate(sql[count]);
		count++;

		return false;
	}

	private static boolean secondNFUpdate(String[] nonKeys, String[] keys,
			String table, int i, int j) throws SQLException {
		sql[count] = "update NF set " + "Y_N = 'N', " + "Reason = '"
				+ "not 2nf, " + keys[i] + "->" + nonKeys[j] + "'"
				+ " WHERE Tables = '" + table + "' and Form = '3NF'";

		stmt.executeUpdate(sql[count]);
		count++;

		sql[count] = "update NF set " + "Y_N = 'N', " + "Reason = '"
				+ "not 3nf " + "'" + " WHERE Tables = '" + table
				+ "' and Form = 'BCNF'";

		stmt.executeUpdate(sql[count]);
		count++;

		return false;
	}

	private static boolean firstNFCertify(int key_size, String[] candidateKey,
			String table, String keystogether) throws SQLException {
		sql[count] = "select count(*) from (select distinct " + keystogether
				+ " from " + table + ") as AA;";

		result = stmt.executeQuery(sql[count]);
		result.next();
		int[] values = new int[3];
		values[0] = result.getInt(1);
		count++;

		sql[count] = "select count(*) from " + table + ";";
		result1 = stmt.executeQuery(sql[count]);
		result1.next();
		values[1] = result1.getInt(1);
		count++;

		values[2] = 0;

		for (int i = 0; i < key_size; i++) {
			sql[count] = "select count(*) " + "from " + table + " where "
					+ candidateKey[i] + " IS NULL;";
			result1 = stmt.executeQuery(sql[count]);
			result1.next();

			values[2] = values[2] + result1.getInt(1);
			count++;
		}

		if (values[0] != values[1] || values[2] > 0) {
			sql[count] = "update NF set " + "Y_N = 'N', "
					+ "Reason = 'not 1nf, candidate Key not clean' "
					+ "WHERE Tables = '" + table + "' and Form = '3NF';";
			stmt.executeUpdate(sql[count]);

			sql[count] = "update NF set " + "Y_N = 'N', "
					+ "Reason = 'not 3nf' " + "WHERE Tables = '" + table
					+ "' and Form = 'BCNF'";

			stmt.executeUpdate(sql[count]);

			return false;
		}
		return true;
	}

	private static void createNFTable() throws SQLException {
		sql[count] = "create table NF (Tables varchar(255), "
				+ "Form varchar(255), Y_N varchar(255),"
				+ "Reason varchar(255), " + "PRIMARY KEY (Tables));";

		stmt.execute(sql[count]);
		count++;
	}

	public static Map<String, List<String>> schemaParse(String line) {

		// read a string before first'('
		String tableName = line.split("\\(")[0];


		String attributeList = line.substring(tableName.length() + 1,
				line.length() - 1);

		String[] attribute = attributeList.split(",");

		int numOfAttributes = attributeList.split(",").length;

		// Creating map to store the attributes and two lists for key and
		// non-key attributes
		Map<String, List<String>> map = new HashMap<String, List<String>>();
		List<String> keys = new ArrayList<String>();
		List<String> nonkeys = new ArrayList<String>();
		List<String> table = new ArrayList<String>();

		table.add(tableName);

		int key_count = 0;
		int nonkey_count = 0;

		for (int i = 0; i < numOfAttributes; i++) {
			if (attribute[i].contains("(k)")) {
				String keyValue = attribute[i].split("\\(")[0];
				keys.add(key_count, keyValue);
				key_count++;
			} else {
				String non_keyValue = attribute[i].trim();
				nonkeys.add(nonkey_count, non_keyValue);
				nonkey_count++;
			}
		}
		// Adding values to map
		map.put("Keys", keys);
		map.put("Nonkeys", nonkeys);
		map.put("Table", table);

		return map;
	}

}
