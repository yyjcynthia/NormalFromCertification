//package SQLCertify;

import java.io.*;

public class CommandLineParse {

	public CommandLineParse() {
	}

	public String GetFileName(String[] args) throws IOException {
		String s = args[0];
		String[] strSplit = s.split("=");
		if (strSplit[0].toLowerCase().contains("database"))
			return strSplit[1];

		else {
			System.out.println("command error, please retype the command line");
			System.in.read();
		}
		return null;
	}
}
