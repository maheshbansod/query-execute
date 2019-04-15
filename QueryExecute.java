
import java.sql.*;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.FileHandler;
import java.util.logging.ConsoleHandler;
import java.util.logging.SimpleFormatter;
import java.util.Date;
import java.text.SimpleDateFormat;

public class QueryExecute {

	QueryExecute(String fname) {
		try {
			Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
			logger.setUseParentHandlers(false);
			FileHandler txthandler = new FileHandler("QueryExecute.log");
			txthandler.setFormatter(new SimpleFormatter());
			logger.addHandler(txthandler);
			logger.setLevel(Level.FINEST); //change to SEVERE for passive logging

			//construct db connection string
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new File("dbinfo.xml"));
			
			doc.getDocumentElement().normalize();

			Element psqEl = ((Element)doc.getElementsByTagName("psql").item(0));
			String username = psqEl.getElementsByTagName("username").item(0).getTextContent();
			String password = psqEl.getElementsByTagName("password").item(0).getTextContent();
			String server_addr = psqEl.getAttribute("server");
			String server_port = psqEl.getAttribute("port");
			String conn_str = "jdbc:postgresql://"+server_addr + ":"+server_port+"/mydatabase";
			String line = null;
			//connect to the database
			try {
				Class.forName("org.postgresql.Driver");
				Connection con = DriverManager.getConnection(conn_str, username, password);
				//execute queries in the file
				BufferedReader brin = new BufferedReader(new FileReader(fname));
				Date date = new Date();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
				String timestamp = sdf.format(date);
				PrintWriter out = new PrintWriter(new FileWriter("QueryExecute_"+timestamp+".json"));
				out.println("{");
				out.println("\"outputs\": [");
				int cc = 1;
				line = brin.readLine();
	
				while(line != null) {
					if(cc != 1) out.println(",");
					out.println("{");
					try {
						out.println("\t\"query\":"+"\""+line+"\",");
						Statement stat = con.createStatement();
						ResultSet rs = stat.executeQuery(line);
						ResultSetMetaData md = rs.getMetaData();
						int coln = md.getColumnCount();
						int c=1;
						out.println("\t\"output\": {");
						out.println("\t\t\"headings\": [");
						for(int i=1;i<=coln;i++) {
							out.print("\t\t\t\""+md.getColumnName(i)+'"');
							if(i != coln) out.println(",");
						}
						out.println("\n\t\t],");
						out.println("\t\t\"data\": [");
						while(rs.next()) {
							//out.println("rows: "+c++);
							if(c != 1) out.println(",");
							out.println("\t\t\t{\n\t\t\t\"row\": [");
							for(int i=1;i<=coln;i++) {
								out.print("\t\t\t\t\""+rs.getString(i)+'"');
								if(i != coln) out.println(",");
							}
							out.println("\n\t\t\t]}");
							c++;
						}
						out.println("]}}");
						logger.info("Executed: "+line);
					}catch(SQLException e) {
						if(line != null) {
							System.out.println("Couldn't execute query: "+line);
							logger.severe("Couldn't execute query: "+line);
						} else logger.severe(""+e);
						System.out.println(""+e);
						out.println("\"output\":null}");
					}
					line = brin.readLine();
					cc++;
				}
				//out.println("}");
				out.println("]");
				out.println("}");
				out.close();
				con.close();
			} catch(Exception e) {
				logger.severe("Exception occured: "+e);
				e.printStackTrace();
			}
		} catch(Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		if(args.length < 1) {
			System.out.println("Enter a file containing queries aas an argument");
			return;
		}

		new QueryExecute(args[0]);
	}

}
