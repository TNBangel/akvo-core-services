package org.akvo.flow.reporting.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Servlet implementation class ListReports
 */
@WebServlet(
		description = "Output a list of defined reports", 
		urlPatterns = { 
				"/ListReports", 
				"/list_reports"
		})
public class ListReports extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public ListReports() {
        super();
    }

    
    void error(HttpServletResponse response) throws IOException {
        // Set response content type
        response.setStatus(400);
        response.setContentType("text/plain");
        PrintWriter out = response.getWriter();
        out.println("You must provide an integer value for the form_id parameter");
    }
    
    void getAsHtml(HttpServletResponse response, ResultSet rs, int id) throws IOException, SQLException {
        // Set response content type
        response.setContentType("text/html");//TODO make a constant
        PrintWriter out = response.getWriter();
        String title = "Reports defined for form " + id + " in the Database";
        String docType = "<!doctype html public \"-//w3c//dtd html 4.0 "
                + "transitional//en\">\n";
        out.println(docType + "<html>\n" + "<head><title>" + title
                + "</title></head>\n" + "<body bgcolor=\"#f0f0f0\">\n"
                + "<h1 align=\"center\">" + title + "</h1>\n"); 
        
        // Extract data from result set
        while (rs.next()) {
            // Retrieve by column name
            int rid = rs.getInt("id");
            String par = rs.getString("parameters");
            String template = rs.getString("template");//hundreds of kB. Remove from resultset after debugging
            Date d = rs.getDate("created");
            
            // Display values
            out.print("[" + rid + "] (" + par + " ) " + template.length() + " bytes, created " + d);
            out.println("<br>");
        }
        out.println("</body></html>");
    }
    
    //JSON jar was fetched from Maven Central as described in https://github.com/eskatos/org.json-java
    
    void getAsJson(HttpServletResponse response, ResultSet rs) throws IOException, SQLException {
        //build JSON structure
        JSONObject root = new JSONObject();
        JSONArray reports = new JSONArray();
        root.put("reports", reports);

        int lastSurveyId = -1;
        while (rs.next()) {
            JSONObject report = new JSONObject();
            report.put("id", rs.getInt("id"));
            report.put("parameters", rs.getString("parameters"));
            report.put("created", rs.getDate("created"));
            reports.put(report);
        }
    
        
        
        
        // Set response content type
        response.setContentType("application/json");//TODO make a constant
        PrintWriter out = response.getWriter();

        
        
        out.print(root.toString());//??
    }
    
  
    public static Map<String, String> getQueryMap(String query)
    {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<String, String>();
        for (String param : params)
        {
            String name = param.split("=")[0].toLowerCase();
            String value = param.split("=")[1];
            map.put(name, value);
        }
        return map;
    }
    
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	    
	    //form id parameter
	    int form_id = 0;
	    Map<String, String> params;
	    try {
	        params = getQueryMap(request.getQueryString());
	        String form_id_str = params.get("form_id");
	        form_id = Integer.parseInt(form_id_str);
	    }
	    catch (Exception e) {
	        error(response);
	        return;
	    }

        // JDBC driver name and database URL
        final String JDBC_DRIVER = "org.postgresql.Driver"; //
        final String DB_URL = "jdbc:postgresql://localhost:1234/flowtestrep"; //

        // Database credentials
        final String USER = "flowtestrep";
        final String PASS = "snippsnappsnurr";


        // Register JDBC driver
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }

        // Open a connection
        Connection conn = null;
        Statement stmt = null;
        
        try {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            // Execute SQL query
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT * FROM birt_template WHERE form_id = " + form_id;
            ResultSet rs = stmt.executeQuery(sql);

            //pick output format
            if (params.get("format") != null && params.get("format").equalsIgnoreCase("json")) {
                getAsJson(response, rs);
            } else {
                getAsHtml(response, rs, form_id);
            }
            
            // Clean-up environment
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            // Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            // Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            // finally block used to close resources
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }// nothing we can do
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }// end finally try
        } // end try
    
	}

}
