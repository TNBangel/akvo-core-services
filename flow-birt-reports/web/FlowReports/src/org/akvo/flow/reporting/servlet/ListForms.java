package org.akvo.flow.reporting.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Servlet implementation class ListForms
 */
@WebServlet({ "/ListForms", "/list_forms" })
public class ListForms extends HttpServlet {
    private static final long serialVersionUID = 1L;

    /**
     * @see HttpServlet#HttpServlet()
     */
    public ListForms() {
        super();
        // TODO Auto-generated constructor stub
    }

    
    
    void getAsHtml(HttpServletResponse response, ResultSet rs) throws IOException, SQLException {
        // Set response content type
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        String title = "Forms in the Database";
        String docType = "<!doctype html public \"-//w3c//dtd html 4.0 "
                + "transitional//en\">\n";
        out.println(docType + "<html>\n" + "<head><title>" + title
                + "</title></head>\n" + "<body bgcolor=\"#f0f0f0\">\n"
                + "<h1 align=\"center\">" + title + "</h1>\n"); 
        
        // Extract data from result set
        while (rs.next()) {
            // Retrieve by column name
            int sid = rs.getInt("survey_pk");
            String sname = rs.getString("survey_text");
            int fid = rs.getInt("form_pk");
            String fname = rs.getString("form_text");

            // Display values
            out.print("[" + sid + "] ");
            out.print(sname);
            out.print("[" + fid + "] ");
            out.println(fname + "<br>");
        }
        out.println("</body></html>");
    }
    
    //JSON jar was fetched from Maven Central as described in https://github.com/eskatos/org.json-java
    
    void getAsJson(HttpServletResponse response, ResultSet rs) throws IOException, SQLException {
        //build JSON structure
        JSONObject root = new JSONObject();
        JSONArray surveys = new JSONArray();
        root.put("surveys", surveys);

        int lastSurveyId = -1;
        while (rs.next()) {
            JSONArray forms = null;

            int sid = rs.getInt("survey_pk");
            //see if new survey
            if (sid != lastSurveyId) {
                JSONObject survey = new JSONObject();
                survey.put("id", sid);
                survey.put("text", rs.getString("survey_text"));
                forms = new JSONArray();
                survey.put("forms", forms);
                surveys.put(survey);
                lastSurveyId = sid;
            }
            JSONObject form = new JSONObject();
            form.put("id", rs.getInt("form_pk"));
            form.put("text", rs.getString("form_text"));
            if (forms != null) forms.put(form);
            
        }
        
        
        
        // Set response content type
        response.setContentType("application/json");//TODO make a constant
        PrintWriter out = response.getWriter();

        
        
        out.print(root.toString());//??
    }
    
    
    
    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
     *      response)
     */
    protected void doGet(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        // JDBC driver name and database URL
        final String JDBC_DRIVER = "org.postgresql.Driver"; //
//        final String DB_URL = "jdbc:postgresql://localhost:1234/flowtestrep"; //
        final String DB_URL = "jdbc:postgresql://localhost:5432/flowtestrep"; //

        // Database credentials
        final String USER = "flowtestrep";
//        final String PASS = "snippsnappsnurr";
        final String PASS = "pertsetwolf";


        // Register JDBC driver
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e1) {
            // TODO Auto-generated catch block
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
            sql = "SELECT survey.id as survey_pk, survey.display_text as survey_text, form.id as form_pk, form.display_text as form_text"+
            " FROM survey left join form on form.survey_id = survey.id";
            ResultSet rs = stmt.executeQuery(sql);

            //pick output format
            if (request.getQueryString() != null && request.getQueryString().toLowerCase().contains("json")) {
                getAsJson(response, rs);
            } else {
                getAsHtml(response, rs);
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
