package org.akvo.flow.reporting.servlet;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.postgresql.util.PGobject;

/**
 * Servlet implementation class ListReports
 */
@WebServlet(
		description = "Output a list of defined reports", 
		urlPatterns = { 
				"/CreateReport", 
				"/create_report"
		})
public class CreateReport extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public CreateReport() {
        super();
    }

    
    public void save(int count) throws Exception {
        FileWriter fileWriter = null;
        PrintWriter printWriter = null;
        fileWriter = new FileWriter("FileCounter.initial");
        printWriter = new PrintWriter(fileWriter);
        printWriter.println(count);

        // make sure to close the file
        if (printWriter != null) {
          printWriter.close();
        }
      }
    
    public void save(String text, int id) throws Exception {
        FileWriter fileWriter = null;
        PrintWriter printWriter = null;
        fileWriter = new FileWriter("C:\\_Dev\\FLOWBIRT\\report_"+id+".rptdesign");
        printWriter = new PrintWriter(fileWriter);
        printWriter.print(text);

        // make sure to close the file
        if (printWriter != null) {
          printWriter.close();
        }
      }
  
    
    
    

        private Map<String, String> createContents(long form_id, ResultSet questions) throws SQLException {
        StringBuilder columnHints = new StringBuilder();
        StringBuilder resultSet = new StringBuilder();
        StringBuilder queryText = new StringBuilder();
        StringBuilder boundDataColumns = new StringBuilder();
        StringBuilder columnSet = new StringBuilder();//need no attributes, but required in table
        StringBuilder headerCells = new StringBuilder();
        StringBuilder detailCells = new StringBuilder();

        int index = 0;
        queryText.append("select data_point.id as dp_pk, data_point.identifier as dp_text, form_instance.id as fi_pk, "); //initially not displayed in the table


        //Iterate over questions and make them into BIRT columns
        while (questions.next()) {
            
            // compute column names
            // q is iteration object of original code
            List<Map<String, String>> colNames = new ArrayList<Map<String, String>>();
            int id = questions.getInt("id");
            String text = questions.getString("display_text");
            
            if (questions.getString("type").equalsIgnoreCase("GEO")) { // JSON object
                //Assume a GEOJSON point, which will have to be split into separate columns
                Map<String, String> names = new HashMap<String, String>();
                names.put("columnName", "latitude");
                names.put("displayName", text + " - latitude");
                names.put("heading", text + " - latitude");
                names.put("position", index + "");
                names.put("dataType", "string");
                colNames.add(names);
                
                // queryText which will do the splitting
//                queryText.append("(select response.value->>'latitude' from response where response.form_instance_id = form_instance.id and response.question_id=2 and response.iteration=0) as latitude,");
                queryText.append("(select response.value#>'{coordinates,1}' from response where response.form_instance_id = form_instance.id and response.question_id="+id+" and response.iteration=0) as latitude,");
                index++;
                
                Map<String, String> names2 = new HashMap<String, String>();
                names2.put("columnName", "longitude");
                names2.put("displayName", text + " - longitude");
                names2.put("heading", text + " - longitude");
                names2.put("position", index + "");
                names2.put("dataType", "string");
                colNames.add(names2);
                
                // queryText
//                queryText.append("q" + id + "->>'lon' AS longitude,");
                queryText.append("(select response.value#>'{coordinates,0}' from response where response.form_instance_id = form_instance.id and response.question_id="+id+" and response.iteration=0) as longitude,");

            } else { //Not GEO - currently, a single JSON data item: string or integer
                Map<String, String> names = new HashMap<String, String>();
                names.put("columnName", "q" + id);
                names.put("displayName", text);
                names.put("heading", text);
                names.put("position", index + "");
                names.put("dataType", "string");
                colNames.add(names);
                
                // queryText
//                queryText.append("q" + id + ",");
                queryText.append("(select response.value from response where response.form_instance_id = form_instance.id and response.question_id="+id+" and response.iteration=0) as q" + id + ",");
            }

                
            //Column names are done
            for (Map<String, String> name : colNames) {
                // columnHints
                columnHints.append("<structure>\n");
                columnHints.append("<property name=\"columnName\">" + name.get("columnName")
                        + "</property>\n");
                columnHints.append("<text-property name=\"displayName\">"
                        + name.get("displayName")
                        + "</text-property>\n");
                columnHints.append("<text-property name=\"heading\">" + name.get("displayName")
                        + "</text-property>\n");
                columnHints.append("</structure>\n");

                // resultSet
                resultSet.append("<structure>\n");
                resultSet.append("<property name=\"position\">" + name.get("position")
                        + "</property>\n");
                resultSet.append("<property name=\"name\">" + name.get("columnName")
                        + "</property>\n");
                resultSet.append("<property name=\"nativeName\">" + name.get("columnName")
                        + "</property>\n");
                resultSet.append("<property name=\"dataType\">" + name.get("dataType")
                        + "</property>\n");
                resultSet.append("</structure>\n");

                // boundColumns
                boundDataColumns.append("<structure>\n");
                boundDataColumns.append("<property name=\"name\">" + name.get("columnName")
                        + "</property>\n");
                boundDataColumns.append("<text-property name=\"displayName\">"
                        + name.get("displayName")
                        + "</text-property>\n");
                boundDataColumns
                        .append("<expression name=\"expression\" type=\"javascript\">dataSetRow[\""
                                + name.get("columnName") + "\"]</expression>");
                boundDataColumns
                        .append("<property name=\"dataType\">" + name.get("dataType")
                                + "</property>\n");
                boundDataColumns.append("</structure>\n");

                // headerCells
                headerCells.append("<cell>\n<label>\n");
                headerCells.append("<text-property name=\"text\">" + name.get("displayName")
                        + "</text-property>\n");
                headerCells.append("</label>\n</cell>\n");

                // detailCells
                detailCells.append("<cell>\n<data>\n");
                detailCells.append("<property name=\"resultSetColumn\">"
                        + name.get("columnName")
                        + "</property>");
                detailCells.append("</data>\n</cell>\n");

                //columnSet
                columnSet.append("<column></column>\n");
                
            }
            index++;
        }
        // remove last added comma
        queryText.deleteCharAt(queryText.length() - 1);

        //queryText.append(" from " + "question");
        // queryText.append(" from " + "f" + questions.get(0).getSurveyId());
        queryText.append(" from data_point left join form_instance on data_point.id=form_instance.data_point_id where data_point.form_id=" + form_id);


        Map<String, String> result = new HashMap<String, String>();
        result.put("columnHints", columnHints.toString());
        result.put("resultSet", resultSet.toString());
        result.put("queryText", queryText.toString());
        result.put("boundDataColumns", boundDataColumns.toString());
        result.put("headerCells", headerCells.toString());
        result.put("detailCells", detailCells.toString());
        result.put("columnSet", columnSet.toString());

        return result;
    }

    
    private String assembleReportOnePass(long form_id, String formName, String formVersion, ResultSet questions) throws Exception {
        // questions must be filtered by Form id and should be sorted by order within form
        //        log.warn("Starting assembly of report ");

//        SurveyDAO surveyDao = new SurveyDAO();
//        Survey s = surveyDao.getById(surveyId); //FORM
//        SurveyGroupDAO surveyGroupDao = new SurveyGroupDAO();
//        SurveyGroup sg = surveyGroupDao.getByKey(s.getSurveyGroupId());
//        Long transactionId = randomNumber.nextLong();
        String lang = "en";
        //TODO: language
        //        if (s != null && s.getDefaultLanguageCode() != null) {
        //            lang = s.getDefaultLanguageCode();
        //        }

//        final String versionAttribute = s.getVersion() == null ? "" : "version='"
//                + s.getVersion() + "'";
//        String name = s.getName();
        final String versionAttribute = formVersion == null ? "" : "version='"
                + formVersion + "'";
        String name = formName;

        final VelocityEngine engine = new VelocityEngine();
        //By default, Velocity will create a file-based logger in the current directory.
        engine.setProperty("runtime.log.logsystem.class",
                "org.apache.velocity.runtime.log.NullLogChute");
        
        //TODO external config for directory and filename of template 
        //getServletContext().getInitParameter("meta_template");
        //For test, define directory here
//        engine.setProperty("file.resource.loader.path","C:\_Dev\DASH and Reports\BIRT report from FLOW");
        engine.setProperty("file.resource.loader.path","C:\\_Dev\\FLOWBIRT\\");

        Template t = null;
//        try {
            engine.init();
            t = engine.getTemplate("simpleReport.vm");
//        } catch (Exception e) {
//            log.error("Could not initialize velocity templating engine or get template", e);
            
//            return "";
//        }

        final VelocityContext context = new VelocityContext();
        
        // create survey specific content
        Map<String, String> contents = createContents(form_id, questions);

        context.put("columnHints", contents.get("columnHints"));
        context.put("resultSet", contents.get("resultSet"));
        context.put("queryText", contents.get("queryText"));
        context.put("boundDataColumns", contents.get("boundDataColumns"));
        context.put("headerCells", contents.get("headerCells"));
        context.put("detailCells", contents.get("detailCells"));
        context.put("columnSet", contents.get("columnSet"));

        StringWriter writer = new StringWriter();
        try {
            t.merge(context, writer);
        } catch (ResourceNotFoundException | ParseErrorException | MethodInvocationException
                | IOException e) {
            //log.error("Could not create XML for report", e);
            e.printStackTrace();
        }
        return writer.toString();

    }

    

    
    void parameterError(HttpServletResponse response) throws IOException {
        // Set response content type
        response.setStatus(400);
        response.setContentType("text/plain");
        PrintWriter out = response.getWriter();
        out.println("You must provide an integer value for the form_id parameter");
    }
    
    void getAsHtml(HttpServletResponse response, int id) throws IOException, SQLException {
        // Set response content type
        response.setContentType("text/html");//TODO make a constant
        PrintWriter out = response.getWriter();
        String title = "Report "+id+" successfully defined!";
        String docType = "<!doctype html public \"-//w3c//dtd html 4.0 "
                + "transitional//en\">\n";
        out.println(docType + "<html>\n" + "<head><title>" + title
                + "</title></head>\n" + "<body bgcolor=\"#f0f0f0\">\n"
                + "<h1 align=\"center\">" + title + "</h1>\n"); 
        
         out.println("</body></html>");
    }
    
    //JSON jar was fetched from Maven Central as described in https://github.com/eskatos/org.json-java
    
    void getAsJson(HttpServletResponse response, int id) throws IOException, SQLException {
        //build JSON structure
        JSONObject root = new JSONObject();
        root.put("id", id);
        
        // Set response content type
        response.setContentType("application/json");//TODO make a constant
        PrintWriter out = response.getWriter();
               
        out.print(root.toString());
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
	    long form_id = 0;
	    Map<String, String> params;
	    try {
	        params = getQueryMap(request.getQueryString());
	        String form_id_str = params.get("form_id");
	        form_id = Integer.parseInt(form_id_str);
	    }
	    catch (Exception e) {
	        parameterError(response);
	        return;
	    }

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
            e1.printStackTrace();
        }

        // Open a connection
        Connection conn = null;
        
        PreparedStatement stmt = null;
        PreparedStatement stmt_insert = null;
        
        try {
            //TODO move connection mgmt to a init/destroy method pair
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            // Execute SQL query to get all questions for this form in order
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            // Execute SQL query
            String sql;
            sql = "SELECT form.id AS form_pk, form.display_text AS form_text, question_group.id as qg_pk, question_group.display_text as qg_text, question.* "+
                  "FROM form LEFT JOIN question_group ON form.id=question_group.form_id LEFT JOIN question ON question_group.id=question.question_group_id "+
                  "WHERE form.id=?";//TODO order by 
            stmt = conn.prepareStatement(sql);
            stmt.setLong(1, form_id);
            
            ResultSet rs = stmt.executeQuery(); 
            String template = assembleReportOnePass(form_id, "TheFormName", "1.0", rs);
            
            // Prepare saving result
            String sql_insert;
            //PostgreSQL specific, return of new primary key:
            // INSERT INTO distributors (did, dname) VALUES (DEFAULT, 'XYZ Widgets') RETURNING did;
            sql_insert = "INSERT INTO birt_template (id, survey_id, form_id, parameters, template) VALUES (DEFAULT, ?, ?, ?, ?) returning id"; 
            String reportParams = "{\"test\":true}";//TODO
            stmt_insert = conn.prepareStatement(sql_insert);
            stmt_insert.setLong(1, 1);//survey id, TODO
            stmt_insert.setLong(2, form_id);
            PGobject jsonObject = new PGobject();
            jsonObject.setType("jsonb");
            jsonObject.setValue(reportParams);
            stmt_insert.setObject(3, jsonObject); //avoid injection attacks...
            stmt_insert.setString(4, template); //avoid quoting problems
            
            ResultSet rs2 = stmt_insert.executeQuery();
            if (rs2.next()) {
                int report_id = rs2.getInt(1); //there can be only one
                //drop it in the file system for BIRT
                save(template, report_id);
            
                //pick output format for success message
                if (params.get("format") != null && params.get("format").equalsIgnoreCase("json")) {
                    getAsJson(response, report_id);
                } else {
                    getAsHtml(response, report_id);
                }
            } else {
                //TODO: could not create db entry error
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
                if (stmt_insert != null)
                    stmt_insert.close();
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
