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

//import javax.xml.bind.DatatypeConverter;
//import org.apache.tomcat.util.codec.binary.Base64;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.json.JSONObject;
import org.postgresql.util.PGobject;

/**
 * Servlet implementation class ListReports
 */
/**
 * @author stellan
 *
 */
@WebServlet(
		description = "Define a report for a specific form", 
		urlPatterns = { 
				"/CreateReport", 
				"/create_report"
		})
public class CreateReport extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
    private static String option_separator;
    private static String iteration_separator;
    private String dbUrl;
    private String dbUser;
    private String dbPassword;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public CreateReport() {
        super();
    }

/*    
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
*/
    
    public void save(String text, String tenant, long form_id, long report_id) throws Exception {
        FileWriter fileWriter = null;
        PrintWriter printWriter = null;
        fileWriter = new FileWriter("C:\\_Dev\\FLOWBIRT\\"+tenant+"_form-"+form_id+"_report-"+report_id+".rptdesign");
        printWriter = new PrintWriter(fileWriter);
        printWriter.print(text);

        // make sure to close the file
        if (printWriter != null) {
          printWriter.close();
        }
      }
  

    /**
     * generates seven replacement strings to be inserted into the template
     * @param form_id
     * @param questions
     * @return
     * @throws SQLException
     */
    private Map<String, String> createContents(long form_id, ResultSet questions, boolean iterate_cells) throws SQLException {
        StringBuilder columnHints = new StringBuilder();
        StringBuilder resultSet = new StringBuilder();
        StringBuilder queryText = new StringBuilder();
        StringBuilder boundDataColumns = new StringBuilder();
        StringBuilder columnSet = new StringBuilder();//need no attributes, but required in table
        StringBuilder headerCells = new StringBuilder();
        StringBuilder detailCells = new StringBuilder();

        int index = 0;
        queryText.append("select form_instance.id as fi_pk,\n"); //for now, not displayed in the table


        //Iterate over questions and make them into BIRT columns
        while (questions.next()) {
            
            // compute column names
            // q is iteration object of original code
            List<Map<String, String>> colNames = new ArrayList<Map<String, String>>();
            int id = questions.getInt("id");
            String text = questions.getString("display_text");
            
            if (questions.getString("type").equalsIgnoreCase("GEO")) { // GeoJSON object with just one point
                // {"type": "GEO", "value": {"type": "Feature", "geometry": {"type": "Point", "coordinates": [-1.75630525, 52.40376391, 189.6]}, "properties": {"code": "6oqmgtjv"}}}
                //The point lat and long will be split into separate columns; coordinates are in [long,lat,ele] order
                Map<String, String> names = new HashMap<String, String>();
                names.put("columnName", "latitude");
                names.put("displayName", text + " - latitude");
                names.put("heading", text + " - latitude");
                names.put("position", index + "");
                names.put("dataType", "string");
                colNames.add(names);
                
                // queryText which will do the splitting of a point from GeoJSON
                queryText.append("(select response.value#>'{value,geometry,coordinates,1}' from response where response.form_instance_id = form_instance.id and response.question_id="+id+" and response.iteration=0) as latitude,\n");
                index++;
                
                Map<String, String> names2 = new HashMap<String, String>();
                names2.put("columnName", "longitude");
                names2.put("displayName", text + " - longitude");
                names2.put("heading", text + " - longitude");
                names2.put("position", index + "");
                names2.put("dataType", "string");
                colNames.add(names2);
                queryText.append("(select response.value#>'{value,geometry,coordinates,0}' from response where response.form_instance_id = form_instance.id and response.question_id="+id+" and response.iteration=0) as longitude,\n");
                index++;
                
                Map<String, String> names3 = new HashMap<String, String>();
                names3.put("columnName", "elevation");
                names3.put("displayName", text + " - elevation");
                names3.put("heading", text + " - elevation");
                names3.put("position", index + "");
                names3.put("dataType", "string");
                colNames.add(names3);
                queryText.append("(select response.value#>'{value,geometry,coordinates,2}' from response where response.form_instance_id = form_instance.id and response.question_id="+id+" and response.iteration=0) as elevation,\n");

            } else if (questions.getString("type").equalsIgnoreCase("OPTION")) { // Currently handling just single option
                // {"type"="OPTION",value="[{"text": "foobar", "code": "foo"}]" //more objects in array for multiple option
                Map<String, String> names = new HashMap<String, String>();
                names.put("columnName", "q" + id);
                names.put("displayName", text);
                names.put("heading", text);
                names.put("position", index + "");
                names.put("dataType", "string");
                colNames.add(names);
                
                // queryText
                queryText.append("(select response.value#>'{value,0,text}' from response where response.form_instance_id = form_instance.id and response.question_id="+id+" and response.iteration=0) as q" + id + ",\n");
            } else
            { //Not GEO - currently, only support a single JSON data item: string or integer
                // {"type": "FREE_TEXT", "value": "foobar"}
                // {"type": "NUMBER", "value": 17}
                // {"type": "PHOTO", "value": "/file/name.ext"}
                // {"type": "SCAN", "value": "string"}
                // {"value": "12-02-2013 00:00:00 ECT"}    //DATE, for example
                Map<String, String> names = new HashMap<String, String>();
                names.put("columnName", "q" + id);
                names.put("displayName", text);
                names.put("heading", text);
                names.put("position", index + "");
                names.put("dataType", "string");//TODO is this true for NUMBER?
                colNames.add(names);
                
                // queryText
                if (iterate_cells) {
                    queryText.append("(select array_to_string( array_agg( response.value->>'value'), '|') from response where response.form_instance_id = form_instance.id and response.question_id="+id+") as q" + id + ",\n");
                } else {
                    queryText.append("(select response.value->>'value' from response where response.form_instance_id = form_instance.id and response.question_id="+id+" and response.iteration=0) as q" + id + ",\n");
                }
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
        queryText.deleteCharAt(queryText.length() - 2);

        //queryText.append(" from " + "question");
        // queryText.append(" from " + "f" + questions.get(0).getSurveyId());
        queryText.append(" from form_instance where form_instance.form_id=" + form_id);


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

    
    private String assembleSimpleReport(Connection conn, long form_id, String formName, String formVersion, boolean iterateInCells) throws Exception {

        // Execute SQL query to get all questions for this form in order
        String sql = "SELECT form.id AS form_pk, form.display_text AS form_text, question_group.id as qg_pk, question_group.display_text as qg_text, question.* " +
                     "FROM form LEFT JOIN question_group ON form.id=question_group.form_id LEFT JOIN question ON question_group.id=question.question_group_id " +
                     "WHERE form.id=? ORDER BY question_group.display_order, question.display_order, question.id";            
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setLong(1, form_id);
        
        ResultSet questions = stmt.executeQuery(); 
        try {
            String lang = "en";
            //TODO: language and version
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
            engine.setProperty("file.resource.loader.path","C:\\_Dev\\FLOWBIRT\\");
    
            Template t = null;
            engine.init();
            t = engine.getTemplate("simpleReport.vm","UTF-8");
            final VelocityContext context = new VelocityContext();
            
            // create survey specific content
            Map<String, String> contents = createContents(form_id, questions, iterateInCells);
            questions.close();
            
            context.put("columnHints", contents.get("columnHints"));
            context.put("resultSet", contents.get("resultSet"));
            context.put("queryText", contents.get("queryText"));
            context.put("boundDataColumns", contents.get("boundDataColumns"));
            context.put("headerCells", contents.get("headerCells"));
            context.put("detailCells", contents.get("detailCells"));
            context.put("columnSet", contents.get("columnSet"));
            context.put("dbUrl", dbUrl);
            context.put("dbUser", dbUser);
            byte[] barr = dbPassword.getBytes("UTF-8");
//            context.put("dbPassword", DatatypeConverter.printBase64Binary(barr)); //silly obfuscation
    
            StringWriter writer = new StringWriter();
            try {
                t.merge(context, writer);
            } catch (ResourceNotFoundException | ParseErrorException | MethodInvocationException
                    | IOException e) {
                //log.error("Could not create XML for report", e);
                e.printStackTrace();
            }
            return writer.toString();
        } finally {
            try {
                if (questions != null)
                    questions.close();
            } catch (SQLException se) {
            }// nothing we can do
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }// nothing we can do

        }
    }


    
    // create contents for one question_group for the the two inner templates
    private Map<String, String> createGroupContents(long form_id, long qg_id, ResultSet questions) throws SQLException {
        StringBuilder columnHints = new StringBuilder();
        StringBuilder resultSet = new StringBuilder();
        StringBuilder queryText1 = new StringBuilder();
        StringBuilder queryText2 = new StringBuilder();
        StringBuilder boundDataColumns = new StringBuilder();
        StringBuilder columnSet = new StringBuilder();//need no attributes, but required in table
        StringBuilder headerCells = new StringBuilder();
        StringBuilder detailCells = new StringBuilder();

        /* Query should look like this for 3 questions, id 3,4,5 of form 1:
        select 
           data_point.id as dp_pk,
           data_point.identifier as dp_text,
           form_instance.id as fi_pk,
           q3, q4, q5
       ------------------------------------------------------------------------------------------cut between text1 and text2    
       from
           data_point left join form_instance on data_point.id=form_instance.data_point_id
           left join (
           --subquery: table of all Q4 responses
           select response.value as q3, response.form_instance_id as fi_id, response.iteration as ite
           from data_point left join form_instance on data_point.id=form_instance.data_point_id left join response on form_instance.id = response.form_instance_id
           where data_point.form_id=1 and response.question_id=3
           ) as temp3 on temp3.fi_id = form_instance.id
            
           left join (
           --subquery: table of all Q4 responses
           select response.value as q4, response.form_instance_id as fi_id, response.iteration as ite
           from data_point left join form_instance on data_point.id=form_instance.data_point_id left join response on form_instance.id = response.form_instance_id
           where data_point.form_id=1 and response.question_id=4
           ) as temp4 on temp4.fi_id = form_instance.id and temp4.ite=temp3.ite

           left join (
           --subquery: table of all Q5 responses
           select response.value as q5, response.form_instance_id as fi_id, response.iteration as ite
           from data_point left join form_instance on data_point.id=form_instance.data_point_id left join response on form_instance.id = response.form_instance_id
           where data_point.form_id=1 and response.question_id=5
           ) as temp5 on temp5.fi_id = form_instance.id and temp5.ite=temp3.ite
       where
           data_point.form_id=1
                   
        */
        
        int index = 0;
        queryText1.append("select data_point.id as dp_pk, data_point.identifier as dp_text, form_instance.id as fi_pk, "); //not actually displayed in the table
        queryText2.append(" from data_point left join form_instance on data_point.id=form_instance.data_point_id " );

        //Iterate over questions and make them into BIRT columns
        boolean firstColumn = true;
        while (questions.next()) {
            
            // compute column names
            List<Map<String, String>> colNames = new ArrayList<Map<String, String>>();
            int id = questions.getInt("id");
            String text = questions.getString("display_text");
            
            if (questions.getString("type").equalsIgnoreCase("GEO")) { // JSON object
                //TODO
            } else { //Not GEO - currently, a single JSON object: {"value"="string";}  or {"value"=integer}
                Map<String, String> names = new HashMap<String, String>();
                names.put("columnName", "q" + id);//these will get long in practice, so maybe we want a counter to make them q1,q2,q3 etc
                names.put("displayName", text);
                names.put("heading", text);
                names.put("position", index + "");
                names.put("dataType", "string");
                colNames.add(names);
                
                // select-list of columns
                queryText1.append("q" + id + ",");//TODO json-extraction of value member?
            }

            queryText1.deleteCharAt(queryText1.length() - 1);// remove last added comma
                
            //Column names are done
            //subquery for this column
            queryText2.append(" left join (");
            queryText2.append("  select response.value as q3, response.form_instance_id as fi_id, response.iteration as ite");
            queryText2.append(" from data_point left join form_instance on data_point.id=form_instance.data_point_id left join response on form_instance.id = response.form_instance_id");
            queryText2.append("        where data_point.form_id=1 and response.question_id=3");
            if (firstColumn) {
                queryText2.append(" and data_point.form_id=1 and response.question_id=3");                    
            }
            queryText2.append(" ) as temp3 on temp3.fi_id = form_instance.id");
            
            firstColumn = false;
            
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
        // remove last added comma from select-list
        queryText1.deleteCharAt(queryText1.length() - 1);
//        queryText.append(" from data_point left join form_instance on data_point.id=form_instance.data_point_id where data_point.form_id=" + qg_id);


        Map<String, String> result = new HashMap<String, String>();
        result.put("columnHints", columnHints.toString());
        result.put("resultSet", resultSet.toString());
        result.put("queryText", queryText1.toString() + queryText2.toString());
        result.put("boundDataColumns", boundDataColumns.toString());
        result.put("headerCells", headerCells.toString());
        result.put("detailCells", detailCells.toString());
        result.put("columnSet", columnSet.toString());

        return result;
    }

    
    private Map<String, String> assembleOneGroupContent(Connection conn, VelocityEngine engine, long form_id, long qg_id, String qg_text) throws Exception {
    
        // Execute SQL query to get all questions for this question_group in order
        String sql;
        String dataSetName = "FlowDataSet_" + qg_id; //Data sets need a name for reference from the table
        Map<String, String> result = new HashMap<String, String>();
        

        //First, assemble the data set
        
        sql = "SELECT question_group.id as qg_pk, question_group.display_text as qg_text, question.* " +
                "FROM question_group LEFT JOIN question ON question_group.id=question.question_group_id " +
                "WHERE question_group.id=? ORDER BY question_group._order, question._order";            
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setLong(1, qg_id);
        
        ResultSet questions = stmt.executeQuery(); 

        // create survey specific content
        Map<String, String> contents = createGroupContents(form_id, qg_id, questions);
        questions.close();
        
        //insert strings into first inner template
        Template t1 = engine.getTemplate("dataSet.vm","UTF-8");
        final VelocityContext context1 = new VelocityContext();
        
        context1.put("columnHints", contents.get("columnHints"));
        context1.put("resultSet", contents.get("resultSet"));
        context1.put("queryText", contents.get("queryText"));
        context1.put("dataSetName", dataSetName);

        StringWriter writer1 = new StringWriter();
        try {
            t1.merge(context1, writer1);
        } catch (ResourceNotFoundException | ParseErrorException | MethodInvocationException
                | IOException e) {
            //log.error("Could not create XML for report", e);
            e.printStackTrace();
        }
        //package it
        result.put("dataSets", writer1.toString());

        //Finally the table content
        
        //insert strings into second inner template
        Template t2 = engine.getTemplate("table.vm","UTF-8");
        final VelocityContext context2 = new VelocityContext();
        
        context2.put("dataSetName", dataSetName);
        context2.put("boundDataColumns", contents.get("boundDataColumns"));
        context2.put("headerCells", contents.get("headerCells"));
        context2.put("detailCells", contents.get("detailCells"));
        context2.put("columnSet", contents.get("columnSet"));

        StringWriter writer2 = new StringWriter();
        try {
            t2.merge(context2, writer2);
        } catch (ResourceNotFoundException | ParseErrorException | MethodInvocationException
                | IOException e) {
            //log.error("Could not create XML for report", e);
            e.printStackTrace();
        }
        //package it
    
        result.put("tables", writer2.toString());

        return result;    
    }
    

    
    //insert the two blocks (data sets and tables) into the main template
    private String assembleGroupByGroupReport(Connection conn, long form_id, String formName, String formVersion) throws Exception {

        final VelocityEngine engine = new VelocityEngine();
        //By default, Velocity will create a file-based logger in the current directory.
        engine.setProperty("runtime.log.logsystem.class",
                "org.apache.velocity.runtime.log.NullLogChute");
        
        //TODO external config for directory and filename of template 
        //getServletContext().getInitParameter("meta_template");
        //For test, define directory here
        engine.setProperty("file.resource.loader.path","C:\\_Dev\\FLOWBIRT\\");

        //Loop on all question groups
        String dataSets = "";
        String tables = "";
        
        Statement stmt = conn.createStatement();
        String sql;
        sql = "select question_group.id, question_group.display_text from question_group where question_group.form_id=" + form_id;
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            Map<String, String> setsAndTables = assembleOneGroupContent(conn, engine, form_id, rs.getInt("id"), rs.getString("display_text"));
            dataSets += setsAndTables.get("dataSets");
            tables += setsAndTables.get("tables");                   
        }
        // Clean-up environment
        rs.close();
        stmt.close();

        //insert content into main template
        try {            String lang = "en";
            //TODO: language and version
            //        if (s != null && s.getDefaultLanguageCode() != null) {
            //            lang = s.getDefaultLanguageCode();
            //        }
            //        final String versionAttribute = s.getVersion() == null ? "" : "version='"
            //                + s.getVersion() + "'";
            //        String name = s.getName();
            final String versionAttribute = formVersion == null ? "" : "version='"
                    + formVersion + "'";
            String name = formName;
    
            Template t = null;
            engine.init();
            t = engine.getTemplate("groupedReport.vm","UTF-8");
            final VelocityContext context = new VelocityContext();
            
            context.put("dataSets", dataSets);
            context.put("tables", tables);
            //TODO reference to the form itself
            
            StringWriter writer = new StringWriter();
            try {
                t.merge(context, writer);
            } catch (ResourceNotFoundException | ParseErrorException | MethodInvocationException
                    | IOException e) {
                //log.error("Could not create XML for report", e);
                e.printStackTrace();
            }
            return writer.toString();
        } finally {
            try {
                if (rs != null)
                    rs.close();
            } catch (SQLException se) {
            }// nothing we can do
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }// nothing we can do

        }
    }

    
    
    
    
    
    
    void parameterError(HttpServletResponse response) throws IOException {
        // Set response content type
        response.setStatus(400);
        response.setContentType("text/plain");
        PrintWriter out = response.getWriter();
        out.println("You must provide an integer value for the form_id parameter");
    }
    
    void getAsHtml(HttpServletResponse response, int id, String tenant) throws IOException, SQLException {
        // Set response content type
        response.setContentType("text/html");//TODO make a constant
        PrintWriter out = response.getWriter();
        String title = "Instance " + tenant + " Report "+id+" successfully defined!";
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
	    
	    //parameters
        long survey_id = 0;
        long form_id = 0;
	    int report_type = 0;
	    String sep;
	    Map<String, String> params;
	    try {
	        params = getQueryMap(request.getQueryString());
            String survey_id_str = params.get("survey_id");
            survey_id = Integer.parseInt(survey_id_str);
            String form_id_str = params.get("form_id");
            form_id = Integer.parseInt(form_id_str);
            String rtype_str = params.get("type");
            report_type = Integer.parseInt(rtype_str);
	    }
	    catch (Exception e) {
	        parameterError(response);
	        return;
	    }

        // JDBC driver name and database URL
        final String JDBC_DRIVER = "org.postgresql.Driver";
        final String tenant = "akvoflow-2";
        dbUrl = "jdbc:postgresql://localhost:5432/akvoflow-2";

        // Database credentials
        dbUser = "reporting";
        dbPassword = "gnitroper";


        // Register JDBC driver
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }

        // Open a connection
        Connection conn = null;
        
        try {
            //TODO move connection mgmt to a init/destroy method pair
            conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);

            //Determine RQG status
            boolean rqg = hasRQG(conn, form_id);
         
            String template="";
            switch (report_type){
                case 1: template = assembleSimpleReport(conn, form_id, "TheFormName", "1.0", false); break;
                case 2: template = assembleSimpleReport(conn, form_id, "TheFormName", "1.0", true); break;
                case 3: template = assembleGroupByGroupReport(conn, form_id, "TheFormName", "1.0"); break;
                default: parameterError(response);
            }
            
            saveReport(response, survey_id, form_id, params, conn, template, tenant, report_type);//TODO: where to get survey id from?
            // Clean-up environment
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
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }// end finally try
        } // end try
    
	}


    private boolean hasRQG(Connection conn, long form_id) throws SQLException {
        // Execute SQL query
        Statement stmt = conn.createStatement();
        String sql;
        sql = "SELECT COUNT(question_group.id) FROM question_group WHERE question_group.repeatable = true AND question_group.form_id=" + form_id;
        ResultSet rs = stmt.executeQuery(sql);
        int count = 0;
        if (rs.next()) {
            count=rs.getInt(1);
        }
        // Clean-up environment
        rs.close();
        stmt.close();
        return (count > 0);
    }


    private void saveReport(HttpServletResponse response,
            long survey_id,
            long form_id,
            Map<String, String> params,
            Connection conn,
            String template,
            String tenant,
            int type
            ) throws SQLException, Exception, IOException {
        PreparedStatement stmt_insert;
        // Prepare saving result
        String sql_insert;
        //PostgreSQL specific, return of new primary key:
        sql_insert = "INSERT INTO birt_template (id, survey_id, form_id, parameters, template) VALUES (DEFAULT, ?, ?, ?, ?) returning id"; 
        JSONObject rp = new JSONObject();
        rp.put("type", type);
        rp.put("test", true);
        rp.put("tenant", tenant);

        stmt_insert = conn.prepareStatement(sql_insert);
        try {
            stmt_insert.setLong(1, 1);//survey id, TODO
            stmt_insert.setLong(2, form_id);
            //how to do a JSON insertion:
            PGobject jsonObject = new PGobject();
            jsonObject.setType("jsonb");
            jsonObject.setValue(rp.toString());
            stmt_insert.setObject(3, jsonObject); //avoid injection attacks...
            stmt_insert.setString(4, template); //avoid quoting problems
            
            ResultSet rs2 = stmt_insert.executeQuery();
            if (rs2.next()) {
                int report_id = rs2.getInt(1); //there can be only one
                //drop it in the file system for BIRT
                save(template, tenant, form_id, report_id);
            
                //pick output format for success message
                if (params.get("format") != null && params.get("format").equalsIgnoreCase("json")) {
                    getAsJson(response, report_id);
                } else {
                    getAsHtml(response, report_id, tenant);
                }
            } else {
                //TODO: some "could not create db entry" error
            }
            rs2.close();
        } finally {
            stmt_insert.close();
        }
    }

}
