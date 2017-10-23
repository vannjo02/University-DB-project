import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.util.Scanner;
import java.util.ArrayList;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

public class University {
    
    public static Connection connect(String url, String user, String password) throws SQLException {
    	Connection con = DriverManager.getConnection(url,user,password);
    	DatabaseMetaData meta = con.getMetaData();
    	
    	return con;
    }
    
    private void createTables(Connection db_connection) throws SQLException {
    	System.out.println(db_connection);
    	
    	Statement statement = db_connection.createStatement();
    	statement.executeUpdate("SET FOREIGN_KEY_CHECKS=0;");
    	
    	statement.executeUpdate("DROP TABLE IF EXISTS semester;");
    	statement.executeUpdate("CREATE TABLE semester ("
    						+ "id int not null auto_increment,"
    						+ "year int not null,"
    						+ "season text not null,"
    						+ "primary key(id));");
    						
    	statement.executeUpdate("DROP TABLE IF EXISTS student;");
    	statement.executeUpdate("CREATE TABLE student ("
    						+ "id int not null auto_increment,"
    						+ "name text not null,"
    						+ "graduationDate int not null,"
    						+ "major int null,"
    						+ "adviser int not null,"
    						+ "primary key(id),"
    						+ "foreign key(graduationDate) references semester(id),"
    						+ "foreign key(major) references major(id),"
    						+ "foreign key(adviser) references faculty(id));");
    						
		statement.executeUpdate("DROP TABLE IF EXISTS major;");					
    	statement.executeUpdate("CREATE TABLE major ("
    						+ "id int not null auto_increment,"
    						+ "department int not null,"
    						+ "name text not null,"
    						+ "primary key(id),"
    						+ "foreign key(department) references department(id));");
    						
		statement.executeUpdate("DROP TABLE IF EXISTS enrollment;");					
    	statement.executeUpdate("CREATE TABLE enrollment ("
    						+ "id int not null auto_increment,"
    						+ "student int not null,"
    						+ "section int not null,"
    						+ "grade text null,"
    						+ "primary key(id),"
    						+ "foreign key(student) references student(id),"
    						+ "foreign key(section) references section(id));");
    						
    	statement.executeUpdate("DROP TABLE IF EXISTS section;");
    	statement.executeUpdate("CREATE TABLE section ("
    						+ "id int not null auto_increment,"
    						+ "course int not null,"
    						+ "instructor int not null,"
    						+ "offered int not null,"
    						+ "location int not null,"
    						+ "startHour time not null,"
    						+ "primary key(id),"
    						+ "foreign key(course) references course(id),"
    						+ "foreign key(instructor) references faculty(id),"
    						+ "foreign key(offered) references semester(id),"
    						+ "foreign key(location) references location(id));");
    						
    	statement.executeUpdate("DROP TABLE IF EXISTS faculty;");
    	statement.executeUpdate("CREATE TABLE faculty ("
    						+ "id int not null auto_increment,"
    						+ "name text not null,"
    						+ "department int not null,"
    						+ "startDate int not null,"
    						+ "endDate int null,"
    						+ "office int not null,"
    						+ "primary key(id),"
    						+ "foreign key(department) references department(id),"
    						+ "foreign key(startDate) references semester(id),"
    						+ "foreign key(endDate) references semester(id),"
    						+ "foreign key(office) references location(id));");
    						
    	statement.executeUpdate("DROP TABLE IF EXISTS course;");
    	statement.executeUpdate("CREATE TABLE course ("
    						+ "id int not null auto_increment,"
    						+ "department int not null,"
    						+ "abbreviation text not null,"
    						+ "number int not null,"
    						+ "title text not null,"
    						+ "credits int not null,"
    						+ "primary key(id),"
    						+ "foreign key(department) references department(id));");
    						
    	statement.executeUpdate("DROP TABLE IF EXISTS location;");
    	statement.executeUpdate("CREATE TABLE location ("
    						+ "id int not null auto_increment,"
    						+ "building text not null,"
    						+ "room int not null,"
    						+ "purpose text not null,"
    						+ "primary key(id));");
    						
    	statement.executeUpdate("DROP TABLE IF EXISTS department;");
    	statement.executeUpdate("CREATE TABLE department ("
    						+ "id int not null auto_increment,"
    						+ "name text not null,"
    						+ "building text not null,"
    						+ "primary key(id));");
    						
    	statement.executeUpdate("SET FOREIGN_KEY_CHECKS=1;");
    }
    
    private void initTables(Connection db_connection) throws SQLException {
    	
    	String lc_courses = "data/lc_courses.txt";
    	String lc_majors = "data/lc_majors.txt";
    	String lc_faculty = "data/lc_faculty.txt";
    	String lc_departments = "data/lc_departments.txt";
    	String line = null;
    	
    	//semester insert
    	Statement statement = db_connection.createStatement();
    	statement.executeUpdate("SET FOREIGN_KEY_CHECKS=0;");
    	String sql = "INSERT INTO semester(year, season) VALUES (?, ?);";
    	PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
        statement_prepared.setString(1, "2015");
        statement_prepared.setString(2, "Fall");
        statement_prepared.addBatch();
        statement_prepared.setString(1, "2016");
        statement_prepared.setString(2, "Spring");
        statement_prepared.addBatch();
        statement_prepared.setString(1, "2016");
        statement_prepared.setString(2, "Fall");
        statement_prepared.addBatch();
        statement_prepared.setString(1, "2017");
        statement_prepared.setString(2, "Spring");
        statement_prepared.addBatch();
        statement_prepared.setString(1, "2017");
        statement_prepared.setString(2, "Fall");
        statement_prepared.addBatch();
        statement_prepared.setString(1, "2018");
        statement_prepared.setString(2, "Spring");
        statement_prepared.addBatch();
        statement_prepared.setString(1, "2018");
        statement_prepared.setString(2, "Fall");
        statement_prepared.addBatch();
        statement_prepared.setString(1, "2019");
        statement_prepared.setString(2, "Spring");
        statement_prepared.addBatch();
        
        db_connection.setAutoCommit(false);
        statement_prepared.executeBatch();
        db_connection.commit();
        
        //student insert
        sql = "INSERT INTO student(name, graduationDate, adviser) VALUES (?, ?, ?);";
    	statement_prepared = db_connection.prepareStatement(sql);
        statement_prepared.setString(1, "Ryne");
        statement_prepared.setString(2, "6");
        statement_prepared.setString(3, "1");
        statement_prepared.addBatch();
        statement_prepared.setString(1, "Forrest");
        statement_prepared.setString(2, "6");
        statement_prepared.setString(3, "2");
        statement_prepared.addBatch();
        statement_prepared.setString(1, "Dacoda");
        statement_prepared.setString(2, "8");
        statement_prepared.setString(3, "1");
        statement_prepared.addBatch();
        
        db_connection.setAutoCommit(false);
        statement_prepared.executeBatch();
        db_connection.commit();
        
        //enrollment insert
        sql = "INSERT INTO enrollment(student, section, grade) VALUES (?, ?, ?);";
    	statement_prepared = db_connection.prepareStatement(sql);
        statement_prepared.setString(1, "1");
        statement_prepared.setString(2, "1");
        statement_prepared.setString(3, "A");
        statement_prepared.addBatch();
        statement_prepared.setString(1, "2");
        statement_prepared.setString(2, "2");
        statement_prepared.setString(3, "C-");
        statement_prepared.addBatch();
        statement_prepared.setString(1, "3");
        statement_prepared.setString(2, "1");
        statement_prepared.setString(3, "B");
        statement_prepared.addBatch();
        
        db_connection.setAutoCommit(false);
        statement_prepared.executeBatch();
        db_connection.commit();
        
        //section insert
        sql = "INSERT INTO section(course, instructor, offered, location, startHour) VALUES (?, ?, ?, ?, ?);";
    	statement_prepared = db_connection.prepareStatement(sql);
        statement_prepared.setString(1, "1");
        statement_prepared.setString(2, "1");
        statement_prepared.setString(3, "1");
        statement_prepared.setString(4, "1");
        statement_prepared.setString(5, "08:00:00");
        statement_prepared.addBatch();
        
        db_connection.setAutoCommit(false);
        statement_prepared.executeBatch();
        db_connection.commit();
        
        //faculty insert
        sql = "INSERT INTO faculty(name, department, startDate, office) VALUES (?, ?, ?, ?);";
    	statement_prepared = db_connection.prepareStatement(sql);
        statement_prepared.setString(1, "Kent Lee");
        statement_prepared.setString(2, "1");
        statement_prepared.setString(3, "1900");
        statement_prepared.setString(4, "1");
        statement_prepared.addBatch();
        statement_prepared.setString(1, "Jobe");
        statement_prepared.setString(2, "2");
        statement_prepared.setString(3, "1950");
        statement_prepared.setString(4, "2");
        statement_prepared.addBatch();
        
        db_connection.setAutoCommit(false);
        statement_prepared.executeBatch();
        db_connection.commit();
        
        //location insert
        sql = "INSERT INTO location(building, room, purpose) VALUES (?, ?, ?);";
    	statement_prepared = db_connection.prepareStatement(sql);
        statement_prepared.setString(1, "Olin");
        statement_prepared.setString(2, "1");
        statement_prepared.setString(3, "Nobody Knows");
        statement_prepared.addBatch();
        statement_prepared.setString(1, "Ockam House");
        statement_prepared.setString(2, "2");
        statement_prepared.setString(3, "Housing(not)");
        statement_prepared.addBatch();
        
        db_connection.setAutoCommit(false);
        statement_prepared.executeBatch();
        db_connection.commit();
        
        //course insert
        sql = "INSERT INTO course(department, abbreviation, number, title, credits) VALUES (?, ?, ?, ?, ?);";
    	statement_prepared = db_connection.prepareStatement(sql);
    	ArrayList<ArrayList<String>> courses = new ArrayList<ArrayList<String>>();
    	
    	try {
    		FileReader fileReader = new FileReader(lc_courses);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			ArrayList<String> node = new ArrayList<String>();
			
			while ((line = bufferedReader.readLine()) != null) {
    			Scanner scanner = new Scanner(line);
    			scanner.useDelimiter("\\|");
    			
    			node = new ArrayList<String>();
    			
    			while (scanner.hasNext()) {
    				String temp = scanner.next();
    				node.add(temp);
    			}
    			scanner.close();
    			courses.add(node);
    		}
    		
    		bufferedReader.close();
    	}
    	
		catch(FileNotFoundException ex) {
		
			System.out.println("Error—file not found");
			ex.printStackTrace();
		}

		catch (IOException ex) {
			
			ex.printStackTrace();
		}
		
		String roman = null;
		
		for (int i=0; i<courses.size(); i++) {
			ArrayList<String> current = courses.get(i);
			
			if (current.get(0).length() == 1 | current.get(0).length() == 2 && current.get(0) != "CS" && current.get(0) != "DS" && current.get(0) != "GS" && current.get(0) != "IS" && current.get(0) != "PE" && current.get(0) != "SW") {
				roman = current.get(0);
				Scanner scanner = new Scanner(current.get(1));
    			scanner.useDelimiter(" ");
    			ArrayList<String> x = new ArrayList<String>();
    		
    			while (scanner.hasNext()) {
    				x.add(scanner.next());
    			}
    		
				scanner.close();
			
				//I am not sure how to figure out which course belongs to which department
				statement_prepared.setString(1, roman);
			
        		statement_prepared.setString(2, x.get(0));
        		statement_prepared.setString(3, x.get(1));
        		statement_prepared.setString(4, current.get(2));
        		statement_prepared.setString(5, current.get(3));
        		statement_prepared.addBatch();
        	
        		db_connection.setAutoCommit(false);
        		statement_prepared.executeBatch();
        		db_connection.commit();
			}
			
			else {

				Scanner scanner = new Scanner(current.get(0));
    			scanner.useDelimiter(" ");
    			ArrayList<String> x = new ArrayList<String>();
    		
    			while (scanner.hasNext()) {
    				x.add(scanner.next());
    			}
    		
				scanner.close();
			
				//I am not sure how to figure out which course belongs to which department
				statement_prepared.setString(1, roman);
			
        		statement_prepared.setString(2, x.get(0));
        		statement_prepared.setString(3, x.get(1));
        		statement_prepared.setString(4, current.get(1));
        		statement_prepared.setString(5, current.get(2));
        		statement_prepared.addBatch();
        	
        		db_connection.setAutoCommit(false);
        		statement_prepared.executeBatch();
        		db_connection.commit();
        	}
		}
        
        //major insert
        sql = "INSERT INTO major(department, name) VALUES (?, ?);";
    	statement_prepared = db_connection.prepareStatement(sql);
    	ArrayList<ArrayList<String>> majors = new ArrayList<ArrayList<String>>();
    	
    	try {
    		FileReader fileReader = new FileReader(lc_majors);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			ArrayList<String> node = new ArrayList<String>();
			
			while ((line = bufferedReader.readLine()) != null) {
    			Scanner scanner = new Scanner(line);
    			scanner.useDelimiter("[|]");
    			
    			node = new ArrayList<String>();
    			
    			while (scanner.hasNext()) {
    				String temp = scanner.next();
    				node.add(temp);
    			}
    			scanner.close();
    			majors.add(node);
    		}
    		
    		bufferedReader.close();
    	}
    	
		catch(FileNotFoundException ex) {
		
			System.out.println("Error—file not found");
			ex.printStackTrace();
		}

		catch (IOException ex) {
			
			ex.printStackTrace();
		}
		  	
		for (int i=0; i<majors.size(); i++) {
			ArrayList<String> current = majors.get(i);
			statement_prepared.setString(1, current.get(2));
        	statement_prepared.setString(2, current.get(0));
        	statement_prepared.addBatch();
        	
        	db_connection.setAutoCommit(false);
        	statement_prepared.executeBatch();
        	db_connection.commit();
		}
        
        //department insert
        sql = "INSERT INTO department(name, building) VALUES (?, ?);";
    	statement_prepared = db_connection.prepareStatement(sql);
    	ArrayList<ArrayList<String>> departments = new ArrayList<ArrayList<String>>();
    	
    	try {
    		FileReader fileReader = new FileReader(lc_departments);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			ArrayList<String> node = new ArrayList<String>();
			
			while ((line = bufferedReader.readLine()) != null) {
    			Scanner scanner = new Scanner(line);
    			scanner.useDelimiter("\\|");
    			
    			node = new ArrayList<String>();
    			
    			while (scanner.hasNext()) {
    				String temp = scanner.next();
    				node.add(temp);
    			}
    			scanner.close();
    			departments.add(node);
    		}
    		
    		bufferedReader.close();
    	}
    	
		catch(FileNotFoundException ex) {
		
			System.out.println("Error—file not found");
			ex.printStackTrace();
		}

		catch (IOException ex) {
			
			ex.printStackTrace();
		}
    	
		for (int i=0; i<departments.size(); i++) {
			ArrayList<String> current = departments.get(i);
			statement_prepared.setString(1, current.get(0));
        	statement_prepared.setString(2, current.get(3));
        	statement_prepared.addBatch();
        	
        	db_connection.setAutoCommit(false);
        	statement_prepared.executeBatch();
        	db_connection.commit();
		}
        
        statement.executeUpdate("SET FOREIGN_KEY_CHECKS=1;");
    }
    
    private void createViews(Connection db_connection) throws SQLException {
    	Statement statement = db_connection.createStatement();
    	
    	//View Advisees of a professor (with id=1)
    	statement.executeUpdate("DROP VIEW IF EXISTS view_professor_advisees;");
    	statement.executeUpdate("CREATE VIEW view_professor_advisees AS "
    						+ "SELECT student.name FROM student "
    						+ "JOIN faculty ON student.adviser=faculty.id "
    						+ "WHERE faculty.id='1';");
    						
    	//View RED_ALERT
    	statement.executeUpdate("DROP VIEW IF EXISTS RED_ALERT;");
    	statement.executeUpdate("CREATE VIEW RED_ALERT AS "
    						+ "SELECT title FROM course "
    						+ "JOIN section ON course.id=section.course "
    						+ "JOIN enrollment ON section.id=enrollment.section "
    						+ "GROUP BY section.id HAVING count(enrollment.id)<8;");

    	//View transcript (grades and titles) where student.id=1
    	statement.executeUpdate("DROP VIEW IF EXISTS transcript;");
    	statement.executeUpdate("CREATE VIEW transcript AS "
    						+ "SELECT name,grade,title FROM student "
    						+ "JOIN enrollment ON student.id=enrollment.student "
    						+ "JOIN section ON enrollment.section=section.id "
    						+ "JOIN course ON section.course=course.id "
    						+ "WHERE student.id=1;");
    						
    	//View Faculty members and locations
    	statement.executeUpdate("DROP VIEW IF EXISTS faculty_info;");
    	statement.executeUpdate("CREATE VIEW faculty_info AS "
    						+ "SELECT faculty.name,department.name as department, "
    						+ "department.building as department_building, location.building as office_location, "
    						+ "room as room_number FROM faculty "
    						+ "JOIN department ON faculty.department=department.id "
    						+ "JOIN location on faculty.office=location.id;");
    						
    	//View Students and their grades of a particular section(id=1 in this case)
    	statement.executeUpdate("DROP VIEW IF EXISTS section_student_grades;");
    	statement.executeUpdate("CREATE VIEW section_student_grades AS "
    						+ "SELECT name, grade, title FROM student "
    						+ "JOIN enrollment ON student.id=enrollment.student "
    						+ "JOIN section on section.id=enrollment.section "
    						+ "JOIN course on course.id=section.course "
    						+ "WHERE section.id=1;");			
    }
    
    /*private void createTriggers(Connection db_connection) throws SQLException {
    	Statement statement = db_connection.createStatement();
    	
    	//Sets a new student's major to NULL
    	
    	statement.executeUpdate("DROP TRIGGER IF EXISTS new_student;");
    	statement.executeUpdate("DELIMITER $$");
    	statement.executeUpdate("CREATE TRIGGER new_student AS "
    						+ ""
    						);
    	
    	statement.executeUpdate("DELIMITER ;");
    }*/
    
    public void createDB(String url, String user, String password) throws SQLException {
    	Connection con = DriverManager.getConnection(url,user,password);
    	
    	this.createTables(con);
    	this.initTables(con);
    	this.createViews(con);
    	//this.createTriggers(con);
    }
}
