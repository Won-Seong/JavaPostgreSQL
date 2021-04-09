import java.sql.*;
import java.util.Vector;
import java.io.*;

public class main {

	public static void query_run(String query, Statement statement) {
		try {
			statement.executeQuery(query);
		} catch (SQLException e) {
			//System.out.println(e.getLocalizedMessage());
		}
	}

	public static ResultSet query_run_and_show(String query, Statement statement) {

		ResultSet temp_result;
		System.out.println("Translated SQL : " + query);
		try {
			temp_result = statement.executeQuery(query);
			return temp_result;
		} catch (SQLException e) {
			//System.out.println(e.getLocalizedMessage());
			return null;
		}
	}

	public static void show_table(String table_name, Statement statement) {
		try {
			ResultSet result;
			result = statement.executeQuery("SELECT * FROM " + table_name + " ORDER BY 1");
			System.out.println("===========< " + table_name + " >===========\n");

			ResultSetMetaData meta_data = result.getMetaData();
			int column_count = meta_data.getColumnCount();

			for (int i = 0; i < column_count; i++) {
				System.out.printf("%-25s", meta_data.getColumnName(i + 1));
			}
			System.out.println();
			while (result.next()) {
				for (int i = 0; i < column_count; i++) {
					System.out.printf("%-25s", result.getString(i + 1));
				}
				System.out.println();
			}
			System.out.println();
		} catch (SQLException e) {
		//	System.out.println(e.getLocalizedMessage());
		}

	}

	public static int getID(String table_name, String value, Statement statement) {
		try {
			ResultSet result;
			result = query_run_and_show("SELECT " + table_name + "ID FROM " + table_name + " WHERE " + table_name
					+ "Name LIKE \'" + value + "\';", statement);
			result.next();
			return result.getInt(1);
		} catch (SQLException e) {
			//System.out.println(e.getLocalizedMessage());
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {

		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your PostgreSQL JDBC Driver? Include in your library path!");
			e.printStackTrace();
			return;
		}
		System.out.println("PostgreSQL JDBC Driver Registered!");
		/// if you have a error in this part, check jdbc driver(.jar file)

		Connection connection = null;

		try {
			connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/project_movie", "postgres",
					"cse3207");
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}
		/// if you have a error in this part, check DB information (db_name, user name,
		/// password)

		if (connection != null) {
			System.out.println(connection);
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
		}
		/////////////////////////////////////////////////////
		////////// write your code on this ////////////
		/////////////////////////////////////////////////////

		String query;
		Statement statement = connection.createStatement();
		ResultSet result;

		// Process 1
		query_run("DROP TABLE movieGenre", statement);
		query_run("DROP TABLE movieObtain", statement);
		query_run("DROP TABLE actorObtain", statement);
		query_run("DROP TABLE directorObtain", statement);
		query_run("DROP TABLE casting", statement);
		query_run("DROP TABLE make", statement);
		query_run("DROP TABLE customerRate", statement);
		query_run("DROP TABLE director", statement);
		query_run("DROP TABLE actor", statement);
		query_run("DROP TABLE movie", statement);
		query_run("DROP TABLE award", statement);
		query_run("DROP TABLE genre", statement);
		query_run("DROP TABLE customer", statement);
		System.out.println("Initialized!");
		// Init Success!

		// Create table
		query_run("CREATE TABLE director("
				+ "directorID SERIAL PRIMARY KEY, directorName VARCHAR(30), dateOfBirth DATE, dateOfDeath DATE);",
				statement);
		query_run("CREATE TABLE actor("
				+ "actorID SERIAL PRIMARY KEY, actorName VARCHAR(30), dateOfBirth DATE, dateOfDeath DATE, gender VARCHAR(10));",
				statement);
		query_run("CREATE TABLE customer("
				+ "customerID SERIAL PRIMARY KEY, customerName VARCHAR(30), dateOfBirth DATE, gender VARCHAR(10));",
				statement);
		query_run("CREATE TABLE movie("
				+ "movieID SERIAL PRIMARY KEY, movieName VARCHAR(30), releaseYear VARCHAR(10), releaseMonth VARCHAR(10), releaseDate VARCHAR(10), publisherName VARCHAR(40), avgRate NUMERIC DEFAULT 0);",
				statement);
		query_run("CREATE TABLE customerRate("
				+ "customerID SERIAL, movieID SERIAL, rate INTEGER, PRIMARY KEY(customerID, movieID), FOREIGN KEY(customerID) REFERENCES customer(customerID) ON DELETE CASCADE, FOREIGN KEY(movieID) REFERENCES movie(movieID) ON DELETE CASCADE);",
				statement);
		query_run("CREATE TABLE award(" + "awardID SERIAL PRIMARY KEY , awardName VARCHAR(30) UNIQUE);", statement);
		query_run("CREATE TABLE genre(" + "genreName VARCHAR(30) PRIMARY KEY);", statement);
		query_run("CREATE TABLE movieGenre("
				+ "movieID SERIAL, genreName VARCHAR(30), PRIMARY KEY(movieID, genreName), FOREIGN KEY(movieID) REFERENCES movie(movieID) ON DELETE CASCADE, FOREIGN KEY(genreName) REFERENCES genre(genreName) ON DELETE CASCADE);",
				statement);
		query_run("CREATE TABLE movieObtain("
				+ "movieID SERIAL, awardID SERIAL, year VARCHAR(10), PRIMARY KEY(movieID, awardID), FOREIGN KEY(movieID) REFERENCES movie(movieID) ON DELETE CASCADE, FOREIGN KEY(awardID) REFERENCES award(awardID) ON DELETE CASCADE);",
				statement);
		query_run("CREATE TABLE actorObtain("
				+ "actorID SERIAL, awardID SERIAL, year VARCHAR(10), PRIMARY KEY(actorID, awardID), FOREIGN KEY(actorID) REFERENCES actor(actorID) ON DELETE CASCADE, FOREIGN KEY(awardID) REFERENCES award(awardID) ON DELETE CASCADE);",
				statement);
		query_run("CREATE TABLE directorObtain("
				+ "directorID SERIAL, awardID SERIAL, year VARCHAR(10), PRIMARY KEY(directorID, awardID), FOREIGN KEY(directorID) REFERENCES director(directorID) ON DELETE CASCADE, FOREIGN KEY(awardID) REFERENCES award(awardID) ON DELETE CASCADE);",
				statement);
		query_run("CREATE TABLE casting("
				+ "movieID SERIAL, actorID SERIAL, role VARCHAR(30), PRIMARY KEY(movieID, actorID), FOREIGN KEY(movieID) REFERENCES movie(movieID) ON DELETE CASCADE, FOREIGN KEY(actorID) REFERENCES actor(actorID) ON DELETE CASCADE);",
				statement);
		query_run("CREATE TABLE make("
				+ "movieID SERIAL, directorID SERIAL, PRIMARY KEY(movieID, directorID), FOREIGN KEY(movieID) REFERENCES movie(movieID) ON DELETE CASCADE, FOREIGN KEY(directorID) REFERENCES director(directorID) ON DELETE CASCADE);",
				statement);
		System.out.println("Table created!");
		// Create table Success!

		// trigger

		query_run("CREATE OR REPLACE FUNCTION avg_rate_func() RETURNS TRIGGER AS $$ " + "DECLARE rate_count INTEGER; "
				+ "BEGIN "
				+ "SELECT COUNT(*) INTO rate_count FROM customerRate natural join movie WHERE movie.movieID = new.movieID; "
				+ "UPDATE movie " + "SET avgRate = ROUND(((avgRate * (rate_count - 1)) + new.rate) / rate_count,2) "
				+ "WHERE movie.movieID = new.movieID; " + "RETURN new; " + "END; " + "$$ language plpgsql;", statement);

		query_run("CREATE TRIGGER cal_avg_rate AFTER INSERT OR UPDATE ON customerRate FOR EACH ROW "
				+ "EXECUTE PROCEDURE avg_rate_func();", statement);

		//

		// Data insert

		query_run("INSERT INTO director(directorName, dateOfBirth) VALUES " + "(\'Tim Burton\', \'1958.8.25\'),"
				+ "(\'David Fincher\', \'1962.8.28\')," + "(\'Christopher Nolan\', \'1970.7.30\');", statement);// director
																												// initial
																												// data

		query_run("INSERT INTO customer(customerName, dateOfBirth, gender) VALUES "
				+ "(\'Ethan\', \'1997.11.14\',\'Male\')," + "(\'John\', \'1978.01.23\',\'Male\'),"
				+ "(\'Hayden\', \'1980.05.04\',\'Female\')," + "(\'Jill\', \'1981.04.17\',\'Female\'),"
				+ "(\'Bell\', \'1990.05.14\',\'Female\');", statement);// customer initial data

		query_run("INSERT INTO actor(actorName, dateOfBirth, gender) VALUES "
				+ "(\'Johnny Depp\', \'1963.6.9\',\'Male\')," + "(\'Winona Ryder\', \'1971.10.29\',\'Female\'),"
				+ "(\'Mia Wasikowska\', \'1989.10.14\',\'Female\')," + "(\'Christian Bale\', \'1974.1.30\',\'Male\'),"
				+ "(\'Heath Ledger\', \'1979.4.4\',\'Male\')," + "(\'Jesse Eisenberg\', \'1983.10.5\',\'Male\'),"
				+ "(\'Justin Timberlake\', \'1981.1.31\',\'Male\')," + "(\'Fionn Whitehead\', \'1997.7.18\',\'Male\'),"
				+ "(\'Tom Hardy\', \'1977.9.15\',\'Male\');", statement); // customer initial data

		query_run("UPDATE actor SET dateOfDeath = '2008.1.22' WHERE actorName LIKE \'Heath Ledger\';", statement);// Heath
																													// LedgerÀÇ
																													// ¼öÁ¤

		query_run(
				"INSERT INTO genre VALUES " + "(\'Fantasy\')," + "(\'Romance\')," + "(\'Adventure\')," + "(\'Family\'),"
						+ "(\'Drama\')," + "(\'Action\')," + "(\'Mystery\')," + "(\'Thriller\')," + "(\'War\');",
				statement);// genre initial data

		query_run("INSERT INTO movie(movieName, releaseYear, releaseMonth, releaseDate, publisherName) VALUES"
				+ "(\'Edward Scissorhands\', \'1991\', \'06\' , \'29\', \'20th Century Fox Presents\'),"
				+ "(\'Alice In Wonderland\', \'2010\', \'03\' , \'04\', \'Korea Sony Pictures\'),"
				+ "(\'The Social Network\', \'2010\', \'11\' , \'18\', \'Korea Sony Pictures\'),"
				+ "(\'The Dark Knight\', \'2008\', \'08\' , \'06\', \'Warner Brothers Korea\'),"
				+ "(\'Dunkirk\', \'2017\', \'07\' , \'13\', \'Warner Brothers Korea\');", statement);// movie initial
																										// data
		query_run("INSERT INTO make VALUES" + "(1, 1)," + "(2,1)," + "(3,2)," + "(4, 3)," + "(5,3);", statement);// make
																													// initial
																													// data

		query_run("INSERT INTO casting VALUES" + "(1, 1, \'Main actor\')," + "(1, 2, \'Main actor\'),"
				+ "(2,1, \'Main actor\')," + "(2,3, \'Main actor\')," + "(3,6, \'Main actor\'),"
				+ "(3,7, \'Supporting Actor\')," + "(4, 4, \'Main actor\')," + "(4, 5, \'Main actor\'),"
				+ "(5,8, \'Main actor\')," + "(5,9, \'Supporting Actor\');", statement);// casting initial data

		query_run("INSERT INTO movieGenre VALUES" + "(1,\'Fantasy\')," + "(1,\'Romance\')," + "(2,\'Fantasy\'),"
				+ "(2,\'Adventure\')," + "(2,\'Family\')," + "(3,\'Drama\')," + "(4,\'Action\')," + "(4,\'Drama\'),"
				+ "(4,\'Mystery\')," + "(4,\'Thriller\')," + "(5,\'Action\')," + "(5,\'Drama\')," + "(5,\'Thriller\'),"
				+ "(5,\'War\');", statement);// movieGenre initial data

		System.out.println("Initial data inserted!\n\n");

		// Data insert Success!

		// Process 2
		System.out.println("===Queries Start===\n");
		int actorID = 0, movieID = 0, directorID = 0, awardID = 0, customerID = 0;

		// 2.1
		System.out.println("2.1. Winona Ryder won the ¡°Best supporting actor¡± award in 1994");
		query = "INSERT INTO award(awardName) VALUES (\'Best supporting actor\') ON CONFLICT (awardName) DO NOTHING;";
		query_run_and_show(query, statement);
		actorID = getID("actor", "Winona Ryder", statement);
		awardID = getID("award", "Best supporting actor", statement);
		query = "INSERT INTO actorObtain VALUES (" + actorID + "," + awardID + ", 1994);";
		query_run_and_show(query, statement);
		show_table("award", statement);
		show_table("actorObtain", statement);

		// 2.2
		System.out.println("2.2. Tom Hardy won the ¡°Best supporting actor¡± award in 2018");
		query = "INSERT INTO award(awardName) VALUES (\'Best supporting actor\') ON CONFLICT (awardName) DO NOTHING;";
		query_run_and_show(query, statement);
		actorID = getID("actor", "Tom Hardy", statement);
		awardID = getID("award", "Best supporting actor", statement);
		query = "INSERT INTO actorObtain VALUES (" + actorID + "," + awardID + ", 2018);";
		query_run_and_show(query, statement);
		show_table("award", statement);
		show_table("actorObtain", statement);

		// 2.3
		System.out.println("2.3. Heath Ledger won the ¡°Best villain actor¡± award in 2009");
		query = "INSERT INTO award(awardName) VALUES (\'Best villain actor\') ON CONFLICT (awardName) DO NOTHING;";
		query_run_and_show(query, statement);
		actorID = getID("actor", "Heath Ledger", statement);
		awardID = getID("award", "Best villain actor", statement);
		query = "INSERT INTO actorObtain VALUES (" + actorID + "," + awardID + ", 2009);";
		query_run_and_show(query, statement);
		show_table("award", statement);
		show_table("actorObtain", statement);

		// 2.4
		System.out.println("2.4. Johnny Depp won the ¡°Best main actor¡± award in 2011");
		query = "INSERT INTO award(awardName) VALUES (\'Best main actor\') ON CONFLICT (awardName) DO NOTHING;";
		query_run_and_show(query, statement);
		actorID = getID("actor", "Johnny Depp", statement);
		awardID = getID("award", "Best main actor", statement);
		query = "INSERT INTO actorObtain VALUES (" + actorID + "," + awardID + ", 2011);";
		query_run_and_show(query, statement);
		show_table("award", statement);
		show_table("actorObtain", statement);

		// 2.5
		System.out.println("2.5. Edward Scissorhands won the ¡°Best fantasy movie¡± award in 1991");
		query = "INSERT INTO award(awardName) VALUES (\'Best fantasy movie\') ON CONFLICT (awardName) DO NOTHING;";
		query_run_and_show(query, statement);
		movieID = getID("movie", "Edward Scissorhands", statement);
		awardID = getID("award", "Best fantasy movie", statement);
		query = "INSERT INTO movieObtain VALUES (" + movieID + "," + awardID + ", 1991);";
		query_run_and_show(query, statement);
		show_table("award", statement);
		show_table("movieObtain", statement);

		// 2.6
		System.out.println("2.6. Alice In Wonderland won the ¡°Best fantasy movie¡± award in 2011");
		query = "INSERT INTO award(awardName) VALUES (\'Best fantasy movie\') ON CONFLICT (awardName) DO NOTHING;";
		query_run_and_show(query, statement);
		movieID = getID("movie", "Alice In Wonderland", statement);
		awardID = getID("award", "Best fantasy movie", statement);
		query = "INSERT INTO movieObtain VALUES (" + movieID + "," + awardID + ", 2011);";
		query_run_and_show(query, statement);
		show_table("award", statement);
		show_table("movieObtain", statement);

		// 2.7
		System.out.println("2.7. The Dark Knight won the ¡°Best picture¡± award in 2009");
		query = "INSERT INTO award(awardName) VALUES (\'Best picture\') ON CONFLICT (awardName) DO NOTHING;";
		query_run_and_show(query, statement);
		movieID = getID("movie", "The Dark Knight", statement);
		awardID = getID("award", "Best picture", statement);
		query = "INSERT INTO movieObtain VALUES (" + movieID + "," + awardID + ", 2009);";
		query_run_and_show(query, statement);
		show_table("award", statement);
		show_table("movieObtain", statement);

		// 2.8
		System.out.println("2.8. Christopher Nolan won the ¡°Best director¡± award in 2018");
		query = "INSERT INTO award(awardName) VALUES (\'Best director\') ON CONFLICT (awardName) DO NOTHING;";
		query_run_and_show(query, statement);
		directorID = getID("director", "Christopher Nolan", statement);
		awardID = getID("award", "Best director", statement);
		query = "INSERT INTO directorObtain VALUES (" + directorID + "," + awardID + ", 2009);";
		query_run_and_show(query, statement);
		show_table("award", statement);
		show_table("directorObtain", statement);
		// Process 2 end

		System.out.println();

		// Process 3 start
		Vector<Integer> temp_vector = new Vector<Integer>();
		// 3.1
		System.out.println("3.1 Ethan rates 5 to ¡°Dunkirk¡±");
		customerID = getID("customer", "Ethan", statement);
		movieID = getID("movie", "Dunkirk", statement);
		query = "INSERT INTO customerRate VALUES (" + customerID + " , " + movieID + " , 5);";
		query_run_and_show(query, statement);
		show_table("customerRate", statement);
		show_table("movie", statement);

		// 3.2
		System.out.println("3.2 Bell rates 5 to the movies whose director is ¡°Tim Burton¡±");
		customerID = getID("customer", "Bell", statement);
		directorID = getID("director", "Tim Burton", statement);
		query = "SELECT movieID FROM movie natural join make WHERE directorID = " + directorID + ";";
		result = query_run_and_show(query, statement);
		while (result.next()) {
			temp_vector.add(result.getInt(1));
		}
		for (int i = 0; i < temp_vector.size(); i++) {
			query = "INSERT INTO customerRate VALUES (" + customerID + " , " + temp_vector.elementAt(i) + " , 5);";
			query_run_and_show(query, statement);
		}
		show_table("customerRate", statement);
		show_table("movie", statement);
		temp_vector.clear();

		// 3.3
		System.out.println("3.3 Jill rates 4 to the movies whose main actor is female");
		customerID = getID("customer", "Jill", statement);
		query = "SELECT DISTINCT movieID FROM casting NATURAL JOIN actor WHERE gender LIKE \'Female\';";
		result = query_run_and_show(query, statement);
		while (result.next()) {
			temp_vector.add(result.getInt(1));
		}
		for (int i = 0; i < temp_vector.size(); i++) {
			query = "INSERT INTO customerRate VALUES (" + customerID + " , " + temp_vector.elementAt(i) + " , 4);";
			query_run_and_show(query, statement);
		}
		show_table("customerRate", statement);
		show_table("movie", statement);
		temp_vector.clear();

		// 3.4
		System.out.println("3.4 Hayden rates 4 to the fantasy movies");
		customerID = getID("customer", "Hayden", statement);
		query = "SELECT DISTINCT movieID FROM movieGenre WHERE genreName LIKE \'Fantasy\';";
		result = query_run_and_show(query, statement);
		while (result.next()) {
			temp_vector.add(result.getInt(1));
		}
		for (int i = 0; i < temp_vector.size(); i++) {
			query = "INSERT INTO customerRate VALUES (" + customerID + " , " + temp_vector.elementAt(i) + " , 4);";
			query_run_and_show(query, statement);
		}
		show_table("customerRate", statement);
		show_table("movie", statement);
		temp_vector.clear();

		// 3.5
		System.out.println("3.5 John rates 5 to the movies whose director won the ¡°Best director¡± award");
		customerID = getID("customer", "John", statement);
		awardID = getID("award", "Best director", statement);
		query = "SELECT DISTINCT movieID FROM make NATURAL JOIN directorObtain WHERE awardID = " + awardID + ";";
		result = query_run_and_show(query, statement);
		while (result.next()) {
			temp_vector.add(result.getInt(1));
		}
		for (int i = 0; i < temp_vector.size(); i++) {
			query = "INSERT INTO customerRate VALUES (" + customerID + " , " + temp_vector.elementAt(i) + " , 5);";
			query_run_and_show(query, statement);
		}
		show_table("customerRate", statement);
		show_table("movie", statement);
		temp_vector.clear();
		// Process 3 end

		// Query 4
		System.out.println("4. Select the names of the movies whose actor are dead.");
		query = "SELECT movieName FROM movie NATURAL JOIN casting NATURAL JOIN actor WHERE dateOfDeath IS NOT NULL;";
		result = query_run_and_show(query, statement);
		System.out.println("====Result====");
		while (result.next()) {
			System.out.println(result.getString(1));
		}
		System.out.println();

		// Query 5
		System.out.println("5. Select the names of the directors who cast the same actor more than once.");
		query = "SELECT directorName FROM director NATURAL JOIN make NATURAL JOIN casting GROUP BY directorID , actorID HAVING COUNT(*) >= 2;";
		result = query_run_and_show(query, statement);
		System.out.println("====Result====");
		while (result.next()) {
			System.out.println(result.getString(1));
		}
		System.out.println();

		// Query 6
		System.out.println("6. Select the names of the movies and the genres, where movies have the common genre.");
		query = "SELECT genreName, movieName FROM movie NATURAL JOIN movieGenre WHERE genreName IN (SELECT genreName FROM movieGenre GROUP BY genreName HAVING COUNT(*) >= 2) ORDER BY genreName;";
		result = query_run_and_show(query, statement);
		String genre = "";
		System.out.println("====Result====");
		while (result.next()) {
			if (!genre.equals(result.getString(1))) {
				genre = result.getString(1);
				System.out.print("\n" + genre + " : " + result.getString(2));
			} else {
				System.out.print(" , " + result.getString(2));
			}
		}
		System.out.println("\n");
		
		// Query 7
		System.out.println("7. Delete the movies whose director or actor did not get any award and delete data from related tables.");
		query = "DELETE FROM movie WHERE movieID NOT IN (SELECT DISTINCT movieID FROM movie NATURAL JOIN casting NATURAL JOIN make WHERE actorID IN (SELECT DISTINCT actorID FROM actorObtain) AND directorID IN (SELECT DISTINCT directorID FROM directorObtain));";
		result = query_run_and_show(query, statement);
		show_table("movie", statement);
		show_table("movieGenre", statement);
		show_table("movieObtain", statement);
		show_table("casting", statement);
		show_table("make", statement);
		show_table("customerRate", statement);
		
		//Query 8
		System.out.println("8. Delete all customers and delete data from related tables.");
		query = "DELETE FROM customer;";
		query_run_and_show(query, statement);
		show_table("customer",statement);
		show_table("customerRate",statement);
		
		//Query 9
		System.out.println("9. Delete all tables and data (make the database empty).");
		query_run("DROP TABLE movieGenre", statement);
		query_run("DROP TABLE movieObtain", statement);
		query_run("DROP TABLE actorObtain", statement);
		query_run("DROP TABLE directorObtain", statement);
		query_run("DROP TABLE casting", statement);
		query_run("DROP TABLE make", statement);
		query_run("DROP TABLE customerRate", statement);
		query_run("DROP TABLE director", statement);
		query_run("DROP TABLE actor", statement);
		query_run("DROP TABLE movie", statement);
		query_run("DROP TABLE award", statement);
		query_run("DROP TABLE genre", statement);
		query_run("DROP TABLE customer", statement);
		System.out.println("Database cleared!");
		// DROP Success!

		connection.close();
	}
}