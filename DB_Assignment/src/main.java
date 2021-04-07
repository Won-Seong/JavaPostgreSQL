import java.sql.*;
import java.io.*;

public class main {

	public static void query_run(String query, Statement statement) {
		try {
			statement.executeQuery(query);
		} catch (SQLException e) {
			System.out.println(e.getLocalizedMessage());
		}
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
		ResultSet result_set;

		// Init process
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
				+ "directorID INTEGER PRIMARY KEY, directorName VARCHAR(30), dateOfBirth DATE, dateOfDeath DATE);",
				statement);
		query_run("CREATE TABLE actor("
				+ "actorID INTEGER PRIMARY KEY, actorName VARCHAR(30), dateOfBirth DATE, dateOfDeath DATE, gender VARCHAR(10));",
				statement);
		query_run("CREATE TABLE customer("
				+ "customerID INTEGER PRIMARY KEY, customerName VARCHAR(30), dateOfBirth DATE, gender VARCHAR(10));",
				statement);
		query_run("CREATE TABLE movie("
				+ "movieID INTEGER PRIMARY KEY, movieName VARCHAR(30), releaseYear VARCHAR(10), releaseMonth VARCHAR(10), releaseDate VARCHAR(10), publisherName VARCHAR(20), avgRate INTEGER);",
				statement);
		query_run("CREATE TABLE customerRate("
				+ "customerID INTEGER, movieID INTEGER, rate INTEGER, PRIMARY KEY(customerID, movieID), FOREIGN KEY(customerID) REFERENCES customer(customerID), FOREIGN KEY(movieID) REFERENCES movie(movieID));",
				statement);
		query_run("CREATE TABLE award(" + "awardID INTEGER PRIMARY KEY, awardName VARCHAR(30));", statement);
		query_run("CREATE TABLE genre(" + "genreName VARCHAR(20) PRIMARY KEY);", statement);
		query_run("CREATE TABLE movieGenre("
				+ "movieID INTEGER, genreName VARCHAR(30), PRIMARY KEY(movieID, genreName), FOREIGN KEY(movieID) REFERENCES movie(movieID), FOREIGN KEY(genreName) REFERENCES genre(genreName));",
				statement);
		query_run("CREATE TABLE movieObtain("
				+ "movieID INTEGER, awardID INTEGER, year VARCHAR(10), PRIMARY KEY(movieID, awardID), FOREIGN KEY(movieID) REFERENCES movie(movieID), FOREIGN KEY(awardID) REFERENCES award(awardID));",
				statement);
		query_run("CREATE TABLE actorObtain("
				+ "actorID INTEGER, awardID INTEGER, year VARCHAR(10), PRIMARY KEY(actorID, awardID), FOREIGN KEY(actorID) REFERENCES actor(actorID), FOREIGN KEY(awardID) REFERENCES award(awardID));",
				statement);
		query_run("CREATE TABLE directorObtain("
				+ "directorID INTEGER, awardID INTEGER, year VARCHAR(10), PRIMARY KEY(directorID, awardID), FOREIGN KEY(directorID) REFERENCES director(directorID), FOREIGN KEY(awardID) REFERENCES award(awardID));",
				statement);
		query_run("CREATE TABLE casting("
				+ "movieID INTEGER, actorID INTEGER, role VARCHAR(30), PRIMARY KEY(movieID, actorID), FOREIGN KEY(movieID) REFERENCES movie(movieID), FOREIGN KEY(actorID) REFERENCES actor(actorID));",
				statement);
		query_run("CREATE TABLE make("
				+ "movieID INTEGER, directorID INTEGER, PRIMARY KEY(movieID, directorID), FOREIGN KEY(movieID) REFERENCES movie(movieID), FOREIGN KEY(directorID) REFERENCES director(directorID));",
				statement);
		System.out.println("Table created!");
		// Create table Success!

		// Data insert

		query_run("INSERT INTO director(directorID, directorName, dateOfBirth) VALUES "
				+ "(1,\'Tim Burton\', \'1958.8.25\')," + "(2,\'David Fincher\', \'1962.8.28\'),"
				+ "(3,\'Christopher Nolan\', \'1970.7.30\');", statement);// director initial data

		query_run("INSERT INTO customer(customerID, customerName, dateOfBirth, gender) VALUES "
				+ "(1,\'Ethan\', \'1997.11.14\',\'Male\')," + "(2,\'John\', \'1978.01.23\',\'Male\'),"
				+ "(3,\'Hayden\', \'1980.05.04\',\'Female\')," + "(4,\'Jill\', \'1981.04.17\',\'Female\'),"
				+ "(5,\'Bell\', \'1990.05.14\',\'Female\');", statement);// customer initial data

		query_run("INSERT INTO actor(actorID, actorName, dateOfBirth, gender) VALUES "
				+ "(1,\'Johnny Depp\', \'1963.6.9\',\'Male\')," + "(2,\'Winona Ryder\', \'1971.10.29\',\'Female\'),"
				+ "(3,\'Mia Wasikowska\', \'1989.10.14\',\'Female\'),"
				+ "(4,\'Christian Bale\', \'1974.1.30\',\'Male\')," + "(5,\'Heath Ledger\', \'1979.4.4\',\'Male\'),"
				+ "(6,\'Jesse Eisenberg\', \'1983.10.5\',\'Male\'),"
				+ "(7,\'Justin Timberlake\', \'1981.1.31\',\'Male\'),"
				+ "(8,\'Fionn Whitehead\', \'1997.7.18\',\'Male\')," + "(9,\'Tom Hardy\', \'1977.9.15\',\'Male\');",
				statement); // customer initial data

		query_run("UPDATE actor SET dateOfDeath = '2008.1.22' WHERE actorName LIKE \'Heath Ledger\';", statement);// Heath Ledger의 수정

		query_run(
				"INSERT INTO genre VALUES " + "(\'Fantasy\')," + "(\'Romance\')," + "(\'Adventure\')," + "(\'Family\'),"
						+ "(\'Drama\')," + "(\'Action\')," + "(\'Mystery\')," + "(\'Thriller\')," + "(\'War\');",
				statement);// genre initial data
		
		//무비 넣으면 됨
		
		

		query = "SELECT * FROM actor ORDER BY actorID;";
		query_run(query, statement);
		result_set = statement.executeQuery(query);
		while (result_set.next()) {
			System.out.println(result_set.getInt(1) + result_set.getString(2) + result_set.getDate(3)
					+ result_set.getDate(4) + result_set.getString(5));
		}
		System.out.println("Initial data inserted!");
		// Data insert Success!

		// DROP all tables and data
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