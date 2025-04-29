# README: Running the Star Schema and OLAP Queries with Java and MySQL Workbench

## Overview
This document provides step-by-step instructions to set up the `metro_dw` database, execute ETL and OLAP queries using a Java application, and run additional OLAP queries directly in **MySQL Workbench**.

---

## Prerequisites
Ensure the following software and files are available:

- **MySQL Server** and **MySQL Workbench**: To host and manage the database.
- **Eclipse IDE**: For executing the Java application.
- **MySQL JDBC Driver**: Connector/J for enabling Java to communicate with MySQL.
- **Star Schema SQL File**: `starSchema.sql` – Creates the `metro_dw` database.
- **Java Application**: `MeshJoin.java` – Performs ETL and executes OLAP queries.
- **OLAP Queries File**: `olap_queries.sql` – Contains additional OLAP queries to run in MySQL Workbench.

---

## Step-by-Step Instructions

### Step 1: Setting Up the Database in MySQL Workbench
1. Open **MySQL Workbench** and connect to your MySQL server.
2. Run the `starSchema.sql` file:
   - Go to **File > Open SQL Script** and select the `starSchema.sql` file.
   - Click **Execute (Lightning Bolt Icon)** to create the `metro_dw` database and associated tables.
3. Verify that the database `metro_dw` has been successfully created:
   - Expand the **Schemas** tab and check for the `metro_dw` schema.

---

### Step 2: Preparing the Java Application in Eclipse
1. Open **Eclipse IDE** and import the project containing `MeshJoin.java`.
2. Add the **MySQL JDBC Driver** to your project:
   - Right-click the project and select **Build Path > Add External JARs**.
   - Select the downloaded **MySQL Connector/J** JAR file.
3. Ensure the database connection details in `MeshJoin.java` (e.g., username, password, database URL) are correct.

---

### Step 3: Running the Java Application
1. In **Eclipse**, open the `MeshJoin.java` file.
2. Run the program:
   - Right-click the file and select **Run As > Java Application**.
3. The application will:
   - Perform the **ETL (Extract, Transform, Load)** process to populate the `metro_dw` database.
   - Execute OLAP queries programmatically and output the results in the console.

---

### Step 4: Running OLAP Queries in MySQL Workbench
1. Open **MySQL Workbench** and connect to your MySQL server.
2. Run the `olap_queries.sql` file:
   - Go to **File > Open SQL Script** and select the `olap_queries.sql` file.
   - Click **Execute (Lightning Bolt Icon)** to run the queries on the `metro_dw` database.
3. Review the results of the OLAP queries in the **Results Grid**.

---

## Notes
- Ensure the `metro_dw` database is created before running `MeshJoin.java` or `olap_queries.sql`.
- The `MeshJoin.java` file is responsible for automating ETL and query execution, while `olap_queries.sql` allows you to manually verify and execute additional OLAP queries.
- Modify the connection details in `MeshJoin.java` (if needed) to match your MySQL server setup.

---

## Troubleshooting
1. **Database Connection Issues**:
   - Verify the database credentials (username, password, and URL) in `MeshJoin.java`.
   - Ensure the MySQL server is running and accessible.
2. **SQL Errors**:
   - Check the syntax of the SQL files if errors occur while running queries.
   - Ensure all necessary tables are created and populated before running OLAP queries.
3. **JDBC Driver Errors**:
   - Confirm that the MySQL Connector/J is correctly added to the project's build path.

---

## Additional Resources
- [MySQL Documentation](https://dev.mysql.com/doc/)
- [Eclipse IDE](https://www.eclipse.org/)
- [JDBC Overview](https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/)
