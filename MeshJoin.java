import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.*;
import java.util.Scanner;

class Customer {
    String customerId;
    String customerName;
    String gender;

    public Customer(String customerId, String customerName, String gender) {
        this.customerId = customerId;
        this.customerName = customerName;
        this.gender = gender;
    }
}

class Product {
    String productId;
    String productName;
    double productPrice;
    String supplierId;
    String supplierName;
    String storeId;
    String storeName;

    public Product(String productId, String productName, double productPrice, String supplierId, String supplierName, String storeId, String storeName) {
        this.productId = productId;
        this.productName = productName;
        this.productPrice = productPrice;
        this.supplierId = supplierId;
        this.supplierName = supplierName;
        this.storeId = storeId;
        this.storeName = storeName;
    }
}

class Transaction {
    String orderId;
    String orderDate;
    String productId;
    int quantityOrdered;
    String customerId;
    String timeId;

    public Transaction(String orderId, String orderDate, String productId, int quantityOrdered, String customerId, String timeId) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.productId = productId;
        this.quantityOrdered = quantityOrdered;
        this.customerId = customerId;
        this.timeId = timeId;
    }
}

public class MeshJoin {

    public static void main(String[] args) throws InterruptedException, ExecutionException, SQLException {
        // File paths
        String customerFilePath = "C:\\Users\\Abdul rehman\\OneDrive\\Desktop\\i21-1780 semester_7\\dataWarehousing\\dw_project\\customers_data.csv";
        String productFilePath = "C:\\Users\\Abdul rehman\\OneDrive\\Desktop\\i21-1780 semester_7\\dataWarehousing\\dw_project\\products_data.csv";
        String transactionFilePath = "C:\\Users\\Abdul rehman\\OneDrive\\Desktop\\i21-1780 semester_7\\dataWarehousing\\dw_project\\transactions.csv";

        // Scanner for user input
        Scanner scanner = new Scanner(System.in);

        // Ask user for MySQL credentials
        System.out.print("Enter MySQL username: ");
        String dbUsername = scanner.nextLine();

        System.out.print("Enter MySQL password: ");
        String dbPassword = scanner.nextLine();

        // Thread pool
        ExecutorService executor = Executors.newFixedThreadPool(3);

        // Phase 1: Extract
        Future<List<Customer>> customersFuture = executor.submit(() -> readCustomers(customerFilePath));
        Future<List<Product>> productsFuture = executor.submit(() -> readProducts(productFilePath));
        Future<List<Transaction>> transactionsFuture = executor.submit(() -> readTransactions(transactionFilePath));

        // Wait for extraction to complete
        List<Customer> customers = customersFuture.get();
        List<Product> products = productsFuture.get();
        List<Transaction> transactions = transactionsFuture.get();

        // Phase 2: Transform and Load using threads
        Runnable transformAndLoadTask = () -> {
            try {
                applyMeshJoin(customers, products, transactions, dbUsername, dbPassword);
                System.out.println("Starting Data Warehouse Query Execution...");

                // Q1: Top Revenue Generating Products by Day and Month
                System.out.println("\nExecuting Query 1: Top Revenue Generating Products by Day and Month");
                getTopRevenueGeneratingProductsByDayAndMonth(dbUsername, dbPassword, 2019);
                System.out.println("Query 1 Execution Completed.\n");

                // Q2: Store Revenue Growth Rate Quarterly for 2019
                System.out.println("Executing Query 2: Store Revenue Growth Rate Quarterly for 2019");
                getStoreRevenueGrowthRateQuarterly2019(dbUsername, dbPassword);
                System.out.println("Query 2 Execution Completed.\n");

                // Q3: Detailed Supplier Sales Contribution
                System.out.println("Executing Query 3: Detailed Supplier Sales Contribution");
                getDetailedSupplierSalesContribution(dbUsername, dbPassword);
                System.out.println("Query 3 Execution Completed.\n");

                // Q4: Seasonal Sales Contribution
                System.out.println("Executing Query 4: Seasonal Sales Contribution");
                getSeasonalSalesContribution(dbUsername, dbPassword);
                System.out.println("Query 4 Execution Completed.\n");

                // Q5: Supplier Revenue Volatility
                System.out.println("Executing Query 5: Supplier Revenue Volatility");
                getSupplierRevenueVolatility(dbUsername, dbPassword);
                System.out.println("Query 5 Execution Completed.\n");

                // Q6: Top Product Pairs
                System.out.println("Executing Query 6: Top Product Pairs");
                getTopProductPairs(dbUsername, dbPassword);
                System.out.println("Query 6 Execution Completed.\n");

                // Q7: Revenue by Product and Supplier
                System.out.println("Executing Query 7: Revenue by Product and Supplier");
                getRevenueByProductAndSupplier(dbUsername, dbPassword);
                System.out.println("Query 7 Execution Completed.\n");

                // Q8: Product Revenue
                System.out.println("Executing Query 8: Product Revenue");
                getProductRevenue(dbUsername, dbPassword);
                System.out.println("Query 8 Execution Completed.\n");

                // Q9: Sales Spikes
                System.out.println("Executing Query 9: Sales Spikes");
                getSalesSpikes(dbUsername, dbPassword);
                System.out.println("Query 9 Execution Completed.\n");

                // Q10: Create Store Quarterly Sales View
                System.out.println("Executing Query 10: Create Store Quarterly Sales View");
                createStoreQuarterlySalesView(dbUsername, dbPassword);
                System.out.println("Query 10 Execution Completed.\n");

                System.out.println("All Queries Executed Successfully.");
                

            } catch (SQLException e) {
                e.printStackTrace();
            }
        };
        Future<?> transformAndLoadFuture = executor.submit(transformAndLoadTask);

        // Wait for transform and load to complete
        transformAndLoadFuture.get();

        // Shutdown the executor
        executor.shutdown();
        System.out.println("ETL processing completed.");
    }

    public static List<Customer> readCustomers(String filePath) {
        List<Customer> customers = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                String customerId = parts[0];
                String customerName = parts[1];
                String gender = parts[2];
                customers.add(new Customer(customerId, customerName, gender));
            }
            System.out.println("Extracted customers data.");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return customers;
    }

    public static List<Product> readProducts(String filePath) {
        List<Product> products = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            boolean isHeader = true; // Flag to skip the header row
            while ((line = br.readLine()) != null) {
                if (isHeader) {
                    isHeader = false; // Skip the first line (header)
                    continue;
                }
                String[] parts = line.split(",");
                if (parts.length < 7) continue; // Skip invalid rows
                String productId = parts[0];
                String productName = parts[1];
                double productPrice = Double.parseDouble(parts[2].replace("$", ""));
                String supplierId = parts[3];
                String supplierName = parts[4];
                String storeId = parts[5];
                String storeName = parts[6];
                products.add(new Product(productId, productName, productPrice, supplierId, supplierName, storeId, storeName));
            }
            System.out.println("Extracted products data.");
        } catch (IOException | NumberFormatException e) {
            e.printStackTrace();
        }
        return products;
    }

    public static List<Transaction> readTransactions(String filePath) {
        List<Transaction> transactions = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            boolean isHeader = true; // Flag to skip the header row
            while ((line = br.readLine()) != null) {
                if (isHeader) {
                    isHeader = false; // Skip the first line (header)
                    continue;
                }
                String[] parts = line.split(",");
                if (parts.length < 6) continue; // Skip invalid rows
                String orderId = parts[0];
                String orderDate = parts[1].split(" ")[0]; // Extract only the date part
                String productId = parts[2];
                int quantityOrdered = Integer.parseInt(parts[3]); // Parse quantity as an integer
                String customerId = parts[4];
                String timeId = parts[5];
                transactions.add(new Transaction(orderId, orderDate, productId, quantityOrdered, customerId, timeId));
            }
            System.out.println("Extracted transactions data.");
        } catch (IOException | NumberFormatException e) {
            e.printStackTrace();
        }
        return transactions;
    }
    
 // Query 1
	
    public static void getTopRevenueGeneratingProductsByDayAndMonth(String dbUsername, String dbPassword, int year) throws SQLException {
     
        String dbUrl = "jdbc:mysql://localhost:3306/metro_dw";
        
        // SQL Query to find the top 5 revenue-generating products by weekday and weekend with monthly drill-down
        String query = """
                SELECT 
                    MONTH(t.OrderDate) AS Month,
                    CASE 
                        WHEN DAYOFWEEK(t.OrderDate) IN (1, 7) THEN 'Weekend'
                        ELSE 'Weekday'
                    END AS DayType,
                    p.ProductID,
                    p.ProductName,
                    SUM(t.Quantity * p.ProductPrice) AS TotalRevenue
                FROM 
                    Transaction_Fact t
                JOIN 
                    Product_Dimension p ON t.ProductID = p.ProductID
                WHERE 
                    YEAR(t.OrderDate) = ?
                GROUP BY 
                    MONTH(t.OrderDate), DayType, p.ProductID
                ORDER BY 
                    TotalRevenue DESC
                LIMIT 10;
                """;

        // Establishing MySQL connection
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query)) {
             
            // Set the year dynamically in the query
            stmt.setInt(1, year);

            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("Top 5 Revenue-Generating Products (Weekday/Weekend) with Monthly Drill-Down:");
                System.out.println("--------------------------------------------------------------");

                // Process the results
                while (rs.next()) {
                    int month = rs.getInt("Month");
                    String dayType = rs.getString("DayType");
                    String productId = rs.getString("ProductID");
                    String productName = rs.getString("ProductName");
                    double totalRevenue = rs.getDouble("TotalRevenue");

                    // Display the result
                    System.out.printf("Month: %d, Day Type: %s, Product: %s (ID: %s), Revenue: $%.2f%n", 
                            month, dayType, productName, productId, totalRevenue);
                }
            }
        }
    }
	 
    
    // query 2
	
    public static void getStoreRevenueGrowthRateQuarterly2019(String dbUsername, String dbPassword) throws SQLException {
        String dbUrl = "jdbc:mysql://localhost:3306/metro_dw"; 

        // SQL Query to calculate store revenue growth rate per quarter for 2019
        String query = """
            WITH QuarterlyRevenue AS (
                -- Calculate the total revenue for each store for each quarter of 2019
                SELECT 
                    t.StoreID,
                    EXTRACT(QUARTER FROM t.OrderDate) AS Quarter,
                    SUM(t.Quantity * p.ProductPrice) AS TotalRevenue
                FROM 
                    Transaction_Fact t
                JOIN 
                    Product_Dimension p ON t.ProductID = p.ProductID
                WHERE 
                    EXTRACT(YEAR FROM t.OrderDate) = 2019  -- Filter for 2019 (change it to 2017 if needed)
                GROUP BY 
                    t.StoreID, EXTRACT(QUARTER FROM t.OrderDate)
            ),
            RevenueGrowth AS (
                -- Calculate the revenue growth rate between consecutive quarters
                SELECT 
                    q1.StoreID,
                    q1.Quarter,
                    q1.TotalRevenue,
                    -- Calculate the growth rate between current and previous quarter
                    ((q1.TotalRevenue - COALESCE(q2.TotalRevenue, 0)) / COALESCE(q2.TotalRevenue, 1)) * 100 AS GrowthRate
                FROM 
                    QuarterlyRevenue q1
                LEFT JOIN 
                    QuarterlyRevenue q2 ON q1.StoreID = q2.StoreID AND q1.Quarter = q2.Quarter + 1
            )
            SELECT 
                StoreID,
                Quarter,
                TotalRevenue,
                GrowthRate
            FROM 
                RevenueGrowth
            ORDER BY 
                StoreID, Quarter;
        """;

        // Establishing MySQL connection
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            System.out.println("Store Revenue Growth Rate for 2019 (Quarterly):");

            // Process the results
            while (rs.next()) {
                String storeId = rs.getString("StoreID");
                int quarter = rs.getInt("Quarter");
                double totalRevenue = rs.getDouble("TotalRevenue");
                double growthRate = rs.getDouble("GrowthRate");

                System.out.println("Store ID: " + storeId + 
                                   ", Quarter: " + quarter + 
                                   ", Total Revenue: $" + totalRevenue + 
                                   ", Growth Rate: " + growthRate + "%");
            }
        }
    }

	 
    	
    // query 3
    public static void getDetailedSupplierSalesContribution(String dbUsername, String dbPassword) throws SQLException {
        String dbUrl = "jdbc:mysql://localhost:3306/metro_dw"; 

        // SQL Query to get detailed supplier sales contribution by store and product name
        String query = """
            SELECT 
                 t.StoreID,                             -- Store identifier
                 p.SupplierID,                          -- Supplier identifier
                 p.SupplierName,                        -- Supplier name (from Product_Dimension)
                 p.ProductName,                         -- Product name
                 SUM(t.Quantity * p.ProductPrice) AS TotalSalesContribution  -- Total sales contribution
            FROM 
                 Transaction_Fact t
            JOIN 
                 Product_Dimension p ON t.ProductID = p.ProductID  -- Join on Product_Dimension
            GROUP BY 
                 t.StoreID, 
                 p.SupplierID, 
                 p.SupplierName, 
                 p.ProductName
            ORDER BY 
                 t.StoreID, 
                 p.SupplierID, 
                 p.ProductName
            LIMIT 0, 50000;
        """;

        // Establishing MySQL connection
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            System.out.println("Detailed Supplier Sales Contribution by Store and Product:");

            // Process the results
            while (rs.next()) {
                String storeId = rs.getString("StoreID");
                String supplierId = rs.getString("SupplierID");
                String supplierName = rs.getString("SupplierName");
                String productName = rs.getString("ProductName");
                double totalSalesContribution = rs.getDouble("TotalSalesContribution");

                // Display the results in a structured format
                System.out.println("Store ID: " + storeId + 
                                   ", Supplier ID: " + supplierId + 
                                   ", Supplier Name: " + supplierName + 
                                   ", Product Name: " + productName + 
                                   ", Total Sales Contribution: $" + totalSalesContribution);
            }
        }
    }
    
    // query 4
    public static void getSeasonalSalesContribution(String dbUsername, String dbPassword) throws SQLException {
        String dbUrl = "jdbc:mysql://localhost:3306/metro_dw"; 

        // SQL Query to calculate seasonal sales contribution of each product
        String query = """
            SELECT 
                p.ProductName,                            -- Product Name
                CASE
                    WHEN EXTRACT(MONTH FROM t.OrderDate) IN (3, 4, 5) THEN 'Spring'
                    WHEN EXTRACT(MONTH FROM t.OrderDate) IN (6, 7, 8) THEN 'Summer'
                    WHEN EXTRACT(MONTH FROM t.OrderDate) IN (9, 10, 11) THEN 'Fall'
                    WHEN EXTRACT(MONTH FROM t.OrderDate) IN (12, 1, 2) THEN 'Winter'
                END AS Season,                            -- Seasonal Period
                EXTRACT(MONTH FROM t.OrderDate) AS Month,  -- Extract Month for Drill-Down
                SUM(t.Quantity * p.ProductPrice) AS TotalSales  -- Total Sales Contribution
            FROM 
                Transaction_Fact t
            JOIN 
                Product_Dimension p ON t.ProductID = p.ProductID  -- Join on Product Dimension Table
            GROUP BY 
                p.ProductName, 
                Season, 
                Month
            ORDER BY 
                Season, 
                Month, 
                p.ProductName;
        """;

        // Establishing MySQL connection
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            System.out.println("Seasonal Sales Contribution by Product:");

            // Process the results
            while (rs.next()) {
                String productName = rs.getString("ProductName");
                String season = rs.getString("Season");
                int month = rs.getInt("Month");
                double totalSales = rs.getDouble("TotalSales");

                // Display the results in a structured format
                System.out.println("Product Name: " + productName + 
                                   ", Season: " + season + 
                                   ", Month: " + month + 
                                   ", Total Sales: $" + totalSales);
            }
        }
    }
    
    // query 5
    public static void getSupplierRevenueVolatility(String dbUsername, String dbPassword) throws SQLException {
        String dbUrl = "jdbc:mysql://localhost:3306/metro_dw";  

        // SQL Query to calculate monthly revenue volatility by store, supplier, and product
        String query = """
            WITH MonthlyRevenue AS (
                SELECT 
                    t.StoreID,
                    p.SupplierID,
                    EXTRACT(MONTH FROM t.OrderDate) AS Month,
                    EXTRACT(YEAR FROM t.OrderDate) AS Year,
                    SUM(t.Quantity * p.ProductPrice) AS TotalRevenue
                FROM 
                    Transaction_Fact t
                JOIN 
                    Product_Dimension p ON t.ProductID = p.ProductID
                GROUP BY 
                    t.StoreID, p.SupplierID, EXTRACT(MONTH FROM t.OrderDate), EXTRACT(YEAR FROM t.OrderDate)
            ),
            RevenueWithPrevious AS (
                SELECT 
                    StoreID,
                    SupplierID,
                    Month,
                    Year,
                    TotalRevenue,
                    LAG(TotalRevenue) OVER (PARTITION BY StoreID, SupplierID ORDER BY Year, Month) AS PreviousMonthRevenue
                FROM 
                    MonthlyRevenue
            )
            SELECT 
                StoreID,
                SupplierID,
                Month,
                Year,
                TotalRevenue,
                PreviousMonthRevenue,
                CASE 
                    WHEN PreviousMonthRevenue = 0 THEN NULL
                    ELSE ((TotalRevenue - PreviousMonthRevenue) / PreviousMonthRevenue) * 100
                END AS RevenueVolatilityPercentage
            FROM 
                RevenueWithPrevious
            ORDER BY 
                StoreID, SupplierID, Year, Month;
        """;

        // Establishing MySQL connection
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            System.out.println("Supplier Revenue Volatility:");

            // Process the results
            while (rs.next()) {
                int storeId = rs.getInt("StoreID");
                int supplierId = rs.getInt("SupplierID");
                int month = rs.getInt("Month");
                int year = rs.getInt("Year");
                double totalRevenue = rs.getDouble("TotalRevenue");
                double previousMonthRevenue = rs.getDouble("PreviousMonthRevenue");
                Double revenueVolatilityPercentage = rs.getObject("RevenueVolatilityPercentage") != null 
                        ? rs.getDouble("RevenueVolatilityPercentage") 
                        : null;

                // Display the results in a structured format
                System.out.println("StoreID: " + storeId +
                        ", SupplierID: " + supplierId +
                        ", Month: " + month +
                        ", Year: " + year +
                        ", Total Revenue: $" + totalRevenue +
                        ", Previous Month Revenue: $" + previousMonthRevenue +
                        ", Revenue Volatility Percentage: " + (revenueVolatilityPercentage != null ? revenueVolatilityPercentage + "%" : "N/A"));
            }
        }
    }
    
    // query 6
    public static void getTopProductPairs(String dbUsername, String dbPassword) throws SQLException {
        String dbUrl = "jdbc:mysql://localhost:3306/metro_dw";  

        // SQL Query to calculate the most frequently paired products
        String query = """
            WITH ProductPairs AS (
                SELECT 
                    t1.OrderID,
                    t1.ProductID AS ProductID1,
                    t2.ProductID AS ProductID2
                FROM 
                    Transaction_Fact t1
                JOIN 
                    Transaction_Fact t2 ON t1.OrderID = t2.OrderID 
                WHERE 
                    t1.ProductID < t2.ProductID  -- Ensures each pair is counted only once
            ),
            ProductPairCount AS (
                SELECT 
                    ProductID1,
                    ProductID2,
                    COUNT(*) AS PairCount
                FROM 
                    ProductPairs
                GROUP BY 
                    ProductID1, ProductID2
            )
            SELECT 
                p1.ProductName AS Product1,
                p2.ProductName AS Product2,
                pp.PairCount
            FROM 
                ProductPairCount pp
            JOIN 
                Product_Dimension p1 ON pp.ProductID1 = p1.ProductID
            JOIN 
                Product_Dimension p2 ON pp.ProductID2 = p2.ProductID
            ORDER BY 
                pp.PairCount DESC
            LIMIT 5;
        """;

        // Establishing MySQL connection
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            System.out.println("Top 5 Most Frequently Paired Products:");

            // Process the results
            while (rs.next()) {
                String product1 = rs.getString("Product1");
                String product2 = rs.getString("Product2");
                int pairCount = rs.getInt("PairCount");

                // Display the results
                System.out.println("Product 1: " + product1 + ", Product 2: " + product2 + ", Pair Count: " + pairCount);
            }
        }
    }
    
    // query 7
    public static void getRevenueByProductAndSupplier(String dbUsername, String dbPassword) throws SQLException {
        String dbUrl = "jdbc:mysql://localhost:3306/metro_dw";  

        // SQL Query to calculate total revenue by store, supplier, and product for 2019 with ROLLUP
        String query = """
            SELECT 
                EXTRACT(YEAR FROM t.OrderDate) AS Year, 
                t.StoreID, 
                p.SupplierID, 
                p.ProductID, 
                p.ProductName, 
                SUM(t.Quantity * p.ProductPrice) AS TotalRevenue
            FROM 
                Transaction_Fact t
            JOIN 
                Product_Dimension p ON t.ProductID = p.ProductID
            WHERE 
                EXTRACT(YEAR FROM t.OrderDate) = 2019  -- Year filter
            GROUP BY 
                EXTRACT(YEAR FROM t.OrderDate), t.StoreID, p.SupplierID, p.ProductID, p.ProductName 
                WITH ROLLUP
            ORDER BY 
                Year, t.StoreID, p.SupplierID, p.ProductID
            LIMIT 0, 50000;
        """;

        // Establishing MySQL connection
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            System.out.println("Revenue by Product, Supplier, and Store (2019):");

            // Process the results
            while (rs.next()) {
                String year = rs.getString("Year");
                int storeId = rs.getInt("StoreID");
                int supplierId = rs.getInt("SupplierID");
                int productId = rs.getInt("ProductID");
                String productName = rs.getString("ProductName");
                double totalRevenue = rs.getDouble("TotalRevenue");

                // Handle the NULL values for ROLLUP rows
                if (year == null) {
                    year = "Total";  // For the ROLLUP row (Total)
                }
                if (productName == null) {
                    productName = "Total";  // For the ROLLUP row (Total)
                }

                // Display the results
                System.out.printf("Year: %s, StoreID: %d, SupplierID: %d, ProductID: %d, ProductName: %s, TotalRevenue: %.2f%n",
                        year, storeId, supplierId, productId, productName, totalRevenue);
            }
        }
    }
    
    // query 8
    public static void getProductRevenue(String dbUsername, String dbPassword) throws SQLException {
        String dbUrl = "jdbc:mysql://localhost:3306/metro_dw"; 

        // SQL Query to calculate revenue and quantity for each product for the year 2019
        String query = """
            SELECT 
                p.ProductID,
                p.ProductName,
                -- Total revenue and quantity for the first half (H1) of the year (Jan-Jun)
                SUM(CASE WHEN EXTRACT(MONTH FROM t.OrderDate) BETWEEN 1 AND 6 THEN t.Quantity * p.ProductPrice ELSE 0 END) AS TotalRevenue_H1,
                SUM(CASE WHEN EXTRACT(MONTH FROM t.OrderDate) BETWEEN 1 AND 6 THEN t.Quantity ELSE 0 END) AS TotalQuantity_H1,
                -- Total revenue and quantity for the second half (H2) of the year (Jul-Dec)
                SUM(CASE WHEN EXTRACT(MONTH FROM t.OrderDate) BETWEEN 7 AND 12 THEN t.Quantity * p.ProductPrice ELSE 0 END) AS TotalRevenue_H2,
                SUM(CASE WHEN EXTRACT(MONTH FROM t.OrderDate) BETWEEN 7 AND 12 THEN t.Quantity ELSE 0 END) AS TotalQuantity_H2,
                -- Total revenue and quantity for the entire year
                SUM(t.Quantity * p.ProductPrice) AS TotalRevenue_Year,
                SUM(t.Quantity) AS TotalQuantity_Year
            FROM 
                Transaction_Fact t
            JOIN 
                Product_Dimension p ON t.ProductID = p.ProductID
            WHERE 
                EXTRACT(YEAR FROM t.OrderDate) = 2019  -- Year filter
            GROUP BY 
                p.ProductID, p.ProductName
            ORDER BY 
                p.ProductID;
        """;

        // Establishing MySQL connection
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            System.out.println("Product Revenue for 2019:");

            // Process the results
            while (rs.next()) {
                int productId = rs.getInt("ProductID");
                String productName = rs.getString("ProductName");
                double totalRevenueH1 = rs.getDouble("TotalRevenue_H1");
                int totalQuantityH1 = rs.getInt("TotalQuantity_H1");
                double totalRevenueH2 = rs.getDouble("TotalRevenue_H2");
                int totalQuantityH2 = rs.getInt("TotalQuantity_H2");
                double totalRevenueYear = rs.getDouble("TotalRevenue_Year");
                int totalQuantityYear = rs.getInt("TotalQuantity_Year");

                // Display the results
                System.out.printf("ProductID: %d, ProductName: %s, " +
                                  "Total Revenue H1: %.2f, Total Quantity H1: %d, " +
                                  "Total Revenue H2: %.2f, Total Quantity H2: %d, " +
                                  "Total Revenue Year: %.2f, Total Quantity Year: %d%n",
                                  productId, productName,
                                  totalRevenueH1, totalQuantityH1,
                                  totalRevenueH2, totalQuantityH2,
                                  totalRevenueYear, totalQuantityYear);
            }
        }
    }
    
    // query 9
    public static void getSalesSpikes(String dbUsername, String dbPassword) throws SQLException {
        String dbUrl = "jdbc:mysql://localhost:3306/metro_dw"; 

        // SQL Query to identify high spikes in daily sales for each product
        String query = """
            WITH Daily_Avg_Sales AS (
                SELECT 
                    t.ProductID,
                    t.OrderDate,
                    SUM(t.Quantity * p.ProductPrice) AS DailySales,
                    AVG(SUM(t.Quantity * p.ProductPrice)) OVER (PARTITION BY t.ProductID) AS AvgDailySales
                FROM 
                    Transaction_Fact t
                JOIN 
                    Product_Dimension p ON t.ProductID = p.ProductID
                GROUP BY 
                    t.ProductID, t.OrderDate
            ),
            High_Spikes AS (
                SELECT 
                    das.ProductID,
                    das.OrderDate,
                    das.DailySales,
                    das.AvgDailySales,
                    CASE 
                        WHEN das.DailySales > 2 * das.AvgDailySales THEN 'Spike' 
                        ELSE 'Normal' 
                    END AS SalesStatus
                FROM 
                    Daily_Avg_Sales das
            )
            SELECT 
                hs.ProductID,
                p.ProductName,
                hs.OrderDate,
                hs.DailySales,
                hs.AvgDailySales,
                hs.SalesStatus
            FROM 
                High_Spikes hs
            JOIN 
                Product_Dimension p ON hs.ProductID = p.ProductID
            ORDER BY 
                hs.ProductID, hs.OrderDate;
        """;

        // Establishing MySQL connection
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            System.out.println("Sales Spike Analysis:");

            // Process the results
            while (rs.next()) {
                int productId = rs.getInt("ProductID");
                String productName = rs.getString("ProductName");
                Date orderDate = rs.getDate("OrderDate");
                double dailySales = rs.getDouble("DailySales");
                double avgDailySales = rs.getDouble("AvgDailySales");
                String salesStatus = rs.getString("SalesStatus");

                // Display the results
                System.out.printf("ProductID: %d, ProductName: %s, OrderDate: %s, " +
                                  "DailySales: %.2f, AvgDailySales: %.2f, SalesStatus: %s%n",
                                  productId, productName, orderDate.toString(),
                                  dailySales, avgDailySales, salesStatus);
            }
        }
    }
    
    // query 10
    public static void createStoreQuarterlySalesView(String dbUsername, String dbPassword) {
        String dbUrl = "jdbc:mysql://localhost:3306/metro_dw";

        // SQL Query to create the view
        String createViewQuery = """
                CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
                SELECT 
                    t.StoreID, 
                    p.StoreName,
                    EXTRACT(YEAR FROM t.OrderDate) AS Year, 
                    QUARTER(t.OrderDate) AS Quarter,
                    SUM(t.Quantity * p.ProductPrice) AS TotalSales
                FROM 
                    Transaction_Fact t
                JOIN 
                    Product_Dimension p ON t.ProductID = p.ProductID
                GROUP BY 
                    t.StoreID, 
                    p.StoreName, 
                    EXTRACT(YEAR FROM t.OrderDate), 
                    QUARTER(t.OrderDate)
                ORDER BY 
                    p.StoreName, 
                    EXTRACT(YEAR FROM t.OrderDate), 
                    QUARTER(t.OrderDate);
                """;

        // Establish connection and execute the query
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             Statement stmt = conn.createStatement()) {

            // Execute the query
            stmt.execute(createViewQuery);
            System.out.println("View 'STORE_QUARTERLY_SALES' has been successfully created or replaced.");

        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("Error while creating the view.");
        }
    }

    public static void applyMeshJoin(List<Customer> customers, List<Product> products, List<Transaction> transactions, String dbUsername, String dbPassword) throws SQLException {
        // Database connection details
        String dbUrl = "jdbc:mysql://localhost:3306/metro_dw";

        // Establish MySQL connection
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword)) {
            conn.setAutoCommit(false); // Disable auto-commit for batch insert

            // Prepare insert statements for each table
            String insertProductQuery = "INSERT INTO Product_Dimension (ProductID, ProductName, ProductPrice, SupplierID, SupplierName, StoreID, StoreName) VALUES (?, ?, ?, ?, ?, ?, ?)";
            String insertCustomerQuery = "INSERT INTO Customer_Dimension (CustomerID, CustomerName, Gender) VALUES (?, ?, ?)";
            String insertTransactionQuery = "INSERT INTO Transaction_Fact (OrderID, OrderDate, ProductID, CustomerID, StoreID, Quantity, TotalSale) VALUES (?, ?, ?, ?, ?, ?, ?)";

            // HashMap to store customer transactions by customerId
            Map<String, List<Transaction>> customerTransactions = new HashMap<>();
            for (Transaction transaction : transactions) {
                customerTransactions
                        .computeIfAbsent(transaction.customerId, k -> new ArrayList<>())
                        .add(transaction);
            }

            try (PreparedStatement productStmt = conn.prepareStatement(insertProductQuery);
                 PreparedStatement customerStmt = conn.prepareStatement(insertCustomerQuery);
                 PreparedStatement transactionStmt = conn.prepareStatement(insertTransactionQuery)) {

                // Insert product data into Product_Dimension
                for (Product product : products) {
                    productStmt.setString(1, product.productId);
                    productStmt.setString(2, product.productName);
                    productStmt.setDouble(3, product.productPrice);
                    productStmt.setString(4, product.supplierId);
                    productStmt.setString(5, product.supplierName);
                    productStmt.setString(6, product.storeId);
                    productStmt.setString(7, product.storeName);
                    productStmt.addBatch();
                }

                // Insert customer data into Customer_Dimension
                for (Customer customer : customers) {
                    customerStmt.setString(1, customer.customerId);
                    customerStmt.setString(2, customer.customerName);
                    customerStmt.setString(3, customer.gender);
                    customerStmt.addBatch();
                }

                // Insert transaction data into Transaction_Fact
                for (Transaction transaction : transactions) {
                    transactionStmt.setString(1, transaction.orderId);
                    transactionStmt.setString(2, transaction.orderDate);
                    transactionStmt.setString(3, transaction.productId);
                    transactionStmt.setString(4, transaction.customerId);
                    transactionStmt.setString(5, transaction.timeId);  // Using timeId for StoreID
                    transactionStmt.setInt(6, transaction.quantityOrdered);
                    transactionStmt.setDouble(7, transaction.quantityOrdered * getProductPriceById(products, transaction.productId));
                    transactionStmt.addBatch();
                }

                // Execute all batches
                productStmt.executeBatch();
                customerStmt.executeBatch();
                transactionStmt.executeBatch();

                // Commit the transaction
                conn.commit();
                System.out.println("Mesh Join applied and data loaded successfully.");
            }
        }
    }

    public static double getProductPriceById(List<Product> products, String productId) {
        for (Product product : products) {
            if (product.productId.equals(productId)) {
                return product.productPrice;
            }
        }
        return 0.0; // Return 0 if product not found
    }
}
