											-- Question 01 --

SELECT 
    EXTRACT(MONTH FROM t.OrderDate) AS Month,
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
    EXTRACT(YEAR FROM t.OrderDate) = 2019  -- Specifying the year for the analysis
GROUP BY 
    EXTRACT(MONTH FROM t.OrderDate),
    DayType,
    p.ProductID,
    p.ProductName
ORDER BY 
    TotalRevenue DESC
LIMIT 5;

									-- Question 02 --
                                    
WITH QuarterlyRevenue AS (
    -- Calculate the total revenue for each store for each quarter of 2017
    SELECT 
        t.StoreID,
        EXTRACT(QUARTER FROM t.OrderDate) AS Quarter,
        SUM(t.Quantity * p.ProductPrice) AS TotalRevenue
    FROM 
        Transaction_Fact t
    JOIN 
        Product_Dimension p ON t.ProductID = p.ProductID
    WHERE 
        EXTRACT(YEAR FROM t.OrderDate) = 2019  
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


									-- Question 03 --
                                    
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


                                    
                                    -- Question 04 --
                                    
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

                                    
                                    -- Question 05 --
                                    
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

                                    
                                    -- Question 06 --
                                    
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

                                    
                                    -- Question 07 --
                                    
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




                                    
                                    -- Question 08 --
                                    
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

                                    
                                    -- Question 09 --
                                    
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

                                    
									-- Question 10 --store_quarterly_sales
                                    
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT
    t.StoreID,                                    -- Store identifier
    p.StoreName,                                   -- Store name (from Product Dimension)
    EXTRACT(YEAR FROM t.OrderDate) AS Year,        -- Year of the sale
    QUARTER(t.OrderDate) AS Quarter,               -- Quarter of the year (1, 2, 3, or 4)
    SUM(t.Quantity * p.ProductPrice) AS TotalSales -- Total sales for the store in the quarter
FROM
    Transaction_Fact t
JOIN
    Product_Dimension p ON t.ProductID = p.ProductID  -- Join with Product Dimension to get product price and store info
GROUP BY
    t.StoreID, p.StoreName, EXTRACT(YEAR FROM t.OrderDate), QUARTER(t.OrderDate)
ORDER BY
    p.StoreName, EXTRACT(YEAR FROM t.OrderDate), QUARTER(t.OrderDate);


                                    
                                    
                                    
                                    
                                    
