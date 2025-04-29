
drop database if exists metro_dw;

CREATE DATABASE metro_dw;
use metro_dw;


-- Create Product Dimension Table (with StoreID)
CREATE TABLE Product_Dimension (
    ProductID VARCHAR(20) PRIMARY KEY,
    ProductName VARCHAR(255) NOT NULL,
    ProductPrice DECIMAL(10, 2) NOT NULL,
    SupplierID VARCHAR(20),
    SupplierName VARCHAR(255),
    StoreID VARCHAR(20),
    StoreName VARCHAR(255)  
);

-- Create Customer Dimension Table
CREATE TABLE Customer_Dimension (
    CustomerID VARCHAR(20) PRIMARY KEY,
    CustomerName VARCHAR(255) NOT NULL,
    Gender VARCHAR(10)
);

-- Create Fact Table: Transaction_Fact
CREATE TABLE Transaction_Fact (
    OrderID VARCHAR(20) PRIMARY KEY,
    OrderDate DATETIME NOT NULL,
    ProductID VARCHAR(20) NOT NULL,
    CustomerID VARCHAR(20) NOT NULL,
    StoreID VARCHAR(20),
    Quantity INT NOT NULL,
    TotalSale DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (ProductID) REFERENCES Product_Dimension(ProductID),
    FOREIGN KEY (CustomerID) REFERENCES Customer_Dimension(CustomerID)
    );
