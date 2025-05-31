SELECT *
FROM [AMAZON SALES]..[amazon_sales_data 2025] AS Amazon_Sales;

--  Detect and eliminate duplicate records within the Amazon sales dataset to ensure data integrity.

WITH DuplicateRecords AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Order_ID, Date, Product, Category, Price, Quantity, Total_Sales, Customer_Name,Customer_Location,
                              Payment_Method, Status ORDER BY (SELECT NULL)) AS RowNum
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
)
DELETE FROM [AMAZON SALES]..[amazon_sales_data 2025]
WHERE Order_ID IN (
    SELECT Order_ID FROM DuplicateRecords WHERE RowNum > 1
);

--  Determine the sum of sales for each individual product and order the products in descending order based on their total sales revenue.

SELECT TOP 10
   Product,
   SUM(Total_Sales) AS Total_Sales 
FROM [AMAZON SALES]..[amazon_sales_data 2025]
GROUP BY Product
ORDER BY Total_Sales DESC;


-- Determine the sum of sales for each product category and present the categories in descending order of their total revenue.

SELECT
   Category,
   SUM(Total_Sales) AS Total_Sales 
FROM [AMAZON SALES]..[amazon_sales_data 2025]
GROUP BY Category
ORDER BY Total_Sales DESC;

-- Determine the frequency of each transaction status for every payment method and present the payment methods in descending order of the total number of statuses recorded.

SELECT
   Payment_Method,
   COUNT(Status) AS Status_Count  
FROM [AMAZON SALES]..[amazon_sales_data 2025]
GROUP BY Payment_Method
ORDER BY Status_Count DESC;

-- Determine the frequency of 'Cancelled', 'Pending', and 'Completed' transaction statuses for each payment method within the Amazon sales dataset.

SELECT
   Payment_Method,Status,
   COUNT(Status) AS Status_Count  
FROM [AMAZON SALES]..[amazon_sales_data 2025]
GROUP BY Payment_Method,Status
ORDER BY Payment_Method,Status;

-- Extract the minimum and maximum values of product prices from the Amazon sales dataset.

SELECT 
     MIN(Price) AS Min_Price ,
     MAX(Price) AS Max_Price
FROM [AMAZON SALES]..[amazon_sales_data 2025];

-- Assign total sales values to specific price categories according to the associated product prices.

SELECT 
    CASE 
        WHEN Price < 50 THEN 'Low Price (Below $50)'
        WHEN Price BETWEEN 50 AND 200 THEN 'Medium Price ($50 - $200)'
        WHEN Price BETWEEN 201 AND 500 THEN 'High Price ($201 - $500)'
        ELSE 'Premium Price (Above $500)'
    END AS Price_Category,
    SUM(Total_Sales) AS Total_Sales,
	SUM(Quantity) AS Total_Quantity
FROM [AMAZON SALES]..[amazon_sales_data 2025]
GROUP BY 
    CASE 
        WHEN Price < 50 THEN 'Low Price (Below $50)'
        WHEN Price BETWEEN 50 AND 200 THEN 'Medium Price ($50 - $200)'
        WHEN Price BETWEEN 201 AND 500 THEN 'High Price ($201 - $500)'
        ELSE 'Premium Price (Above $500)'
    END
ORDER BY Total_Sales DESC;

-- Extract the 10 customers who generated the most total sales revenue and display them in descending order of their sales figures.

WITH CustomerTotals AS (
    SELECT Customer_Name, SUM(Total_Sales) AS Total_Sales,
           ROW_NUMBER() OVER (ORDER BY SUM(Total_Sales) DESC) AS RowNum
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY Customer_Name
)
SELECT *
FROM CustomerTotals
WHERE RowNum <= 10;


-- Extract the 5 customer locations that generated the most total sales revenue and display them in descending order of their sales figures.

WITH Customer_Location AS 
(
    SELECT 
        Customer_Location,
        SUM(Total_Sales) AS Total_Sales
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY Customer_Location
), Customer_Location_RANK AS
(
SELECT *, DENSE_RANK() OVER (ORDER BY Total_Sales DESC) AS RANKING
FROM Customer_Location
)
SELECT *
FROM Customer_Location_RANK
WHERE RANKING <=5 ;

-- Creates a view named 'Monthly_Sales_View' that aggregates total sales data from the 'amazon_sales_data 2025' table at the monthly level.
-- The view calculates the sum of 'Total_Sales' for each unique combination of 'Sales_Year' and 'Sales_Month'.

CREATE VIEW Monthly_Sales_View AS
SELECT 
    YEAR(DATE) AS Sales_Year,
    MONTH(DATE) AS Sales_Month,
    SUM(Total_Sales) AS Total_Sales
FROM [AMAZON SALES]..[amazon_sales_data 2025]
GROUP BY YEAR(DATE), MONTH(DATE);

-- Retrieves monthly total sales from the 'Monthly_Sales_View' and orders the results by total sales in descending order.
-- The 'Month' column is formatted as 'YYYY-MM' (e.g., '2025-01').

SELECT 
    CONCAT(Sales_Year, '-', FORMAT(Sales_Month, '00')) AS Month,
    Total_Sales
FROM Monthly_Sales_View
ORDER BY Total_Sales DESC;


-- Computes a running total of sales by calculating the sum of sales up to and including each date, using data from the 'amazon_sales_data 2025' table.

WITH Sales AS (
    SELECT
        Date, -- Include the Date column for ordering
        SUM(Total_Sales) AS Total_Sales
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    WHERE Date IS NOT NULL
    GROUP BY
        Date
),
ROLLING_TOTAL AS (
    SELECT
        Date,
        Total_Sales,
        SUM(Total_Sales) OVER (ORDER BY Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Rolling_Total
    FROM Sales
)
SELECT
    Date,
    Total_Sales,
    Rolling_Total
FROM ROLLING_TOTAL
ORDER BY Date;

-- Performs RFM (Recency, Frequency, Monetary) analysis to segment customers from the Amazon sales dataset into distinct categories.
-- The query calculates RFM metrics for each customer in the 'CustomerMetrics' CTE, assigns RFM scores based on these metrics in the 'RFM_Scores' CTE,
-- and then categorizes customers into segments like 'VIP Customers', 'Loyal Customers', 'At Risk', etc., based on their RFM scores.
-- Finally, the results are ordered to prioritize higher-value segments (VIP and Loyal Customers).

WITH CustomerMetrics AS (
    SELECT 
        Customer_Name,
        DATEDIFF(day, MAX(Date), GETDATE()) AS Recency,
        COUNT(DISTINCT Order_ID) AS Frequency,
        SUM(Total_Sales) AS Monetary
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY Customer_Name
),
RFM_Scores AS (
    SELECT 
        *,
        NTILE(5) OVER (ORDER BY Recency ASC) AS R_Score,
        NTILE(5) OVER (ORDER BY Frequency DESC) AS F_Score,
        NTILE(5) OVER (ORDER BY Monetary DESC) AS M_Score
    FROM CustomerMetrics
)
SELECT 
    Customer_Name,
    Recency, Frequency, Monetary,
    R_Score, F_Score, M_Score,
    CONCAT(R_Score, F_Score, M_Score) AS RFM_Combined,
    CASE 
        WHEN (R_Score >= 4 AND F_Score >= 4 AND M_Score >= 4) THEN 'VIP Customers'
        WHEN (R_Score >= 3 AND F_Score >= 3 AND M_Score >= 3) THEN 'Loyal Customers'
        WHEN (R_Score >= 3 AND F_Score >= 1 AND M_Score >= 2) THEN 'Potential Loyalists'
        WHEN (R_Score <= 2 AND F_Score <= 2 AND M_Score <= 2) THEN 'At Risk'
        WHEN (R_Score = 1 AND F_Score = 1 AND M_Score >= 1) THEN 'Lost Customers'
        ELSE 'Others'
    END AS Customer_Segment
FROM RFM_Scores
ORDER BY 
    CASE 
        WHEN (R_Score >= 4 AND F_Score >= 4 AND M_Score >= 4) THEN 1
        WHEN (R_Score >= 3 AND F_Score >= 3 AND M_Score >= 3) THEN 2
        ELSE 3
    END;


-- Analyzes customer purchase patterns to segment customers into distinct buyer categories based on their purchase diversity, average price point, price variety, number of premium purchases (price > 500), and average quantity purchased.
-- The query first calculates these key customer purchase metrics in the 'CustomerPurchasePatterns' CTE.
-- Then, it uses a CASE statement to assign a 'Purchase_Segment' label to each customer based on predefined rules applied to these metrics.
-- Finally, the results are ordered to prioritize customers with more premium purchases and higher purchase diversity.


WITH CustomerPurchasePatterns AS (
    SELECT 
        Customer_Name,
        COUNT(DISTINCT Category) AS Category_Diversity,
        AVG(Price) AS Avg_Price_Point,
        STDEV(Price) AS Price_Variety,
        SUM(CASE WHEN Price > 500 THEN 1 ELSE 0 END) AS Premium_Purchases,
        AVG(Quantity) AS Avg_Quantity
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY Customer_Name
)
SELECT 
    *,
    CASE 
        WHEN Category_Diversity >= 3 AND Premium_Purchases > 0 THEN 'Diverse Premium Buyer'
        WHEN Category_Diversity >= 3 AND Premium_Purchases = 0 THEN 'Diverse Value Buyer'
        WHEN Category_Diversity < 3 AND Avg_Price_Point > 200 THEN 'Category Specialist (High-End)'
        WHEN Category_Diversity < 3 AND Avg_Price_Point <= 200 THEN 'Category Specialist (Budget)'
        ELSE 'Occasional Buyer'
    END AS Purchase_Segment
FROM CustomerPurchasePatterns
ORDER BY Premium_Purchases DESC, Category_Diversity DESC;

-- Performs cohort analysis to calculate customer retention rates.  Customers are grouped into cohorts based on their first purchase month.
-- The query tracks how many customers from each cohort are still active (making purchases) in subsequent months.  
-- The final result shows the percentage of customers from each original cohort who are retained over time.

WITH FirstPurchase AS (
    SELECT 
        Customer_Name,
        MIN(Date) AS First_Purchase_Date,
        FORMAT(MIN(Date), 'yyyy-MM') AS Cohort_Month
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY Customer_Name
),
CustomerActivity AS (
    SELECT 
        fp.Customer_Name,
        fp.Cohort_Month,
        s.Date,
        FORMAT(s.Date, 'yyyy-MM') AS Activity_Month,
        DATEDIFF(MONTH, fp.First_Purchase_Date, s.Date) AS Months_Since_First_Purchase
    FROM FirstPurchase fp
    JOIN [AMAZON SALES]..[amazon_sales_data 2025] s ON fp.Customer_Name = s.Customer_Name
)
SELECT 
    Cohort_Month,
    COUNT(DISTINCT Customer_Name) AS Cohort_Size,
    Months_Since_First_Purchase,
    COUNT(DISTINCT Customer_Name) AS Active_Customers,
    CAST(COUNT(DISTINCT Customer_Name) AS FLOAT) / 
        FIRST_VALUE(COUNT(DISTINCT Customer_Name)) OVER (
            PARTITION BY Cohort_Month 
            ORDER BY Months_Since_First_Purchase
        ) * 100 AS Retention_Rate
FROM CustomerActivity
GROUP BY Cohort_Month, Months_Since_First_Purchase
ORDER BY Cohort_Month, Months_Since_First_Purchase;


-- Analyzes cohort purchasing behavior to understand sales trends over time for different customer cohorts.
-- Customers are grouped into cohorts based on their first purchase month.
-- The query calculates the total number of orders, total sales, and average order value for each cohort over subsequent months.

WITH FirstPurchase AS (
    SELECT 
        Customer_Name,
        MIN(Date) AS First_Purchase_Date,
        FORMAT(MIN(Date), 'yyyy-MM') AS Cohort_Month
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY Customer_Name
),
CohortPurchases AS (
    SELECT 
        fp.Cohort_Month,
        DATEDIFF(MONTH, fp.First_Purchase_Date, s.Date) AS Months_Since_First_Purchase,
        s.Total_Sales
    FROM FirstPurchase fp
    JOIN [AMAZON SALES]..[amazon_sales_data 2025] s ON fp.Customer_Name = s.Customer_Name
)
SELECT 
    Cohort_Month,
    Months_Since_First_Purchase,
    COUNT(*) AS Number_of_Orders,
    SUM(Total_Sales) AS Total_Sales,
    AVG(Total_Sales) AS Average_Order_Value
FROM CohortPurchases
GROUP BY Cohort_Month, Months_Since_First_Purchase
ORDER BY Cohort_Month, Months_Since_First_Purchase;


-- Calculates customer lifetime value (CLV) and related metrics for each customer in the Amazon sales dataset.
-- The query first calculates key customer metrics such as first purchase date, last purchase date, customer lifespan, total orders, and total revenue in the 'CustomerRevenue' CTE.
-- It then calculates annual revenue, annual purchase frequency, average order value, and projected 3-year CLV for each customer.
-- Finally, the results are ordered in descending order of projected 3-year CLV.


WITH CustomerRevenue AS (
    SELECT 
        Customer_Name,
        MIN(Date) AS First_Purchase_Date,
        MAX(Date) AS Last_Purchase_Date,
        DATEDIFF(day, MIN(Date), MAX(Date)) AS Customer_Lifespan_Days,
        COUNT(DISTINCT Order_ID) AS Total_Orders,
        SUM(Total_Sales) AS Total_Revenue
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY Customer_Name
)
SELECT 
    Customer_Name,
    First_Purchase_Date,
    Last_Purchase_Date,
    Customer_Lifespan_Days,
    Total_Orders,
    Total_Revenue,
    CASE 
        WHEN Customer_Lifespan_Days = 0 THEN Total_Revenue
        ELSE Total_Revenue / (Customer_Lifespan_Days / 365.0)
    END AS Annual_Revenue,
    CASE 
        WHEN Customer_Lifespan_Days = 0 THEN Total_Orders
        ELSE Total_Orders / (Customer_Lifespan_Days / 365.0)
    END AS Annual_Purchase_Frequency,
    Total_Revenue / NULLIF(Total_Orders, 0) AS Average_Order_Value,
    CASE 
        WHEN Customer_Lifespan_Days = 0 THEN Total_Revenue * 3
        ELSE (Total_Revenue / (Customer_Lifespan_Days / 365.0)) * 3
    END AS Projected_3_Year_CLV
FROM CustomerRevenue
ORDER BY Projected_3_Year_CLV DESC;



-- Create a dashboard-ready view for Sales Overview with pre-calculated YTD metrics
CREATE VIEW Dashboard_Sales_Overview AS
SELECT 
    DATEPART(YEAR, Date) AS Year,
    DATEPART(MONTH, Date) AS Month,
    DATENAME(MONTH, Date) AS Month_Name,
    SUM(Total_Sales) AS Monthly_Sales,
    COUNT(DISTINCT Order_ID) AS Order_Count,
    SUM(Total_Sales) / COUNT(DISTINCT Order_ID) AS Average_Order_Value,
    SUM(SUM(Total_Sales)) OVER (PARTITION BY DATEPART(YEAR, Date) ORDER BY DATEPART(MONTH, Date)) AS YTD_Sales,
    LAG(SUM(Total_Sales)) OVER (ORDER BY DATEPART(YEAR, Date), DATEPART(MONTH, Date)) AS Previous_Month_Sales,
    CASE 
        WHEN LAG(SUM(Total_Sales)) OVER (ORDER BY DATEPART(YEAR, Date), DATEPART(MONTH, Date)) = 0 THEN NULL
        ELSE (SUM(Total_Sales) - LAG(SUM(Total_Sales)) OVER (ORDER BY DATEPART(YEAR, Date), DATEPART(MONTH, Date))) / 
             LAG(SUM(Total_Sales)) OVER (ORDER BY DATEPART(YEAR, Date), DATEPART(MONTH, Date)) * 100 
    END AS MoM_Growth_Percentage
FROM [AMAZON SALES]..[amazon_sales_data 2025]
GROUP BY DATEPART(YEAR, Date), DATEPART(MONTH, Date), DATENAME(MONTH, Date);



-- Create a Category by Month pivot for heat map visualization
WITH Monthly_Category_Sales AS (
    SELECT 
        Category,
        DATEPART(YEAR, Date) AS Year,
        DATEPART(MONTH, Date) AS Month,
        SUM(Total_Sales) AS Total_Sales
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY Category, DATEPART(YEAR, Date), DATEPART(MONTH, Date)
)
SELECT 
    Category,
    [1] AS Jan, [2] AS Feb, [3] AS Mar, [4] AS Apr, 
    [5] AS May, [6] AS Jun, [7] AS Jul, [8] AS Aug, 
    [9] AS Sep, [10] AS Oct, [11] AS Nov, [12] AS Dec
FROM (
    SELECT Category, Month, Total_Sales
    FROM Monthly_Category_Sales
    WHERE Year = 2025
) AS SourceTable
PIVOT (
    SUM(Total_Sales)
    FOR Month IN ([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12])
) AS PivotTable
ORDER BY Category;


-- Prepare location data for mapping visualizations
SELECT 
    Customer_Location,
    COUNT(DISTINCT Customer_Name) AS Customer_Count,
    COUNT(DISTINCT Order_ID) AS Order_Count,
    SUM(Total_Sales) AS Total_Sales,
    AVG(Total_Sales) AS Average_Sales,
    SUM(CASE WHEN Status = 'Cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS Cancellation_Rate
FROM [AMAZON SALES]..[amazon_sales_data 2025]
GROUP BY Customer_Location
ORDER BY Total_Sales DESC;

-- Prepare time series data with moving averages for trend visualization
WITH DailySales AS (
    SELECT 
        Date,
        SUM(Total_Sales) AS Daily_Sales
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY Date
)
SELECT 
    Date,
    Daily_Sales,
    AVG(Daily_Sales) OVER (ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS Seven_Day_Moving_Avg,
    AVG(Daily_Sales) OVER (ORDER BY Date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS Thirty_Day_Moving_Avg
FROM DailySales
ORDER BY Date;

-- Prepare funnel analysis data for visualization
WITH CustomerJourney AS (
    SELECT 
        Customer_Name,
        MIN(CASE WHEN Status = 'Completed' THEN Date END) AS First_Purchase_Date,
        COUNT(DISTINCT CASE WHEN Status = 'Completed' THEN Order_ID END) AS Completed_Orders,
        COUNT(DISTINCT CASE WHEN Status = 'Pending' THEN Order_ID END) AS Pending_Orders,
        COUNT(DISTINCT CASE WHEN Status = 'Cancelled' THEN Order_ID END) AS Cancelled_Orders,
        SUM(CASE WHEN Status = 'Completed' THEN Total_Sales ELSE 0 END) AS Completed_Sales,
        SUM(CASE WHEN Status = 'Pending' THEN Total_Sales ELSE 0 END) AS Pending_Sales,
        SUM(CASE WHEN Status = 'Cancelled' THEN Total_Sales ELSE 0 END) AS Cancelled_Sales
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY Customer_Name
)
SELECT
    COUNT(*) AS Total_Customers,
    COUNT(CASE WHEN Completed_Orders > 0 THEN 1 END) AS Customers_With_Completed_Orders,
    COUNT(CASE WHEN Completed_Orders > 1 THEN 1 END) AS Repeat_Customers,
    COUNT(CASE WHEN Completed_Orders > 3 THEN 1 END) AS Loyal_Customers,
    SUM(Completed_Sales) AS Total_Completed_Sales,
    SUM(Pending_Sales) AS Total_Pending_Sales,
    SUM(Cancelled_Sales) AS Total_Cancelled_Sales
FROM CustomerJourney;

-- Prepare hierarchical data for treemap visualization
SELECT 
    Category,
    Product,
    COUNT(DISTINCT Order_ID) AS Order_Count,
    SUM(Total_Sales) AS Total_Sales,
    SUM(Quantity) AS Units_Sold
FROM [AMAZON SALES]..[amazon_sales_data 2025]
GROUP BY Category, Product
ORDER BY Category, Total_Sales DESC;


-- Create segment summaries for pie/donut charts based on your RFM segments
WITH CustomerMetrics AS (
    SELECT 
        Customer_Name,
        DATEDIFF(day, MAX(Date), GETDATE()) AS Recency,
        COUNT(DISTINCT Order_ID) AS Frequency,
        SUM(Total_Sales) AS Monetary
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY Customer_Name
),
RFM_Scores AS (
    SELECT 
        *,
        NTILE(5) OVER (ORDER BY Recency ASC) AS R_Score,
        NTILE(5) OVER (ORDER BY Frequency DESC) AS F_Score,
        NTILE(5) OVER (ORDER BY Monetary DESC) AS M_Score
    FROM CustomerMetrics
),
CustomerSegments AS (
    SELECT
        Customer_Name,
        CASE
            WHEN (R_Score >= 4 AND F_Score >= 4 AND M_Score >= 4) THEN 'VIP Customers'
            WHEN (R_Score >= 3 AND F_Score >= 3 AND M_Score >= 3) THEN 'Loyal Customers'
            WHEN (R_Score >= 3 AND F_Score >= 1 AND M_Score >= 2) THEN 'Potential Loyalists'
            WHEN (R_Score <= 2 AND F_Score <= 2 AND M_Score <= 2) THEN 'At Risk'
            WHEN (R_Score = 1 AND F_Score = 1 AND M_Score >= 1) THEN 'Lost Customers'
            ELSE 'Others'
        END AS Customer_Segment
    FROM RFM_Scores
),
SegmentSummary AS (
    SELECT
        Customer_Segment,
        COUNT(*) AS Customer_Count
    FROM CustomerSegments
    GROUP BY Customer_Segment
)
SELECT
    Customer_Segment,
    Customer_Count,
    (Customer_Count * 100.0) / SUM(Customer_Count) OVER () AS Percentage
FROM SegmentSummary
ORDER BY Customer_Count DESC;


-- Create a procedure that exports visualization-ready data sets to CSV format
CREATE PROCEDURE ExportVisualizationData
AS
BEGIN
    -- Monthly Sales Performance for Line Charts
    SELECT 
        FORMAT(Date, 'yyyy-MM') AS Month,
        SUM(Total_Sales) AS Monthly_Sales,
        COUNT(DISTINCT Order_ID) AS Order_Count,
        COUNT(DISTINCT Customer_Name) AS Customer_Count
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY FORMAT(Date, 'yyyy-MM')
    ORDER BY Month;

    -- Category Performance for Bar Charts
    SELECT 
        Category,
        SUM(Total_Sales) AS Total_Sales,
        COUNT(DISTINCT Order_ID) AS Order_Count,
        SUM(Quantity) AS Units_Sold,
        AVG(Price) AS Average_Price
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY Category
    ORDER BY Total_Sales DESC;

    -- Payment Method Analysis for Pie Charts
    SELECT 
        Payment_Method,
        COUNT(*) AS Transaction_Count,
        SUM(Total_Sales) AS Total_Sales,
        SUM(CASE WHEN Status = 'Cancelled' THEN 1 ELSE 0 END) AS Cancelled_Count,
        SUM(CASE WHEN Status = 'Cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS Cancellation_Rate
    FROM [AMAZON SALES]..[amazon_sales_data 2025]
    GROUP BY Payment_Method
    ORDER BY Transaction_Count DESC;
END;


-- Create a Date Dimension table for consistent time-based visualizations
CREATE TABLE DateDimension (
    DateKey INT PRIMARY KEY,
    FullDate DATE,
    DayOfWeek INT,
    DayName NVARCHAR(10),
    DayOfMonth INT,
    DayOfYear INT,
    WeekOfYear INT,
    MonthNumber INT,
    MonthName NVARCHAR(10),
    Quarter INT,
    QuarterName NVARCHAR(6),
    Year INT,
    IsWeekend BIT,
    IsHoliday BIT
);

-- Fill with data for your date range (example)
DECLARE @StartDate DATE = '2025-01-01';
DECLARE @EndDate DATE = '2025-12-31';

WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO DateDimension (
        DateKey, FullDate, DayOfWeek, DayName, DayOfMonth, DayOfYear,
        WeekOfYear, MonthNumber, MonthName, Quarter, QuarterName, Year, IsWeekend, IsHoliday
    )
    SELECT 
        CONVERT(INT, CONVERT(VARCHAR, @StartDate, 112)) AS DateKey,
        @StartDate AS FullDate,
        DATEPART(WEEKDAY, @StartDate) AS DayOfWeek,
        DATENAME(WEEKDAY, @StartDate) AS DayName,
        DATEPART(DAY, @StartDate) AS DayOfMonth,
        DATEPART(DAYOFYEAR, @StartDate) AS DayOfYear,
        DATEPART(WEEK, @StartDate) AS WeekOfYear,
        DATEPART(MONTH, @StartDate) AS MonthNumber,
        DATENAME(MONTH, @StartDate) AS MonthName,
        DATEPART(QUARTER, @StartDate) AS Quarter,
        'Q' + CAST(DATEPART(QUARTER, @StartDate) AS VARCHAR) AS QuarterName,
        DATEPART(YEAR, @StartDate) AS Year,
        CASE WHEN DATEPART(WEEKDAY, @StartDate) IN (1, 7) THEN 1 ELSE 0 END AS IsWeekend,
        CASE WHEN @StartDate IN ('2025-01-01', '2025-12-25') THEN 1 ELSE 0 END AS IsHoliday
    
    SET @StartDate = DATEADD(DAY, 1, @StartDate);
END;


-- Create a configuration view that sets up parameters for dashboards
CREATE VIEW Dashboard_Configuration AS
SELECT
    'Amazon Sales Dashboard' AS Dashboard_Title,
    'Data last updated: ' + CONVERT(VARCHAR, MAX(Date), 120) AS Last_Update,
    MIN(Date) AS Data_Range_Start,
    MAX(Date) AS Data_Range_End,
    COUNT(DISTINCT Order_ID) AS Total_Orders,
    COUNT(DISTINCT Customer_Name) AS Total_Customers,
    SUM(Total_Sales) AS Total_Revenue,
    AVG(Total_Sales) AS Average_Order_Value,
    SUM(CASE WHEN Status = 'Completed' THEN Total_Sales ELSE 0 END) / SUM(Total_Sales) * 100 AS Completion_Rate
FROM [AMAZON SALES]..[amazon_sales_data 2025];

