# Instructor-s-Online-Server

USE invest;

1. /* this is the final code used to make analysis, co-authored-by Htet Aung Kyaw, Hanz Nathan Trajano, Oyindamola Iwalehin and Tangina Parvez */

	-- create customer information
		WITH customer_information_table AS (
        SELECT  cs.customer_id, 
				full_name, 
                opt38_desc, 
                account_id, 
                acct_open_date,
                CASE WHEN acct_open_status THEN 'Active'
					 ELSE 'Inactive' END,
				email,
                customer_location
		FROM customer_details AS cs
        LEFT JOIN account_dim AS ad
        ON cs.customer_id = ad.client_id
        WHERE cs.customer_id = 25),
	-- create total number of investment for the customer
        total_investment_amount_table AS (
        SELECT client_id AS customer_id,
			   SUM(value * quantity) AS total_investment
        FROM holdings_current
        LEFT JOIN account_dim
        USING(account_id)
        WHERE client_id = 25),
	-- create customer ticker list
		customer_ticker_list AS (
		SELECT  ad.client_id AS customer_id, 
				hc.ticker AS ticker,
                AVG(value) AS value,
                SUM(quantity) AS quantity,
                SUM(value * quantity) AS amount,
                SUM(value * quantity) / total_investment AS weight,
                sec_type,
                CASE WHEN major_asset_class = 'fixed_income' THEN 'fixed income'
					 ELSE major_asset_class END AS major_asset_class,
                minor_asset_class
		 FROM account_dim AS ad
		 LEFT JOIN holdings_current hc
		 USING(account_id)
         LEFT JOIN security_masterlist AS sm
         USING(ticker)
         LEFT JOIN total_investment_amount_table AS tiam
         ON tiam.customer_id = ad.client_id
		 WHERE ad.client_id = 25
		 GROUP BY  ad.client_id, hc.ticker, sec_type, major_asset_class, minor_asset_class),
	-- create return of each ticker table
		return_of_each_ticker_table AS (
		SELECT	returns.ticker AS ticker,
				AVG(returns.returns) AS mean,
				STDDEV(returns.returns) * SQRT(250) AS standard_dev,
				AVG(returns.returns)/(STDDEV(returns.returns)* SQRT(250)) AS risk_adjusted_return
		FROM (SELECT a.date,
					 a.ticker,
					 (a.value-a.lagged_price)/a.lagged_price AS returns
			  FROM ( SELECT *, LAG(value, 1)OVER(
					 PARTITION BY ticker ORDER BY date) AS lagged_price
					 FROM 	pricing_daily_new
					 WHERE	price_type='Adjusted') AS a
		INNER JOIN customer_ticker_list
        USING(ticker)
		WHERE  date >= CONCAT(2022-1,'-09-09')) AS returns
		GROUP BY returns.ticker),
	-- combine customer ticker table and return of each ticker table
		customer_ticker_return_detail_table AS (
        SELECT  customer_id, 
				ctl.ticker, 
                ctl.value, 
                ctl.quantity, 
                ctl.weight, 
                sec_type,
                major_asset_class,
                minor_asset_class,
                mean,
                standard_dev,
                risk_adjusted_return
			FROM customer_ticker_list AS ctl
            LEFT JOIN return_of_each_ticker_table
            USING (ticker)  ),
    -- investment per each ticker per customer
		group_by_ticker AS (
        SELECT customer_id,
                CONCAT('$ ',FORMAT(AVG(value),2)),
                SUM(quantity),
                CONCAT(FORMAT(SUM(weight * 100),2),'%'),
                sec_type,
                major_asset_class,
                FORMAT(AVG(mean),5),
                FORMAT(AVG(standard_dev),5),
                FORMAT(AVG(risk_adjusted_return),5)
		FROM customer_ticker_return_detail_table
        GROUP BY sec_type, major_asset_class
        ORDER BY SUM(weight * 100) DESC
        ),
        group_by_customer AS (
                SELECT customer_id,
                CONCAT('$ ',FORMAT(AVG(value),2)),
                SUM(quantity),
                CONCAT(FORMAT(SUM(weight * 100),2),'%'),
                sec_type,
                major_asset_class,
                FORMAT(AVG(mean),5),
                FORMAT(AVG(standard_dev),5),
                FORMAT(AVG(risk_adjusted_return),5)
		FROM customer_ticker_return_detail_table
        GROUP BY customer_id
        ORDER BY SUM(quantity) DESC
        ),
	-- create table for risk adj. return for all tickers
		return_of_all_tickers_table AS (
		SELECT	returns.ticker AS ticker,
				AVG(returns.returns) AS mean,
				STDDEV(returns.returns) * SQRT(250) AS standard_dev,
				AVG(returns.returns)/(STDDEV(returns.returns)* SQRT(250)) AS risk_adjusted_return
		FROM (SELECT a.date,
					 a.ticker,
					 (a.value-a.lagged_price)/a.lagged_price AS returns
			  FROM ( SELECT *, LAG(value, 1)OVER(
					 PARTITION BY ticker ORDER BY date) AS lagged_price
					 FROM 	pricing_daily_new
					 WHERE	price_type='Adjusted') AS a
		WHERE  date >= CONCAT(2022-1,'-09-09')) AS returns
		GROUP BY returns.ticker),
	-- compare 2 tickers per each class for a total of 4 tickers
	-- ticker 1
		ticker1_table AS (
        SELECT  ticker,
				FORMAT(mean,5),
                FORMAT(standard_dev,5),
                FORMAT(risk_adjusted_return,5)
        FROM return_of_all_tickers_table
        WHERE ticker = 'TSM'),
	-- ticker 2
		ticker2_table AS (
        SELECT ticker,
				FORMAT(mean,5),
                FORMAT(standard_dev,5),
                FORMAT(risk_adjusted_return,5)
        FROM return_of_all_tickers_table
        WHERE ticker = 'INTC'),		
	-- ticker 3
        ticker3_table AS (
        SELECT ticker,
				FORMAT(mean,5),
                FORMAT(standard_dev,5),
                FORMAT(risk_adjusted_return,5)
        FROM return_of_all_tickers_table
        WHERE ticker = 'SLV'),
	-- ticker 4
		ticker4_table AS (
        SELECT ticker,
				FORMAT(mean,5),
                FORMAT(standard_dev,5),
                FORMAT(risk_adjusted_return,5)
        FROM return_of_all_tickers_table
        WHERE ticker = 'AAAU'),
	-- correlation test for four tickers
		price1 AS (
		SELECT 	ticker AS ticker1, date, row_names, value AS value1
		FROM pricing_daily_new as pdn
		WHERE pdn.ticker = varTicker
		AND date >= CONCAT(2022-1,'-09-09')
		AND price_type = 'Adjusted'),
	-- create temp table for price 2
		price2 AS (
		SELECT 	date, ticker AS ticker2, value AS value2
		FROM pricing_daily_new as pdn
		WHERE pdn.ticker = 'INTC'
		AND date >= CONCAT(2022-1,'-09-09')
		AND price_type = 'Adjusted'),
	-- create temp table for lag price 1
		lag_price_table1 AS (
		SELECT date, row_names + 1 AS row_names, value AS lag_price1
		FROM pricing_daily_new
		WHERE ticker = varTicker
		AND date >= CONCAT(2022-1,'-09-08')
		AND price_type = 'Adjusted'),
	-- create temp table for lag price 2
		lag_price_table2 AS (
		SELECT date, row_names + 1 AS row_names, value AS lag_price2
		FROM pricing_daily_new
		WHERE ticker = 'INTC'
		AND date >= CONCAT(2022-1,'-09-08')
		AND price_type = 'Adjusted'),
	-- create temp table for return calculation
		return_table AS (
		SELECT p1.ticker1,
			   p2.ticker2,
			   value1, 
			   value2, 
			   lag_price1, 
			   lag_price2, 
			   (value1 - lag_price1)/lag_price1 AS return1, 
			   (value2 - lag_price2)/lag_price1 AS return2
		FROM price1 AS p1
		LEFT JOIN price2 AS p2
		ON p1.date = p2.date
		LEFT JOIN lag_price_table1 AS lp1
		ON lp1.row_names = p1.row_names
		LEFT JOIN lag_price_table2 AS lp2
		ON lp2.date = lp1.date),
	-- calculate mean, standard deviation
		mean AS (
		SELECT  ticker1, 
				ticker2, 
				FORMAT(AVG(value1),5) AS avg_p1, 
				FORMAT(AVG(value2),5) AS avg_p2, 
				FORMAT(AVG(return1),5) AS avg_r1, 
				FORMAT(AVG(return2),5) AS avg_r2,
				FORMAT(STDDEV(value1),5) AS std_p1,
				FORMAT(STDDEV(value2),5) AS std_p2,
				FORMAT(STDDEV(return1),5) AS std_r1,
				FORMAT(STDDEV(return2),5) AS std_r2
		FROM return_table
		GROUP BY ticker1, ticker2),
	-- calculate covariance
		covariance AS (
		SELECT FORMAT(AVG((value1 - avg_p1) * (value2 - avg_p2)),5) AS cov_price,
			   FORMAT(AVG((return1 - avg_r1) * (return2 - avg_r2)),5) AS cov_return
		FROM return_table AS rt
		LEFT JOIN mean AS m
		ON rt.ticker1 = m.ticker1),
	-- calculate correlation
		correlation AS (
		SELECT 	*,
				FORMAT(cov_price / (std_p1 * std_p2),5) AS corr_price,
				FORMAT(cov_return / (std_r1 * std_r2),5) AS corr_return
		FROM mean
		CROSS JOIN covariance
		)
	
        
		-- create display form
			SELECT 'Customer Portfolio','','','','','','','','','','','','',''
			UNION
			SELECT '','','','','','','','','','','','','',''
            UNION
			SELECT 'Customer Information','','','','','','','','','','','','',''
            UNION
			SELECT 'Customer ID','Full Name','Type','Account ID','Open Date','Status','Email','Location','','','','','',''
            UNION            
            SELECT *,'','','','','',''
            FROM customer_information_table
			UNION
			SELECT ' ','','','','','','',' ','','','','','',''
            UNION
			SELECT 'Customer Invesment Summary','','','','','','','','','','','','',''
            UNION
			SELECT 'Customer ID','AVG. Price','Total Quantity','Weight','Sec. Type','Major Class','AVG. Mean','AVG. Std.','AVG. Risk Adj. Return','','','','',''
            UNION
            SELECT *,'','','','',' '
            FROM group_by_customer
			UNION
			SELECT ' ','','','','','','','','',' ','','','',''
            UNION
			SELECT 'Customer Major Classes','','','','','','','','','','','','',''
            UNION
			SELECT 'Customer ID','AVG. Price','Total Quantity','Weight','Sec. Type','Major Class','AVG. Mean','AVG. Std.','AVG. Risk Adj. Return','','','','',''
            UNION
            SELECT *,'','','','',''
            FROM group_by_ticker
			UNION
			SELECT '',' ',' ','','','  ',' ','','','','','','',''
            UNION
			SELECT 'Ticker Recommendations 1','','','','','','','','','','','','',''
            UNION
			SELECT 'Ticker','Mean','Std. Dev','Risk Adj. Return','','','','','','','','','',''
            UNION
            SELECT *,'','','','','','','','','',''
            FROM ticker1_table
            UNION
			SELECT *,'','','','','','','','','',''
            FROM ticker2_table
            UNION
            SELECT 'Ticker Recommendations 2','','','','','','','',' ','','','','',''
            UNION
			SELECT 'Ticker','Mean','Std. Dev','Risk Adj. Return','','','','','','','','','',''
            UNION
            SELECT *,'','','','','','','','','',''
            FROM ticker3_table
            UNION
			SELECT *,'','','','','','','','','',''
            FROM ticker4_table
            UNION
            SELECT '','','','',' ','','','','','','','','',''
            UNION
            SELECT '','','','','','','','','','','','','',''
            UNION
			SELECT 'APPENDIX','','','','','','','','','','','','',''
            UNION
			SELECT 'Customer ID','Ticker','AVG. Value','Total Quantity','Weight','Sec Type','Major Class','Minor Class','Mean','STD.','Risk Adj. Return','','',''
            UNION
			SELECT *,'','',''
            FROM customer_ticker_return_detail_table
            ;




2./* this is used to make analysis but not in main code */


SELECT a.date, a.ticker, value, price_type, security_name,sp500_weight, sec_type, major_asset_class, minor_asset_class,
(a.value - a.lagged_price)/a.lagged_price as returns
FROM 
(
SELECT date, s.ticker, value, price_type, security_name,sp500_weight, sec_type, major_asset_class, minor_asset_class, LAG(value, 250) OVER(
						   PARTItiON BY p.ticker
                           ORDER BY date
                           ) AS lagged_price
 -- 1  according to time requiered
FROM pricing_daily_new AS p
JOIN security_masterlist AS s
ON p.ticker = s.ticker
WHERE price_type = 'Adjusted'
AND date LIKE '%2017%' 
) a

WHERE a.lagged_price IS NULL
ORDER BY (a.value - a.lagged_price)/a.lagged_price DESC
LIMIT 5000
;

3./* this is used to make analysis but not in main code */

SELECT cd.full_name, sm.major_asset_class, sm.minor_asset_class, sm.ticker, sm.security_name, hc.quantity
FROM account_dim AS ad
JOIN customer_details AS cd
ON  ad.client_id = cd.customer_id
JOIN holdings_current AS hc
ON ad.account_id = hc.account_id
JOIN security_masterlist AS sm
ON sm.ticker = hc.ticker
WHERE hc.account_id = '208' OR  hc.account_id = '20801' OR  hc.account_id = '20802' AND `date` = '2017-09-09'
GROUP BY sm.major_asset_class,sm.minor_asset_class,sm.ticker, sm.security_name;
