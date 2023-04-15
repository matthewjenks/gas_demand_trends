{\rtf1\ansi\ansicpg1252\cocoartf2708
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww22800\viewh13640\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 -- STEP 1: OCTOPUS PORTFOLIO SIZING USING CUSTOMER NUMBERS AND AQs at EUC LEVEL\
\
WITH gm as \
(\
\
-- CNs / AQs by EUC. Currently from GM code. Ideally from a daily industry snapshot. Alternative source: Procurement view\
\
SELECT settlement_date\
	,EUC\
	,count(mprn) as EUC_MPRN\
	,sum(annual_consumption) as EUC_AQ\
FROM consumer_gm.fnl_grossmargin_gasdaily\
WHERE updated_at = '2023-04-01'\
	AND settlement_date\
		BETWEEN '2020-03-01' AND '2023-03-31'\
	AND COALESCE(product_brand_id, 1) != 14\
GROUP BY settlement_date, EUC\
)\
,\
gm_mprns as\
(\
SELECT settlement_date\
	, sum(EUC_MPRN) as MPRN\
FROM gm\
GROUP BY 1\
ORDER BY 1\
)\
\
,\
\
-- STEP 2: ALLOCATED CONSUMPTION VOLUMES AT D+5\
\
total_uig AS \
(\
SELECT\
	delivery_date\
	, SUM(uig) AS uig\
FROM consumer.stg_uig \
GROUP BY delivery_date\
)\
\
,\
total AS\
(\
\
-- gemini daily D+5 allocated volumes and uig volumes\
\
SELECT\
	total_uig.delivery_date\
	, allocated_output as total_alloc_uig\
	, allocated_output - uig as alloc\
	, uig\
	, uig / allocated_output * 100 AS uig_as_proportion_of_total_alloc_uig\
FROM consumer.stg_allocation\
JOIN total_uig\
	ON stg_allocation.delivery_date = total_uig.delivery_date\
WHERE portfolio = 'oeuk'\
	AND total_uig.delivery_date\
		BETWEEN '2020-03-01' AND '2023-03-31'\
ORDER BY delivery_date\
)\
\
,\
sum_daily as\
(\
\
-- create combined total allocation from raw allocation (algorithm) and uig\
\
SELECT \
	delivery_date\
	, sum(alloc) as allocation\
	, sum(uig) as uig\
	, sum(total_alloc_uig) as total_allocation\
	FROM total\
GROUP BY 1\
ORDER BY 1\
)\
\
,\
sum_daily1 AS \
(\
\
-- add customer numbers\
\
SELECT d.*\
	, gm.mprn\
FROM sum_daily d JOIN gm_mprns gm\
	ON d.delivery_date = gm.settlement_date\
ORDER BY d.delivery_date\
)\
,\
\
-- STEP 2: WEATHER CORRECTION\
\
-- Seasonal Normal ALPs (Annual Load Profiles) and Weathered LPAs (Load Profile Allocation) by EUC\
\
latest_alp AS (\
SELECT\
	settlement_date\
	, ldz\
	, euc\
	, MAX_BY(alp, effective_from) / 365 AS seasonal_coefficient\
  FROM consumer.wh_annual_load_profile_latest\
  GROUP BY settlement_date, ldz, euc\
)\
\
,\
merged as (\
SELECT\
	lpa.settlement_date\
	, lpa.ldz\
	, lpa.euc\
	, seasonal_coefficient\
	, lpa\
FROM consumer.wh_lpa_latest lpa\
JOIN latest_alp alp\
	ON lpa.ldz = alp.ldz\
		AND lpa.settlement_date = alp.settlement_date\
			AND lpa.euc = alp.euc\
	WHERE lpa.settlement_date\
		BETWEEN '2020-03-01' AND '2023-03-31'\
)\
\
,\
merged2 as \
(\
SELECT gm.settlement_date\
	, gm.euc\
	, gm.euc_Aq\
	, mrg.seasonal_coefficient\
	, mrg.lpa\
FROM gm gm\
JOIN merged mrg\
	ON gm.EUC = mrg.EUC\
		AND gm.settlement_date = mrg.settlement_date\
WHERE gm.settlement_date\
	BETWEEN '2020-03-01' AND '2023-03-31'\
ORDER BY gm.euc, gm.settlement_date\
)\
,\
	weather as\
	\
-- Create AQ-weighted weather impact\
\
(\
SELECT *\
	, seasonal_coefficient * euc_AQ as snd_kwh\
	, lpa * euc_AQ as alloc_kwh\
	, (lpa * euc_AQ) - (seasonal_coefficient * euc_AQ) as weather_kwh\
FROM merged2\
  )\
,\
\
weather_factors AS\
(\
-- agg from euc to total portfolio\
 SELECT settlement_date\
  	, sum (weather_kwh) / sum (alloc_kwh) as total_weather_factor\
FROM weather\
GROUP BY settlement_date\
ORDER BY settlement_date\
)\
,\
\
-- STEP 3: WEATHER-CORRECT ALLOCATIONS\
\
sum_daily_weathercorrected as\
(\
SELECT\
	d.*\
	, wf.total_weather_factor\
	, d.allocation - (d.allocation * wf.total_weather_factor) as weather_corrected_allocation\
	, (d.allocation - (d.allocation * wf.total_weather_factor)) + d.uig as total_weather_corrected_allocation\
FROM sum_daily1 d\
JOIN weather_factors wf\
	ON d.delivery_date = wf.settlement_date\
ORDER BY d.delivery_date\
)\
,\
\
sum_daily_weathercorrected1 as\
(\
SELECT\
	*\
	, YEAR(delivery_date) as delivery_year\
	, MONTH(delivery_date) as delivery_month\
	, date_add(last_day(add_months(delivery_date, -1)),1) as delivery_monthyear\
FROM sum_daily_weathercorrected	\
)\
,\
\
-- STEP 4: AGGREGATE TO MONTHLY AND CREATE YEAR ON YEAR PERCENTAGE MOVEMENTS\
\
sum_monthly as\
(\
SELECT\
	delivery_monthyear\
	, delivery_month\
	, delivery_year\
	, avg(mprn) as mprn\
	, sum(allocation) as allocation\
	, sum(uig) as uig\
	, sum(total_allocation) as total_allocation\
	, sum(uig) / sum(allocation) as uig_alloc_percent\
	, sum(total_allocation) / avg(mprn) as total_alloc_per_mprn\
	, sum(weather_corrected_allocation) as weather_corrected_allocation\
	, sum(total_weather_corrected_allocation) as total_weather_corrected_allocation\
	, sum(total_weather_corrected_allocation) / avg(mprn) as total_weather_corrected_alloc_per_mprn \
	, add_months(delivery_monthyear, -12) as delivery_monthyear_12m\
FROM sum_daily_weathercorrected1\
GROUP BY 1, 2, 3\
ORDER BY 1, 2, 3\
)\
,\
\
lag_joiner AS\
(\
SELECT delivery_monthyear\
	, allocation\
	, uig\
	, total_allocation\
	, total_alloc_per_mprn\
	, weather_corrected_allocation\
	, total_weather_corrected_allocation\
	, total_weather_corrected_alloc_per_mprn\
FROM sum_monthly\
)\
\
SELECT\
	m.delivery_monthyear\
	, m.delivery_month\
	, m.delivery_year\
	, m.mprn\
	, m.allocation\
	, m.uig\
	, m.total_allocation\
	, m.total_alloc_per_mprn\
	, m.uig_alloc_percent\
	, m.weather_corrected_allocation\
	, m.total_weather_corrected_allocation\
	, m.total_weather_corrected_alloc_per_mprn\
	, (m.allocation / lj.allocation) - 1 as allocation_yoy_percent\
	-- , (m.uig / lj.uig) - 1 as uig_yoy_percent\
	, (m.total_allocation / lj.total_allocation) - 1 as total_allocation_yoy_percent\
	-- , (m.total_alloc_per_mprn / lj.total_alloc_per_mprn as total_alloc_per_mprn_yoy_percent\
	, (m.weather_corrected_allocation / lj.weather_corrected_allocation) - 1 as weather_corrected_allocation_yoy_percent\
	, (m.total_weather_corrected_allocation / lj.total_weather_corrected_allocation) - 1 as total_weather_corrected_allocation_yoy_percent\
	, (m.total_weather_corrected_alloc_per_mprn / lj.total_weather_corrected_alloc_per_mprn) - 1 as total_weather_corrected_alloc_per_mprn_yoy_percent\
FROM sum_monthly m LEFT JOIN lag_joiner lj ON m.delivery_monthyear_12m = lj.delivery_monthyear\
ORDER BY m.delivery_monthyear}