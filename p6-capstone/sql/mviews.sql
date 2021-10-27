CREATE MATERIALIZED VIEW immigrant_stay_length_by_state AS
SELECT 
    state_settled_code,
    departure_month,
    departure_month - 4 AS no_months_in_usa,
    COUNT(*) as no_immigrants
FROM immigration_base
GROUP BY state_settled_code, departure_month
ORDER BY no_immigrants DESC;

CREATE MATERIALIZED VIEW immigrants_by_city AS
SELECT 
    arrival_port_state,
    arrival_port_city,
    COUNT(*) as no_immigrants,
    ROUND(AVG(age), 2) AS avg_age,
    SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS no_males,
    SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS no_females
FROM immigration_base
GROUP BY arrival_port_state, arrival_port_city
ORDER BY no_immigrants DESC;

CREATE MATERIALIZED VIEW immigrants_by_state AS
SELECT 
    arrival_port_state,
    COUNT(*) as no_immigrants,
    ROUND(AVG(age), 2) AS avg_age,
    SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS no_males,
    SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS no_females
FROM immigration_base
GROUP BY arrival_port_state
ORDER BY no_immigrants DESC;

CREATE MATERIALIZED VIEW immigrants_by_state_settled AS
SELECT 
    state_settled_code,
    COUNT(*) as no_immigrants,
    ROUND(AVG(age), 2) AS avg_age,
    SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS no_males,
    SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS no_females
FROM immigration_base
GROUP BY state_settled_code
ORDER BY no_immigrants DESC;
