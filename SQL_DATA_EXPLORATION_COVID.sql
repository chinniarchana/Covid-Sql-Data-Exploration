/*
COVID 19 SQL DATA EXPLORATION
[LINK TO DATASET](https://ourworldindata.org/covid-deaths)
SKILLS USED: AGGREGATE FUNCTIONS, CONVERTING DATA TYPES, JOINS, WINDOW FUNCTIONS, CTE'S, TEMP TABLES, CREATING VIEWS
*/

 
SELECT*
FROM ProtfolioProject..Covid_Deaths
ORDER BY 3,4;

SELECT*
FROM ProtfolioProject..Covid_Vaccinations
ORDER BY 3,4;


-- SELECTING DATA THAT WE ARE GOING TO BE BEGIN WITH

SELECT location, date, total_cases, new_cases, total_deaths, population
FROM ProtfolioProject..Covid_Deaths
ORDER BY 1,2;


-- LOOKING AT TOTAL CASES VS TOTAL DEATHS 
-- CALCULATING DEATH PERCENTAGE 

SELECT location, date, total_cases, total_deaths, ROUND((total_deaths/total_cases)*100,2) AS Death_Percentage
FROM ProtfolioProject..Covid_Deaths
ORDER BY 1,2;


-- DEATH PERCENTAGE IN SPECIFIC COUNTRY

SELECT location, date, total_cases, total_deaths, ROUND((total_deaths/total_cases)*100,2) AS Death_Percentage
FROM ProtfolioProject..Covid_Deaths
WHERE location LIKE 'India'
ORDER BY 2 DESC;


-- AS OF APRIL 15, 2022, INDIA HAS 3.6% 0F TOTAL DEATHS COMPARED TO TOTAL CASES

SELECT location, MAX(total_cases), MAX(CAST(total_deaths AS INT)), ROUND(MAX((total_deaths/total_cases))*100,2) AS Death_Percentage
FROM ProtfolioProject..Covid_Deaths
WHERE location LIKE 'India'
GROUP BY location


-- LOOKING AT TOTALCASES VS POPULATION
-- CALCULATING PERCENTAGE OF POPULATION INFECTED

SELECT location, date, total_cases, population, ROUND((total_cases/population)*100,2) AS Population_Infected_Percentage
FROM ProtfolioProject..Covid_Deaths
ORDER BY 1,2;


-- LOOKING AT TOTAL CASES, POPULATION AND PERCENTAGE OF POPULATION INFECTED IN SPECIFIC COUNTRY

SELECT location, date, total_cases, population, ROUND((total_cases/population)*100,2) AS Population_Infected_Percentage
FROM ProtfolioProject..Covid_Deaths
WHERE location LIKE 'India'
ORDER BY 2 DESC; 


-- LOOKING AT COUNTRIES WITH THE HIGHEST INFECTION RATE VS POPULATION

SELECT location, population, MAX(total_cases) AS Highest_Infection_Count, ROUND(MAX((total_cases/population))*100,2) AS Population_Infected_Percentage
FROM ProtfolioProject..Covid_Deaths
GROUP BY location, Population
ORDER BY 4 DESC;


-- TOP 10 COUNTRIES WITH HIGHEST INFECTION RATE COMPARED TO POPULATION
-- AS OF APRIL 15, 2022, FAEROE ISLANDS HAS THE HIGHEST INFECTED RATE COMPARED TO POPULATION

SELECT TOP 10 location, population, MAX(total_cases) AS Highest_Infection_Count, ROUND(MAX((total_cases/population))*100,2) AS Population_Infected_Percentage
FROM ProtfolioProject..Covid_Deaths
GROUP BY location, Population
ORDER BY 4 DESC;


-- HIGHEST INFECTION RATE COMPARED TO POPULATION IN SPECIFIC COUNTRY
-- AS OF APRIL 15, 2022, INDIA HAS 3.09% OF POPULATION INFECTED

SELECT location, population, MAX(total_cases) AS Highest_Infection_Count, ROUND(MAX((total_cases/population))*100,2) AS Population_Infected_Percentage
FROM ProtfolioProject..Covid_Deaths
WHERE location LIKE 'India'
GROUP BY location, Population;


-- LOOKING AT COUNTRIES WITH THE HIGHEST DEATH COUNT PER POPULATION
-- AS OF APRIL 15, 2022, UNITED STATES HAS THE HIGHEST DEATH COUNT WITH 9,88,558 TOTAL DEATHS 

SELECT location, MAX(CAST(total_deaths AS int)) AS Total_Death_Count
FROM ProtfolioProject..Covid_Deaths
WHERE continent IS NOT NULL AND total_deaths IS NOT NULL
GROUP BY location
ORDER BY 2 DESC;

-- TOP 10 COUNTRIES IN TOTAL DEATHS

SELECT TOP 10 location, MAX(CAST(total_deaths AS int)) AS Total_Death_Count
FROM ProtfolioProject..Covid_Deaths
WHERE continent IS NOT NULL AND total_deaths IS NOT NULL
GROUP BY location
ORDER BY 2 DESC;


-- HIGHEST DEATH COUNT IN SPECIFIC COUNTRY 
-- AS OF APRIL 15, 2022, INDIA HAS 1,393,409,033 POPULATION AND 5,21,747 TOTAL DEATHS

SELECT location, population, MAX(CAST(total_deaths AS INT)) AS Total_Death_Count
FROM ProtfolioProject..Covid_Deaths
WHERE location LIKE 'India'
GROUP BY location, population;


-- LET'S BREAK THINGS DOWN BY CONTINENT
-- SHOWING CONTINENTS WITH THE HIGHEST DEATH COUNT PER POPULATION
-- AS 0F APRIL 15, 2022, NORTH AMERICA HAS THE HIGHEST DEATH COUNT

SELECT continent, MAX(CAST(total_deaths AS int)) AS Total_Death_Count
FROM ProtfolioProject..Covid_Deaths
WHERE continent IS NOT NULL 
GROUP BY continent
ORDER BY 2 DESC;


-- GLOBAL NUMBERS
-- AS 0F APRIL 15, 2022, THERE ARE 502,453,378 TOTAL CASES, 6,154,985 TOTAL DEATHS AND 1.22% DEATH PERCENTAGE GLOBAL

SELECT SUM(new_cases) AS Total_Cases, sum(CAST(new_deaths AS int)) AS Total_Deaths, ROUND((sum(CAST(new_deaths AS int))/SUM(new_cases))*100,2) AS Death_Percentage
FROM ProtfolioProject..Covid_Deaths
WHERE continent IS NOT NULL
ORDER BY 1,2;


-- JOIN COVID DEATHS AND COVID VACCINATIONS

SELECT *
FROM ProtfolioProject..Covid_Deaths AS d
JOIN ProtfolioProject..Covid_Vaccinations AS v
       ON d.location = v.location 
	   AND d.date = v.date 


-- LOOKING AT TOTAL POPULATION VS VACCINATIONS
-- CALCULATING ROLLING PEOPLE VACCINATED 


SELECT d.continent, d.location, d.date, d.population, v.new_vaccinations, SUM(CONVERT(INT,v.new_vaccinations)) 
       OVER (PARTITION BY d.location ORDER BY d.location, d.date) AS Rolling_People_Vaccinated --,(Rolling_People_Vaccinated/population)*100
FROM ProtfolioProject..Covid_Deaths AS d
JOIN ProtfolioProject..Covid_Vaccinations AS v
       ON d.location = v.location 
	   AND d.date = v.date
WHERE v.continent IS NOT NULL 
ORDER BY 2,3;


-- USING CTE TO PERFORM CALCULATION ON PARTITION BY IN PREVIOUS QUERY
-- CALCULATING PERCENTAGE OF ROLLING PEOPLE VACCINATED FROM TOTAL POPULATION

WITH popvsvac (continent, location, date, population, new_vaccinations, Rolling_People_Vaccinated)
AS 
(
SELECT d.continent, d.location, d.date, d.population, v.new_vaccinations, SUM(CONVERT(INT,v.new_vaccinations)) 
       OVER (PARTITION BY d.location ORDER BY d.location, d.date) AS Rolling_People_Vaccinated
FROM ProtfolioProject..Covid_Deaths AS d
JOIN ProtfolioProject..Covid_Vaccinations AS v
       ON d.location = v.location 
	   AND d.date = v.date 
WHERE d.continent IS NOT NULL
-- ORDER BY 2,3
)
SELECT *, ROUND((Rolling_People_Vaccinated/population)*100,2) AS Vaccinated_Percentage
FROM popvsvac


-- USING TEMP TABLE TO PERFORM CALCULATION ON PARTITION BY IN PREVIOUS QUERY

DROP TABLE IF EXISTS #Vaccinated_Percentage
CREATE TABLE #Vaccinated_Percentage
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
Rolling_People_Vaccinated numeric
)

INSERT INTO #Vaccinated_Percentage
SELECT d.continent, d.location, d.date, d.population, v.new_vaccinations, SUM(CONVERT(INT,v.new_vaccinations)) 
       OVER (PARTITION BY d.location ORDER BY d.location, d.date) AS Rolling_People_Vaccinated --, (Rolling_People_Vaccinated/Population)*100 
FROM ProtfolioProject..Covid_Deaths AS d
JOIN ProtfolioProject..Covid_Vaccinations AS v
       ON d.location = v.location 
	   AND d.date = v.date 
WHERE d.continent IS NOT NULL
-- ORDER BY 2,3

SELECT *, ROUND((Rolling_People_Vaccinated/population)*100,2) AS Vaccinated_Percentage
FROM #Vaccinated_Percentage


-- CREATING VIEW TO STORE DATA FOR LATER VISUALIZATIONS

CREATE VIEW Vaccinated_Percentage_View AS
SELECT d.continent, d.location, d.date, d.population, v.new_vaccinations, SUM(CONVERT(INT,v.new_vaccinations)) 
       OVER (PARTITION BY d.location ORDER BY d.location, d.date) AS Rolling_People_Vaccinated --, (Rolling_People_Vaccinated/Population)*100 
FROM ProtfolioProject..Covid_Deaths AS d
JOIN ProtfolioProject..Covid_Vaccinations AS v
       ON d.location = v.location 
	   AND d.date = v.date 
WHERE d.continent IS NOT NULL
-- ORDER BY 2,3

DROP VIEW [Vaccinated_Percentage_View

SELECT *
FROM Vaccinated_Percentage
