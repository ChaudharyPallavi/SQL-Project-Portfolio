-- Exploratory Data Analysis

USE layoffs;

-- Let's first review the dataset structure and contets
SELECT *
FROM layoffs_staging2;

-- Identifies the highest recorded layoffs and the maximum percentage of layoffs in the company.
SELECT  max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2 ;

/* Lists companies that laid off all employees, sorted by the total laid off.
   finds the most funded companies that laid off all employees.*/
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC ;

-- Here 1 basically represents 100 percent layoffs
SELECT COUNT(funds_raised_millions)
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC ;

-- Finds which companies, industries, and countries experienced the most layoffs.
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Determines the dataset's time range
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

--  Aggregates layoffs per year.
SELECT year(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY year(`date`)
ORDER BY 1 DESC;

-- Analyzes layoffs by business stage.
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- 
  SELECT substring(`date`,1,7) AS `Month`, SUM(total_laid_off)
  FROM layoffs_staging2
  WHERE substring(`date`,1,7) IS NOT NULL
  GROUP BY `Month`
  ORDER BY 1 Asc; 
 
 SELECT substring(`date`,1,7) AS `Year`, SUM(total_laid_off)
  FROM layoffs_staging2
  WHERE substring(`date`,1,7) IS NOT NULL
  GROUP BY `Year`
  ORDER BY 1 Asc; 
 
 -- Rolling Total of layoffs over time
 -- Used window functions to compute cumulative layoffs month by month.
  
  WITH Rolling_Total As
  (
   SELECT substring(`date`,1,7) AS `Month`, SUM(total_laid_off) AS total_off
  FROM layoffs_staging2
  WHERE substring(`date`,1,7) IS NOT NULL
  GROUP BY `Month`
  ORDER BY 1 Asc
  )
 SELECT `Month`, total_off,
 SUM(total_off) OVER (ORDER BY `Month`) AS rolling_total
 from Rolling_Total;

-- Earlier we looked at companies with most layoffs
-- Now let's look at that per year 
 
 SELECT company, year(`date`), SUM(total_laid_off)
 from layoffs_staging2
 GROUP BY company, year(`date`)
 ORDER BY company  ASC;
 
 -- Year-wise Company Layoff Trends
 -- It ranks companies by  highest layoffs per year.
 WITH Year_wise_laid_off (company, years, total_laid_off) AS
 (
  SELECT company, year(`date`), SUM(total_laid_off)
 from layoffs_staging2
 GROUP BY company, year(`date`)
 )
 SELECT *,
 DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
 FROM Year_wise_laid_off
 WHERE years IS NOT NULL
 ORDER BY Ranking ASC;
 
 
 -- Top 5 Companies with the Most Layoffs Each Year
 WITH Year_wise_laid_off (company, years, total_laid_off) AS
 (
  SELECT company, year(`date`), SUM(total_laid_off)
 from layoffs_staging2
 GROUP BY company, year(`date`)
 ), Company_year_rank AS
 (
 SELECT *,
 DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
 FROM Year_wise_laid_off
 WHERE years IS NOT NULL
 )
SELECT*
FROM Company_year_rank
WHERE Ranking <= 5;
 