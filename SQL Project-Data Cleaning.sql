-- DATA CLEANING

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values 
-- 4. remove any columns and rows that are not necessary 

use layoffs;  
SELECT * FROM layoffs.layoffs;
ALTER TABLE layoffs RENAME TO World_layoffs;

SELECT * 
from World_layoffs; 

/* Staging table is a kind of temporary table were we will hold our data temporarily 
   It sereves as an intermediary step in data transformation process 
   A staging table acts as a backup, allowing data transformation without affecting the original dataset.*/
 CREATE TABLE layoffs_staging
LIKE world_layoffs;     # This create a new empty table called layoffs_staging that has the same structure 
                        # (i.e., same columns, data types, constraints) as the existing world_layoffs table — but without copying the data.

SELECT *
from layoffs_staging;

INSERT layoffs_staging    # This copies all rows from the world_layoffs table and inserts them into the layoffs_staging table.
SELECT *				  # Now, layoffs_staging becomes a duplicate of world_layoffs, both in structure and content 
FROM world_layoffs;


# First let's check for duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,funds_raised_millions,stage, country,
 industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;


-- This identifies duplicates rows and assigns row numbers to each duplicate set.
-- Insted of writing a nested subquery I have used CTE in order to simplify complex queries

WITH duplicate_cte As
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

# Now what we are going to do is creating another temporary table with new column as row_num.
# So that we can identify and filter duplicates later

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

# It groups the data by all those columns: company, location, funds_raised_millions, stage, country, industry, total_laid_off, percentage_laid_off, date.
# Within each group, it assigns a unique row number starting from 1.
# So, if there are exact duplicate rows in terms of all those fields, one will be row number 1, another 2, and so on.

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,funds_raised_millions,stage, country,
 industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

SELECT *
from layoffs_staging2
WHERE row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2 or greater essentially
DELETE
FROM layoffs_staging2
WHERE row_num >1;

SELECT *
from layoffs_staging2;


-- Standardizing Data ( It's a process of finding issues in the data and fixing it)

SELECT company,
trim(company)
from layoffs_staging2;

-- By default trim() function removes leadind and trailing spaces from a string
-- It will make text column more standardized
UPDATE layoffs_staging
set company = trim(company);

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto 
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

-- In SQL, TRAILING is a keyword used with the TRIM() function to remove specific characters from the end (right side) of a string.
SELECT DISTINCT country, trim(trailing '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = trim(trailing '.' FROM country)
WHERE country LIKE 'United States%';

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

-- Let's also fix the date columns:
SELECT `date`
FROM layoffs_staging2;

-- This query will convert date column which is currently stored as a text type in the format 'MM/DD/YYYY'
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase
-- so there isn't anything I want to change with the null values

SELECT *
from layoffs_staging2;

SELECT *
from layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- let's take a look at these
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';
-- nothing wrong here

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Airbnb%';
-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

 
# Searching for rows where the industry column is an empty string ('')
# Replacing them with a NULL value, which more accurately represents "missing" or "unknown" data.

 UPDATE layoffs_staging2
 set industry = NULL
 WHERE industry = '';
 
# t1: Rows where the industry is missing (NULL or empty string)
# t2: Rows where the same company does have an industry value
# we are joining the table to itself on the company name, trying to "borrow" industry values from rows where it's known.

SELECT *
FROM layoffs_staging2 as t1
JOIN  layoffs_staging2 as t2
	ON t1.company = t2.company
 WHERE (t1.industry is NULL OR t1.industry = '')
 AND t2.industry IS NOT NULL;
 
UPDATE layoffs_staging2
SET industry = 'Travel'
WHERE company = 'Airbnb' AND industry IS NULL;

UPDATE layoffs_staging2
SET industry = 'Consumer'
WHERE company = 'Juul' AND industry IS NULL;

-- This query will retrive all the rows with both total_laid_off and percentage_laid_off is null
-- These rows probably don’t contain useful layoff information
SELECT *
from layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- This deletes the same records identified above.
DELETE
from layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select *
from layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
