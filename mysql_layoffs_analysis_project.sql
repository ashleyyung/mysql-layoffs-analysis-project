-- MySQL Layoffs Analysis Project
-- Based on tutorial from Alex the Anayst, adapted for practice

-- Data Cleaning
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Remove Null Values or Blank Values
-- 4. Remove Redundant Columns / Rows

-- create staging table (copy of raw table)
SELECT
	*
FROM
	layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT 
	*
FROM 
	layoffs_staging;

INSERT layoffs_staging
SELECT 
	*
FROM 
	layoffs;
    
SELECT 
	*
FROM 
	layoffs_staging;

-- 1. Remove Duplicates

-- need to create ranking as there is no unique identifier
WITH duplicate_cte AS (
	SELECT 
		*,
		ROW_NUMBER() OVER
			(PARTITION BY
				company,
                location,
                industry, 
                total_laid_off, 
                percentage_laid_off, 
                'date',
                stage,
                country,
                funds_raised_millions) AS row_num
	FROM layoffs_staging
)

SELECT
	*
FROM
	duplicate_cte
WHERE
	row_num > 1;

-- create another staging table with duplicates removed
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

SELECT 
	*
FROM 
	layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT 
	*,
	ROW_NUMBER() OVER
		(PARTITION BY
			company,
			location,
			industry, 
			total_laid_off, 
			percentage_laid_off, 
			'date',
			stage,
			country,
			funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT 
	*
FROM 
	layoffs_staging2
WHERE
	row_num > 1;
    
DELETE
FROM layoffs_staging2
WHERE row_num > 1;
    
SELECT 
	*
FROM 
	layoffs_staging2;
    
-- 2. Standardize the Data

-- trim the blank space
SELECT
	company,
	TRIM(company)
FROM
	layoffs_staging2;
    
UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT
	DISTINCT industry
FROM
	layoffs_staging2;

-- rename rows with varying names
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';  

-- trim trailing values
SELECT
	DISTINCT country,
    TRIM(TRAILING '.' FROM country)
FROM
	layoffs_staging2
ORDER BY
	country;
    
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- update date column from text to date datatype
SELECT 
	`date`
FROM
	layoffs_staging2;
    
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT 
	*
FROM 
	layoffs_staging2;

-- 3. Remove Null Values or Blank Values

SELECT 
	*
FROM 
	layoffs_staging2
WHERE 
	total_laid_off IS NULL
	AND
	percentage_laid_off IS NULL;
    
SELECT
	*
FROM
	layoffs_staging2
WHERE
	industry IS NULL
    OR
    industry = '';
    
-- populate missing values with data from other rows
SELECT
	*
FROM
	layoffs_staging2
WHERE
	company = 'Airbnb';
    
SELECT
	*
FROM
	layoffs_staging2 AS t1
JOIN
	layoffs_staging2 AS t2
ON
	t1.company = t2.company
    AND
    t1.location = t2.location
WHERE
	(t1.industry IS NULL OR t1.industry = '')
    AND
    t2.industry IS NOT NULL;
  
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';
  
UPDATE 
	layoffs_staging2 AS t1
JOIN 
	layoffs_staging2 AS t2
ON 
	t1.company = t2.company
SET 
	t1.industry = t2.industry
WHERE
	t1.industry IS NULL
	AND
	t2.industry IS NOT NULL;

-- 4. Remove Redundant Columns / Rows

-- as these rows don't provide any helpful information, can be removed
SELECT 
	*
FROM 
	layoffs_staging2
WHERE 
	total_laid_off IS NULL
	AND
	percentage_laid_off IS NULL;
    
DELETE
FROM 
	layoffs_staging2
WHERE 
	total_laid_off IS NULL
	AND
	percentage_laid_off IS NULL; 
    
SELECT 
	*
FROM 
	layoffs_staging2; 

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Exploratory Data Analysis

SELECT 
	*
FROM 
	layoffs_staging2; 
    
-- check out how long layoffs have been documented for in this dataset
SELECT
	MIN(`date`),
    Max(`date`)
FROM
	layoffs_staging2;

SELECT
	MAX(total_laid_off),
    MAX(percentage_laid_off)
FROM
	layoffs_staging2;
    
SELECT 
	*
FROM 
	layoffs_staging2
WHERE
	percentage_laid_off = 1
ORDER BY
	funds_raised_millions DESC; 
    
-- check out total laid off grouped by various attributes
SELECT
	company,
    SUM(total_laid_off)
FROM
	layoffs_staging2
GROUP BY
	company
ORDER BY 
	2 DESC;
    
SELECT
	industry,
    SUM(total_laid_off)
FROM
	layoffs_staging2
GROUP BY
	industry
ORDER BY 
	2 DESC;
  
SELECT
	country,
    SUM(total_laid_off)
FROM
	layoffs_staging2
GROUP BY
	country
ORDER BY 
	2 DESC;
    
SELECT
	YEAR(`date`),       -- note: only first 3 months recorded for 2023 
    SUM(total_laid_off)
FROM
	layoffs_staging2
GROUP BY
	YEAR(`date`)
ORDER BY 
	1 DESC;
    
SELECT
	stage,      
    SUM(total_laid_off)
FROM
	layoffs_staging2
GROUP BY
	stage
ORDER BY 
	2 DESC;
    
-- not super helpful because we don't know the total # of employees
SELECT
	company,
    SUM(percentage_laid_off),
    AVG(percentage_laid_off)
FROM
	layoffs_staging2
GROUP BY
	company
ORDER BY 
	2 DESC;
    
-- calculate rolling total layoffs
SELECT
	SUBSTRING(`date`, 1, 7) AS `MONTH`,
    SUM(total_laid_off)
FROM
	layoffs_staging2
WHERE
	SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY
	SUBSTRING(`date`, 1, 7)
ORDER BY
	1 ASC;

WITH rolling_total AS
(
	SELECT
		SUBSTRING(`date`, 1, 7) AS `MONTH`,
		SUM(total_laid_off) AS total_off
	FROM
		layoffs_staging2
	WHERE
		SUBSTRING(`date`, 1, 7) IS NOT NULL
	GROUP BY
		SUBSTRING(`date`, 1, 7)
	ORDER BY
		1 ASC
)

SELECT
	`MONTH`,
    total_off,
    SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM
	rolling_total;
    
-- top 5 highest # of layoffs each year
SELECT
	company, 
    YEAR(`date`),
    SUM(total_laid_off)
FROM
	layoffs_staging2
GROUP BY
	company,
    YEAR(`date`)
ORDER BY 
	3 DESC;

WITH company_year (company, years, total_laid_off) AS
(
	SELECT
		company, 
		YEAR(`date`),
		SUM(total_laid_off)
	FROM
		layoffs_staging2
	GROUP BY
		company,
		YEAR(`date`)
),

company_year_rank AS
(
SELECT
	*,
    DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM
	company_year
WHERE
	years IS NOT NULL
)

SELECT
	*
FROM 
	company_year_rank
WHERE
	ranking <= 5;