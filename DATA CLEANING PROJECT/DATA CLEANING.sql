/*
 Data Cleaning Project Overview

In this project, we will perform data cleaning on a dataset by following a systematic approach. The steps are as follows:

    Schema Creation
    First, we will create a schema named world_layoffs. This schema will serve as the foundation for our project.

    Table Importation
    Next, we will import the necessary tables and data into the world_layoffs schema.

    Data Cleaning Process
    To clean the data, we will apply the following four steps:
    
        1.Remove Duplicate Values: We will eliminate any duplicate records to ensure the data is unique.
        2.Standardize the Data: The data will be standardized to ensure consistency, such as formatting dates or correcting case sensitivity.
        3.Remove Null/Blank Values: We will remove any records or fields that contain null or blank values to maintain data integrity.
        4.Remove Unnecessary Columns: Columns that do not contribute to the analysis or are irrelevant will be dropped.

By following these steps, we will ensure the data is clean, consistent, and ready for further analysis.
Lets Dig in.
*/
# 1.Remove Duplicate Values:
# We will see is our table and data are ok.
SELECT *
FROM layoffs;

# Then we will going to dulpicate our main table and name it layoffs_staging. we don't want any harm at our raw data. 
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT*
FROM layoffs;

/*Now we will partition the whole table by column. And we will get a number 1 for distinct row. But if there is any
duplicate value then we will get 2.*/
SELECT *,
ROW_NUMBER () OVER(
PARTITION BY company,location, industry, total_laid_off , percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

# Now we will put the query in the CTE for the filtration. where we will see the row which have values 2.
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER () OVER(
PARTITION BY company, location, industry, total_laid_off , percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

/* Here we will get the Rows which have valu = 2. But the matter of fact we can't update a CTE. so we will create new table 
name world_staging2 by using the same query we use for filtration. and by this method we are deleting all duplicate values.*/
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

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER () OVER(
PARTITION BY company,location, industry, total_laid_off , percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;



DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

# 2.Standardize the Data: Here we take variable action like TRIM or many other to make data more clear.

UPDATE layoffs_staging2
SET company = TRIM(company);


SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

# 3.Remove Null/Blank Values: We will remove any records or fields that contain null or blank values to maintain data integrity.
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# 4.Remove Unnecessary Columns: Columns that do not contribute to the analysis or are irrelevant will be dropped.
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2

# Done!!! Our data is good to go.
