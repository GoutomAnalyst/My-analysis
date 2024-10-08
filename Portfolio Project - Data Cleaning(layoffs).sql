-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- data cleaning
-- 1. Removie Dublicates
-- 2. Standardise The Data
-- 3. Null Value/ Blank Values
-- 4. Remove Any Columns
                                     
-- create a staging table

select*
from layoffs;

create table layoffs_staging
like layoffs;

select*
from layoffs_staging;

insert layoffs_staging
select*
from layoffs;

--                              Removie Dublicates

select*,
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,`date`) as row_num
from layoffs_staging;

with dublicate_cte as
(
select*,
row_number() over(
partition by company,location,
industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging
)
select*
from dublicate_cte
where row_num > 1;


with dublicate_cte as
(
select*,
row_number() over(
partition by company,location,
industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging
)
delete
from dublicate_cte
where row_num > 1;



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

select*
from layoffs_staging2;

insert into layoffs_staging2
select*,
row_number() over(
partition by company,location,
industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

select*
from layoffs_staging2
where row_num > 1;

delete
from layoffs_staging2
where row_num > 1;

select*
from layoffs_staging2
where row_num > 1;

--                               Standardizing data

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select*
from layoffs_staging2
Where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
Where industry like 'Crypto%';

select distinct country, trim(trailing '.' from country)
from layoffs_staging2;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- Changing data type (Date table)

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2;

 --                               Handling Null Values/ Blank Values

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update  layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where t1.industry is null
And  t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
And  t2.industry is not null;

select *
from layoffs_staging2
where company = 'Airbnb';

--                               Remove Column

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select*
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

