# Big Data Project

## Project Overview

This project analyzes IMDb data using **PySpark**, covering movies, TV shows, people, ratings, and more by combining multiple IMDb datasets. The goal is to answer important business questions about industry trends, ratings, genres, and professionals through large-scale data analysis.

---
## Data Cleaning and Preprocessing

A detailed table of all columns and their cleaning decisions is available in [`docs/data_cleaning_decisions.pdf`](docs/data_cleaning_decisions.pdf). For convenience, you can also view it as a [Google Sheet](https://docs.google.com/spreadsheets/d/1i52seVmuWA1UiSCvTe07IA5_Yk3pGCG0QF10QySD0oo/edit?usp=sharing) or download the [`Excel file`](docs/data_cleaning_decisions.xlsx).

Before analysis, all datasets were cleaned and preprocessed to ensure consistency and reliability. The following strategies were applied:

- Replaced placeholder values such as `\N` (used to represent missing data) with proper null values.
- Removed columns with excessive missing data, or they are not needed for analysis and could not be reliably filled.
- Applied missing value handling techniques, using the **mode** for categorical fields and the **median** for numerical fields.

These steps helped standardize the datasets and minimize noise in the analysis.

---

## Business Questions for the Data

1. Which actors have starred in the greatest number of movies or TV shows?  
2. What are the 5 most popular genres by the number of movies in each genre?  
3. How many movies were released in each release year?  
4. What are the three main professions most common among all people in the database?  
5. Which movie or TV show has the highest number of votes?  
6. In which years were the 5 movies with the highest number of votes released, and what are their titles?  
7. How many people combine the professions of "actor" and "director"?  
8. When was the highest number of movies released?  
9. Which regions produce the most movies and TV shows?  
10. How does the number of movies from a certain region change over time?  
11. Is there a connection between the number of genres a director works in and the ratings of their movies?  
12. Which regions have the most foreign (non-original) titles?  
13. Movies from which regions have the highest ratings?  
14. Is there a correlation between the length of a movie and its rating?  
15. Which genres have the highest ratings?  
16. Which director has worked on the greatest number of movies?  
17. Movies of which genre have the longest duration?  
18. Which movies have the highest ratings?  
19. How does the number of movies in a certain genre change over time?  
20. Is there a connection between the number of episodes in a TV series and its average rating?  
21. How many movies have an average rating above 5?  
22. How many movies have been translated into Ukrainian?  
23. How many movies has William K.L. Dickson (nm0005690) directed?  
24. What are the unique IDs and ratings of movies with more than 10,000 votes?  
25. What are the main professions of Raquel Welch (nm0000079)?  
26. What are the unique IDs and local titles for movies from the regions 'DE' (Germany) or 'CH' (Switzerland)?  
27. How many episodes are there in the 1990 TV series "Twin Peaks"?  
28. Which movies are longer than two hours?  
29. Which movies are permitted for children to watch?  
30. In which region are there the most translated (non-original) movies?  
31. Which movies with a high average rating (over 8.0) have the largest number of votes and rank in the top 10 by vote count?  
32. What are the top five longest movies in each genre?  
33. What is the duration of the longest movie for each format, and what is the difference in duration between each movie and the longest movie in the same format (possible formats: 'movie', 'short', 'tvseries', etc.)?  
34. What is the rank by movie duration for each movie within its release year?  
35. How many movies (or other types) of each genre were released each year?  
36. How many movies have each whole-number rating?  
37. What is the average duration of movies for each year?  
38. What are the minimum, maximum, and average durations of movies for each title type?  
39. How many TV series has each director directed?

---

## Business Questions Answered

The analysis addressed a range of questions, including:

- Movie and TV show counts by actor, genre, region, and release year  
- Ratings, vote counts, and durations for movies and TV shows  
- Key professions and activities of people in the database  
- Top titles by length, votes, or ratings, including filtering by specific criteria  
- Episode counts and formats for TV series  

Specifically, questions numbered 3, 4, 6, 8, 16, 17, 24–33, and 35–39 were analyzed.

---

## Data Sources and Licensing

This project utilizes multiple datasets from IMDb’s official non-commercial data repository, available at [https://datasets.imdbws.com/](https://datasets.imdbws.com/).  Required datasets:

- `title.basics.tsv.gz`
- `title.akas.tsv.gz`
- `title.crew.tsv.gz`
- `title.principals.tsv.gz`
- `title.ratings.tsv.gz`
- `title.episode.tsv.gz`
- `name.basics.tsv.gz`


Please note the following:

- The IMDb datasets are used strictly for **personal, educational, and non-commercial use** only.
- This repository does **not include the raw IMDb data files**.
- All use of IMDb data complies with IMDb’s [Non-Commercial Licensing Terms](https://www.imdb.com/interfaces/) and copyright policies.

By including this information, the project acknowledges the data source and its licensing requirements, ensuring transparent and responsible use of IMDb data.

---
## Setup Instructions

### 1. Clone the Repository
Run the following commands to clone the repo and move into it:

```bash
git clone https://github.com/tetianasokolova/computer-vision-project.git
cd computer-vision-project
```
### 2. Add IMDb Data

Download the required IMDb datasets (TSV files) from [IMDb Datasets](https://datasets.imdbws.com/) and place them in a folder called `data` at the project root:

```bash
mkdir -p data
# place the .tsv.gz files here
```

### 3. Configure Paths

The default paths assume your datasets are in `data/`. Make sure the results folder exists:

```bash 
mkdir -p data/results
```

### 4. Run with Docker

Build the Docker image:

```bash
docker build -t imdb-pyspark .
```
Run the container:
```bash
docker run imdb-pyspark
```


Results will be saved in `data/results/` according to the paths defined in settings.py.

---

## Authors

- Tetiana Sokolova (tanya.sokolova1406@gmail.com)
- Volodymyr Mishchenia 
- Yuliia Stoliaruk

---

## Acknowledgments

Information courtesy of  
**IMDb**  
(https://www.imdb.com).  
Used with permission.