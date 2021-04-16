install.packages("RPostgreSQL")
library('RPostgreSQL')

## Loading required package: DBI
pg = dbDriver("PostgreSQL")
# Local Postgres.app database; no password by default
# Of course, you fill in your own database information here.
con = dbConnect(pg, user="airflow", password="airflow",
                host="localhost", port=5432, dbname="airflow")

dtab = dbGetQuery(con, "select * from listings")




library(dplyr)
library('stringr')
library(tidyr)
dtab <- dtab %>% drop_na()

dtab$price <- str_replace_all(dtab$price, '$', '')
# price issue here, but actually not important because it is already anaylsed
dtab$price <- chartr("$", "", dtab$price)
dtab$price <- gsub("$", "", dtab$price)
dtab$price <- gsub(",", "", dtab$price)
dtab$price

# filter out non numeric variables
dtab_numeric <- dtab %>%
  select(accommodates, bathrooms, bedrooms, beds, square_feet, minimum_nights, maximum_nights, number_of_reviews, review_scores_rating)

# analyse how all the variables influence 'number of reviews'
summary(lm(formula = dtab_numeric$number_of_reviews ~ ., data=dtab_numeric))
# analyse how all the variables influence 'review scores rating'
summary(lm(formula = dtab_numeric$review_scores_rating ~ ., data=dtab_numeric))

# visualize correlation plot
head(dtab_numeric)
M<-cor(dtab_numeric)
head(round(M,2))

library(corrplot)
corrplot(M)
