# Analytics-Vidhya-Job-A-Thon-June-2021
Data Engineering hackathon - Development of input features for a ECom website

Final Rank : 145 of 6467 participants, Accuracy : 88%

Overview of Approach: 
1. Clean the data for missing UserIDs by merging both given data frames. 
2. Convert unix timestamps to human-readable data using pd_to.datetime. 
3. Impute missing values using bfill. 
4. Solve each requirement of input feature table and create dataframe for each 
requirement. 
5. In each dataframe change column names to the required submission data column names. 
6. Merge all the dataframes for the final feature table. 

Detailed Approach-Data Cleaning and Imputation: 

a. Data Import: 
1. Import userTable and VisitorLogsData dataframes 
2. In the problem description, it was given that UserID is available only for registered 
users. So, initial assumption can be that if UserID is missing or null, the user is not 
registered. However there is a situation that the user is registered by the UserID can 
be missing in the dataframe. For imputing these missing UserIDs use the 
webClientID. If all UserIDs of a matching webClientID are missing leave the 
UserID as it is. However if webClientID has a UserID available, impute UserID to 
the matching webClientIDs. 
3. Merging the imputed VisitorLogsData and userTable will clear all the rows with 
missing UserIDs. 

b. VisitDateTime Conversion: 
1. The VisitDateTime conversion is a mix of standard format and unix timestamp. 
2. First a new column is created which gives the respective string length of 
VisitDateTime data. 
3. The standard time format has string length 23 whereas unix timestamps have length 
of 19. 
4. A separate data frame is created which separates standard format data. 
5. On the second data frame pd.to_datetime is used on the cells which have string 
length 19 using if loop. 
6. The two dataframes are merged back. 
7. Create a new column “Date” which gives only the Date part of VisitDateTime. 

c. Null-value imputation: 
1. The data of entire table is converted into similar case format (Upper or Lower). 
2. Take the assumption that similar segment users have similar viewing behaviour. 
3. Sort the data in order User Segment, webClient ID, OS and Browser. 
4. Using bfill i.e backward fill the missing values are imputed. 

Solution for each requirement in Input Feature Table: 

a. No_of_days_Visited_7_Days - How many days a user was active on platform in the 
last 7 days. 
1. The assumed present date is 2018-5-28. 
2. Create a mask with dates 2018-5-21 to 2018-5-27 i.e for last 7 days. 
3. Using ‘loc’ get the data corresponding to last 7 days as a new dataframe. 
4. Use Groupby to get data corresponding to uservisits in last 7 days. 
5. Use pd.series.nunique on ‘Date’ to get the unique visits. 

b. No_Of_Products_Viewed_15_Days - Number of Products viewed by the user in the 
last 15 days 
1. Create a mask with dates 2018-5-12 to 2018-5-27 
2. Using ‘loc’ get the data corresponding to last 15 days as a new dataframe. 
3. Use Groupby to get data corresponding to uservisits in last 15 days. 
4. Use pd.series.nunique of ‘ProductID’ to get the unique products viewed. 
c. User_Vintage - Vintage (In Days) of the user as of today 
1. From signup date extract only the date part using pd.to_datetime and dt.date. 
2. Create a new dataframe with UserID and date part of Signup Date. 
3. Add a new column Today Date with date as 2018-5-28. 
4. Subtract today date with signup date. This gives user vintage in days. 
5. Use pd.to_timedelta and dt.days to get only the numerical value of user vintage. 
6. Since Groupby function isn’t used, use drop_duplicates to remove duplicate values. 

d. Most_Viewed_product_15_Days - Most frequently viewed (page loads) product by the 
user in the last 15 days. 
1. In this requirement, there is a condition that only data with Activity=pageload 
should be considered. 
2. Use the last 15 days which was already created. 
3. Apply condition Activity=pageload to get last 15 days data with only pageloads. 
4. Use combition of Groupby on UserID and pd.Series.max on ProductID to get most 
viewed product for each user. 
5. Data imputation with Product101 where a user has not viewed any product in the 
last 15 days is done in the last step. 

e. Most_Active_OS - Most Frequently used OS by user. 
1. Use the unmodified dataframe as no condition such as last 7 or 15 days is not 
provided. 
2. Use combition of Groupby on UserID and pd.Series.max on OS to get most used 
OS by each user. 

f. Recently_Viewed_Product - Most recently viewed (page loads) product by the user. 
1. In this also there is a condition that Activity is pageload. Since the requirement is 
most recent product use dataframe without any modification. 
2. For most recent value use groupby on UserID and ‘idxmax’ on date, this returns a 
dataframe with latest date for each UserID. 
3. From this dataframe extract only UserID and ProductID which are required. 

g. Pageloads_last_7_days - Count of Page loads in the last 7 days by the user 
1. Use the last 7 days dataframe and extract the data corresponding to Activity 
pageloads. 
2. Use combination of groupby on UserID and pd.Series.count on Activity to get 
pageloads in last 7 days by each user. 

h. Pageloads_last_7_days - Count of Page loads in the last 7 days by the user 
1. Use the last 7 days dataframe and extract the data corresponding to Activity clicks. 
2. Use combination of groupby on UserID and pd.Series.count on Activity to get 
clicks in last 7 days by each user. 

Creating Input Feature Table: 
1. Use userTable data frame and merge with each dataframe on “UserID” using left join. 
2. Merge the dataframes in order of columns in the submission table requirement. 
3. Impute ‘Product101’ in missing Most_Viewed_product_15_Days and Recently_Viewed_Product columns. 
4. Fill the remaining missing values in dataframe with zero. 
5. Sort the data frame on UserID and export it for final submission.
