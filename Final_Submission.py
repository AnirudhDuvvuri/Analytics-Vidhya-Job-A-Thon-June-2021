#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import standard modules
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")


# In[2]:


#Upload the visitor logs data as Dataframe df
df=pd.read_csv("VisitorLogsData.csv")


# In[3]:


#check the structure of dataframe
df.describe()

In the problem description, it was given that UserID is available only for registered users. So, initial assumption can be that if UserID is missing or null, the user is not registered. However there is a situation that the user is registered by the UserID can be missing in the dataframe. For imputing these missing UserIDs we are going to use the webClientID. If all UserIDs of a matching webClientID are missing we are going to leave the UserID as it is. However if webClientID has a UserID available, we are going to impute UserID to the matching webClientIDs.
# In[4]:


#impute missing UserID if trailing matching webClientID has a UserID
for i in range(0,6587999):
    if df['webClientID'][i+1]==df['webClientID'][i]:
        df['UserID'][i+1]=df['UserID'].fillna(df['UserID'][i]) 


# In[5]:


#impute missing UserID if trailing matching webClientID has a UserID
for i in range(1,6588000):
    if df['webClientID'][i]==df['webClientID'][i-1]:
        df['UserID'][i]=df['UserID'].fillna(df['UserID'][i-1]) 


# In[6]:


#check number of missing values
df.isnull().sum()

Next create a copy of the data as imputing the missing UserIDs again can take a lot of time.
# In[7]:


df.to_csv("UserID_Cleaned.csv")


# In[8]:


#import the dataset as UserID_Cleaned
UserID_Cleaned=pd.read_csv("UserID_Cleaned.csv")


# In[9]:


#import usertable as userTable
user=pd.read_csv("userTable.csv")


# In[10]:


#merge both the tables to eliminate all the missing UserID data and get new columns in the dataset
df=pd.merge(user,UserID_Cleaned,on='UserID')


# In[11]:


#check number of missing values in each column
df.isnull().sum()


# In[12]:


#drop Unnamed:0 which is not useful
df=df.drop(['Unnamed: 0'],axis=1)

Now, convert all VisitDateTime data into human readable format. For this use the logic that unixtimestamps in the column are 19 strings length and rest are 23.
# In[13]:


#convert the data into string to compare data with string lengths
df["VisitDateTime"]= df["VisitDateTime"].astype(str)


# In[14]:


#create a new column with string lengths of VisitDateTime data
df["VisitDateTime Length"]= df["VisitDateTime"].str.len()


# In[15]:


#check counts to split the dataframes
df["VisitDateTime Length"].value_counts()


# In[16]:


#sort values by length for creating a new dataframe
df=df.sort_values(by="VisitDateTime Length",ascending=False)


# In[17]:


#cross-check sorting
df.head()


# In[18]:


#split dataframes based on string lengths to get human readable data, unix and nan into seperate dataframes
df_1 = df.iloc[:527038,:]
df_2 = df.iloc[527038:,:]


# In[19]:


#reset index of df_2
df_2=df_2.reset_index()

Convert the data in unix using loops in below steps:
1.Check if the length of string is 19 characters 
2.Extract first 13 characters of the string
3.Apply pd.to_datetime with unit 'ms' 
4.If string is not 19 characters return data without any changes
# In[20]:


#Convert unix data to human readable data
for i in range(len(df_2)):
    if len(str(df_2['VisitDateTime'][i]))==19:
        df_2['VisitDateTime'][i]=str(df_2['VisitDateTime'][i])[0:13]
        df_2['VisitDateTime'][i]=pd.to_datetime((df_2['VisitDateTime'][i]),unit='ms')
        print(df_2['VisitDateTime'][i])
    else:
        df_2['VisitDateTime'][i]=df_2['VisitDateTime'][i]
        print(df_2['VisitDateTime'][i])


# In[21]:


df_2["VisitDateTime"]= df_2["VisitDateTime"].astype(object)


# In[22]:


#merge the dataframes back using concat
df=pd.concat([df_1,df_2])


# In[23]:


#reset the index of dataframe
df=df.reset_index()


# In[24]:


#check the dataframe columns
df.columns


# In[25]:


#drop all the unwanted columns from dataframe
df=df.drop(['level_0','index','VisitDateTime Length'], axis = 1)


# In[26]:


#Create a copy of dataframe
df.to_csv("UserandTimeData.csv")


# In[27]:


#Add a date column into the dataframe
df.insert(5, "Date",True)


# In[28]:


#extract only date component from VisitDateTime into Date Column 
df['Date']=pd.to_datetime(df['VisitDateTime']).dt.date


# In[29]:


#keep only datepart
df['Date'] = pd.to_datetime(df['Date']).dt.floor('d')

Now, change the data into a uniform format. The letter cases are not uniform in columns. Check the dataset and change the letter case as required. 
# In[30]:


#convert first letter of ProductID to uppercase
df['ProductID'] = df['ProductID'].str.capitalize()


# In[31]:


#check Activity valuecounts
df['Activity'].value_counts()


# In[32]:


#Convert total data to small case
df['Activity'].replace({"CLICK":"click","PAGELOAD":"pageload"},inplace=True)


# In[33]:


#check OS valuecounts
df['OS'].value_counts()


# In[34]:


#convert all data to matching case
df["OS"].replace({"windows": "Windows", "android": "Android","ios":"iOS","mac os x":"Mac OS X","linux":"Linux","ubuntu":"Ubuntu","chrome os":"Chrome OS","fedora":"Fedora","tizen":"Tizen"}, inplace=True)

Next step is to impute the remaining missing data. The assumptions are:
1. Users of same User Segment have similar usage pattern.
2. Matching webClientID is from a same user, so missing data could be similar to available data
3. OS and Browser have no missing values, so these can also be useful for categorizing data
# In[35]:


#sort the data with above mentioned parameters
df=df.sort_values(by=["User Segment","webClientID","OS","Browser"])


# In[36]:


#cross-check sorting
df.head()


# In[37]:


#Impute missing values using bfill
df=df.fillna(method='bfill')

Feature Model Creation Part:
1st question asks the days a user was active on platform for last 7days.
For this we will first create dataframe with last 7 days data
# In[38]:


#Create dataframe with last 7 days data
start_date = '2018-05-21'
end_date = '2018-05-27'
mask = (df['Date'] >=start_date) & (df['Date'] <= end_date)
  
df_Last_7days = df.loc[mask]


# In[39]:


#Group by UserID and use pd.Series.nunique on Date
No_of_days_Visited_7_Days = df_Last_7days.groupby(by='UserID', as_index=False).agg({'Date': pd.Series.nunique})


# In[40]:


#Rename columns
No_of_days_Visited_7_Days.columns =['UserID', 'No_of_days_Visited_7_Days']


# In[41]:


#Check the solution format
print(No_of_days_Visited_7_Days)

2nd question asks the products viewed for last 15 days.
For this we will create dataframe with last 15 days data
# In[42]:


#Create dataframe with last 15 days data
start_date = '2018-05-12'
end_date = '2018-05-27'
mask2 = (df['Date'] >=start_date) & (df['Date'] <= end_date)
  
df_Last_15days = df.loc[mask2]


# In[43]:


#Use Groupby on UserID and pd.Series.nunique on ProductID
No_Of_Products_Viewed_15_Days = df_Last_15days.groupby(by='UserID', as_index=False).agg({'ProductID': pd.Series.nunique})


# In[44]:


#Rename Columns
No_Of_Products_Viewed_15_Days.columns =['UserID', 'No_Of_Products_Viewed_15_Days']


# In[45]:


#Check solution format
print(No_Of_Products_Viewed_15_Days)


# In[46]:


#Extract only Date from Signup Date into samecolumn
df['SignUpDate']=pd.to_datetime(df['Signup Date']).dt.date


# In[47]:


#Create new dataframe with SignupDate
SignUpDate = [df['UserID'],df['SignUpDate']]


# In[48]:


#Name Columns
headers = ["UserID", "SignUpDate"]


# In[49]:


#Create data frame SignUpDate
SignUpDate = pd.concat(SignUpDate, axis=1, keys=headers)


# In[50]:


#Add new column TodayDate
SignUpDate.insert(2, "TodayDate",True)


# In[51]:


#Add 2018-5-28 as TodayDate
import datetime
SignUpDate['TodayDate']=datetime.date(2018,5,28)


# In[52]:


#Add User_Vintage Column
SignUpDate.insert(3, "User_Vintage",True)


# In[53]:


#Subtract SignupDate from TodayDate to get User_Vintage
SignUpDate['User_Vintage']=SignUpDate['TodayDate']-SignUpDate['SignUpDate']


# In[54]:


#Extract only number from SignUpDate
SignUpDate['User_Vintage']=pd.to_timedelta(SignUpDate['User_Vintage']).dt.days


# In[55]:


#Drop unwanted columns
User_Vintage = SignUpDate.drop(['SignUpDate','TodayDate'], axis = 1)


# In[56]:


#Drop duplicate rows
User_Vintage=User_Vintage.drop_duplicates()


# In[57]:


#Check solution format
print(User_Vintage)


# In[58]:


#Rename Columns
User_Vintage.columns =['UserID', 'User_Vintage']


# In[59]:


#Use last 15 days dataframe to get only pageloads in last 15 days
df_pageload_15days=df_Last_15days[df_Last_15days['Activity']=='pageload']


# In[60]:


df['ProductID'].value_counts()


# In[61]:


#Use groupby on UserID and pd.Series.max on ProductID to get most viewed product
Most_Viewed_product_15_Days=df_pageload_15days.groupby(by='UserID', as_index=False).agg({'ProductID': pd.Series.max})


# In[62]:


#check solution format
print(Most_Viewed_product_15_Days)


# In[63]:


#Rename columns
Most_Viewed_product_15_Days.columns =['UserID', 'Most_Viewed_product_15_Days']


# In[64]:


#Use Groupby on UserID and pd.Series.max on original merged dataframe
Most_Active_OS= df.groupby(by='UserID', as_index=False).agg({'OS': pd.Series.max})


# In[65]:


#Rename columns
Most_Active_OS.columns =['UserID', 'Most_Active_OS']


# In[66]:


#check solution format
print(Most_Active_OS)


# In[67]:


#Use merged dataframe and get only data corresponding to pageloads
df_pageload=df[df['Activity']=='pageload']


# In[68]:


#Use groupby on UserID and idxmax on Date
Recently_Viewed_Product=df_pageload.loc[df_pageload.groupby('UserID').Date.idxmax()]


# In[69]:


#Extract UserID and ProductID columns
Recently_Viewed_Product=[Recently_Viewed_Product['UserID'],Recently_Viewed_Product['ProductID']]


# In[70]:


#Add columns
headers = ["UserID", "ProductID"]


# In[71]:


#Merge column names and data
Recently_Viewed_Product = pd.concat(Recently_Viewed_Product, axis=1, keys=headers)


# In[72]:


#Check solution format
print(Recently_Viewed_Product)


# In[73]:


#Rename columns
Recently_Viewed_Product.columns =['UserID', 'Recently_Viewed_Product']


# In[74]:


#Use last 7 days dataframe and extract data corresponding to pageloads
PageLoads_7Days=df_Last_7days[df_Last_7days['Activity']=='pageload']


# In[75]:


#Use groupby on UserID and pd.Series.count on Activity
Pageloads_last_7_days=PageLoads_7Days.groupby(by='UserID', as_index=False).agg({'Activity': pd.Series.count})


# In[76]:


#check solution format
print(Pageloads_last_7_days)


# In[77]:


#Rename columns
Pageloads_last_7_days.columns =['UserID', 'Pageloads_last_7_days']


# In[78]:


#Use last 7 days dataframe and extract data corresponding to clicks
Click_7Days=df_Last_7days[df_Last_7days['Activity']=='click']


# In[79]:


#Use groupby on UserID and pd.Series.count on Activity
Clicks_last_7_days=Click_7Days.groupby(by='UserID', as_index=False).agg({'Activity': pd.Series.count})


# In[80]:


#check solution format
print(Clicks_last_7_days)


# In[81]:


#Rename columns
Clicks_last_7_days.columns =['UserID', 'Clicks_last_7_days']


# In[82]:


#Read userTable dataframe as df2
df2=pd.read_csv("userTable.csv")

For final solution, merge all the solution dataframes in order as per sample solution using 'left' join on 'UserID'
# In[83]:


#left join df2 and No_of_days_Visited_7_Days 
Submission=pd.merge(df2,No_of_days_Visited_7_Days, on='UserID',how='left')
Submission.sort_values(by='UserID')


# In[84]:


#left join Submission and No_Of_Products_Viewed_15_Days
Submission=pd.merge(Submission,No_Of_Products_Viewed_15_Days, on='UserID',how='left')
Submission.sort_values(by='UserID')


# In[85]:


#left join Submission and User_Vintage
Submission=pd.merge(Submission,User_Vintage, on='UserID',how='left')
Submission.sort_values(by='UserID')


# In[86]:


#left join Submission and Most_Viewed_product_15_Days
Submission=pd.merge(Submission,Most_Viewed_product_15_Days, on='UserID',how='left')
Submission.sort_values(by='UserID')


# In[87]:


#left join Submission and Most_Active_OS
Submission=pd.merge(Submission,Most_Active_OS, on='UserID',how='left')
Submission.sort_values(by='UserID')


# In[88]:


#left join Submission and Recently_Viewed_Product
Submission=pd.merge(Submission,Recently_Viewed_Product, on='UserID',how='left')
Submission.sort_values(by='UserID')


# In[89]:


#left join Submission and Pageloads_last_7_days
Submission=pd.merge(Submission,Pageloads_last_7_days, on='UserID',how='left')
Submission.sort_values(by='UserID')


# In[90]:


#left join Submission and Clicks_last_7_days
Submission=pd.merge(Submission,Clicks_last_7_days, on='UserID',how='left')
Submission.sort_values(by='UserID')


# In[91]:


#Replace missing values in Most_Viewed_product_15_Days and Recently_Viewed_Product with 'Product101'
Submission["Most_Viewed_product_15_Days"].fillna('Product101',inplace=True)
Submission["Recently_Viewed_Product"].fillna('Product101',inplace=True)


# In[92]:


#Impute missing values in dataframe with 0
Submission.fillna('0',inplace=True)


# In[93]:


#Drop unwanted columns
Submission=Submission.drop(['Signup Date','User Segment'], axis = 1)


# In[94]:


#Finally sort the values by UserID
Submission=Submission.sort_values(by="UserID")


# In[95]:


#Export the dataframe as FinalSolution.csv
Submission.to_csv('FinalSubmission.csv', index=False)

