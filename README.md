# Project-1

import numpy as np
import pandas as pd
import matplotlib.pyplot as pyplot
%matplotlib inline

df=pd.read_csv('https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv')
df1=pd.read_csv('https://raw.githubusercontent.com/kjam/data-wrangling-pycon/master/data/berlin_weather_oldest.csv')
df.head(2)

df1.head(2)

# 1. Get the Metadata from the above files.
print("Metadata for df")
print("-------------------")
df.info()

print("Metadata for df1")
print("-------------------")
df1.info()

# 2. Get the row names from the above files.
print("All the rows for df are :")
df.index.values

print("All the rows for df1 are :")
df1.index.values

# 3. Change the column name from any of the above file.
df.rename(columns={"Indicator":"Indicator_1"}).head(2)

# 4. Change the column name from any of the above file and store the changes made permanently.
df.rename(columns={"Indicator":"Indicator_1"},inplace=True)
df.head(2)

# 5. Change the names of multiple columns. (Is this going to be a perm change? any other method for changing column names?)
df.rename(columns={"PUBLISH STATES":"Publication status","WHO region":"WHO Region"},inplace=True)
df.head(2)

# 6. Arrange values of a particular column in ascending order.
df.sort_values('Year').head(5)

# 7. Arrange multiple column values in ascending order.
df[0:3].sort_values(['Indicator_1','Country','Year','WHO Region','Publication status'])
df[['Indicator_1','Country','Year','WHO Region','Publication status']].head(3)

# 8. Make country as the first column of the dataframe.
df[['Country','Indicator_1', 'Publication status', 'Year', 'WHO Region',
       'World Bank income group', 'Sex','Display Value', 'Numeric',
       'Low', 'High', 'Comments']].head()
       
# 9. Get the column array using a variable
np.array(df[['WHO Region']])      

# 10 Get the subset rows 11, 24, 37 Expected Output:
df[11:38:13]

# 11.Get the subset rows excluding 5, 12, 23, and 56
df.drop([5,12,23,56])[0:54]

users=pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')
sessions=pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv')
products=pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv')
transactions=pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')
users.head()

sessions.head()

products.head()

transactions.head()

# 12. Join users to transactions, keeping all rows from transactions and only matching rows from users (left join)
pd.merge(transactions,users, how="left")

# 13. Which transactions have a UserID not in users?

transactions[~transactions['UserID'].isin(users['UserID'])]


# 14. Join users to transactions, keeping only rows from transactions and users that match via UserID (inner join)
pd.merge(transactions,users,how="inner",on="UserID")

# 15. Join users to transactions, displaying all matching rows AND all non-matching rows (full outer join)
pd.merge(transactions,users,how='outer')

# 16. Determine which sessions occurred on the same day each user registered

pd.merge(left=users,right=sessions,how="inner",left_on=['UserID','Registered'],right_on=['UserID','SessionDate'])

# 17. Build a dataset with every possible (UserID, ProductID) pair (cross join)
df_userid = pd.DataFrame({"UserID":users["UserID"]})
df_Tran = pd.DataFrame({"ProductID":products["ProductID"]})
#create new column Key with value as 1 for both the dataframe as this would become the common key to be merged
df_userid['Key'] = 1
df_Tran['Key'] = 1
dataset=pd.merge(df_userid,df_Tran,how='outer',on="Key")[['UserID','ProductID']]
dataset

# 18. Determine how much quantity of each product was purchased by each user

#do a left join on the output table df_out from previous step with transactions table on the keys ['UserID','ProductID]
df_user_prod_quant = pd.merge(dataset,transactions,how='left',on=['UserID','ProductID'])

#Groupby the table on ['UserID','ProductID] and calculate the sum of Qunatity entity for each group
df_user_quantity = df_user_prod_quant.groupby(['UserID','ProductID'])['Quantity'].sum()

#reset index so that the index column will have consecutive default numbers and fill NAN values with 0
df_user_quantity.reset_index().fillna(0)

# 19. For each user, get each possible pair of pair transactions (TransactionID1,TransacationID2)
pd.merge(transactions,transactions,how='outer',on='UserID')

# 20. Join each user to his/her first occuring transaction in the transactions table

#do an left outer join on user and transactions dataframe on the UserID column
df_usertran = pd.merge(users,transactions,how='left',on='UserID')
# craete a new dataframe df_ with all duplicates on UserID being dropped , only keeping the first entry
df_ = df_usertran.drop_duplicates(subset='UserID')
#reset the index to the default integer index.
df_ = df_.reset_index(drop=True)

#display the contents of the dataframe df_
df_

# 21. Test to see if we can drop columns
#Retieve the column list for the dataframe df_ created in problem statement 20
my_columns = list(df_.columns)
print(my_columns)

list(df_.dropna(thresh=int(df_.shape[0] * .9), axis=1).columns) #set threshold to drop NAs

missing_info = list(df_.columns[df_.isnull().any()])
missing_info

for col in missing_info:
    num_missing = df_[df_[col].isnull() == True].shape[0]
    print('number missing for column {}: {}'.format(col, num_missing))
    
#count of missing df_
for col in missing_info:
    percent_missing = df_[df_[col].isnull() == True].shape[0] / df_.shape[0]
    print('percent missing for column {}: {}'.format(col, percent_missing))
