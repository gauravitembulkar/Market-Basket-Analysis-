###################################################
######## MARKET BASKET ANALYSIS ALGORITHM  ########
############### DATE: 08/03/2019 ##################
###################################################

import pandas as pd

####### Read the file
data = pd.read_csv("orders_small.csv", low_memory=False)

data2 = pd.read_csv("order_products_prior_small.csv")

####### Mearging files
result = pd.merge(data2,data[['order_id','user_id']],on='order_id')

result.to_csv('merged_result.csv')




####### product_count_user.csv contains count of product_id for each user
data3 = pd.read_csv("merged_result.csv", low_memory=False)

temp = data3.groupby(['user_id','product_id']).size().to_frame('count_id')


temp.to_csv("product_count_user.csv")


####### Rename unamed column to count_id
####### order_product_count_user.csv contains order by count of product_id for each user
data4 = pd.read_csv("product_count_user.csv", low_memory=False)

temp2 = data4.sort_values(by=['count_id'],ascending=False)

temp2 = data4.groupby(['user_id'])

temp3 = temp2.apply(lambda x: x.sort_values(by=['count_id'],ascending=False))

temp3.to_csv("order_product_count_user.csv")
#-------------------------------------------------------------------#

####### temp5 contains total number of products bought by each user
temp4 = data3.groupby(['user_id'])

temp5 = temp4.apply(lambda x: x['product_id'].count())

####### Series to DataFrame conversion
temp5 = temp5.to_frame(name="count_product_id")



####### temp6 conatins total number of order by each user

temp6 = temp4.apply(lambda x: x['order_id'].nunique())

temp6 =temp6.to_frame(name="count_order_id")


####### result2 contains merging of temp5 and temp6
result2 = pd.merge(temp5,temp6[['count_order_id']],on='user_id')

result2['predict_product_count'] = result2['count_product_id']/result2['count_order_id']

result2['predict_product_count'] = result2.predict_product_count.astype(int)

####### predicted_product_id.csv contains the final predicted product id for each user with predicted variety of products

data5 = pd.read_csv("order_product_count_user.csv", low_memory=False)

temp7 = pd.merge(data5,result2[['predict_product_count']],on='user_id')

temp8 = temp7.groupby(['user_id'])

temp9 = temp8.apply(lambda x: x['product_id'].head(x['predict_product_count'].iloc[0]))
	
temp9 = temp9.to_frame(name='product_id')

temp9.to_csv("predicted_product_id.csv")

####### Product names are also shown in final_predicted_product_id.csv - final cart ready
data6 = pd.read_csv("products.csv", low_memory=False)

data7 = pd.read_csv("predicted_product_id.csv", low_memory=False)

temp10 = pd.merge(data7,data6[['product_name','product_id','aisle_id','department_id']],on='product_id',how="left")

temp10.to_csv("final_predicted_product_id.csv")

####### count_aisle_id_file contains count_aisle_id is calculated on merged_database
####### aisle_id | count_aisle_id

temp11 = pd.merge(data3[['product_id']],data6[['aisle_id','product_id']],on='product_id')

temp12 = temp11.groupby(['aisle_id'])

temp13 = temp12.apply(lambda x: x['aisle_id'].count())

temp13 = temp13.to_frame(name='count_aisle_id')

temp13.to_csv("count_aisle_id_file.csv")

####### Make new dataframe #DF1#
####### count_product_id_aisle_id conatins product_id and count_product_id across merged result (all user data)
####### product_id | count_product_id | aisle_id

temp14 = temp11.groupby(['product_id'])

temp15 = temp14.apply(lambda x: x['product_id'].count())

temp16 = temp15.to_frame(name='count_product_id')

temp17 = pd.merge(temp16,data6[['aisle_id','product_id']],on='product_id')

temp17.to_csv("count_product_id_aisle_id.csv")

####### Merge final_predicted_product_id.csv and aisle dataframe (count_aisle_id) with respect to aisle_id
####### count_aisle_id_predicted_cart contains final result with count_aisle_id in predicted_cart

data8 = pd.read_csv("final_predicted_product_id.csv", low_memory=False)
data8 = data8.drop(data8.columns[data8.columns.str.contains('unnamed',case = False)],axis = 1)

data9 = pd.read_csv("count_aisle_id_file.csv", low_memory=False)

temp18 = pd.merge(data8,data9,on='aisle_id')

temp18 = temp18.sort_values('user_id',ascending = True)
temp18.to_csv("count_aisle_id_predicted_cart.csv")

####### Get maximum top 4 aisle_id for each user (sort by count_aisle_id against each user)
####### top_aisle_id contains aisle_id top4 unique
data10 = pd.read_csv("count_aisle_id_predicted_cart.csv")

data10 = data10.drop(data10.columns[data10.columns.str.contains('unnamed',case = False)],axis = 1)

temp19 = data10.groupby(["user_id"])

temp20 = temp19.apply(lambda x: x.drop_duplicates(subset=['count_aisle_id']).sort_values(by=['count_aisle_id'], ascending=False).head(4))

temp20.to_csv("top_aisle_id.csv")

####### Get 2x product_id whose count_product_id is max for given aisle_id

data11 = pd.read_csv("count_product_id_aisle_id.csv")

data12 = pd.read_csv("top_aisle_id.csv")

temp21 = data12.groupby(["aisle_id"])

temp22 = temp21.apply(lambda x: data11[data11['aisle_id'].isin(x['aisle_id'])].sort_values(by=['count_product_id'], ascending=False).head(2))

temp22.to_csv("possible_suggested_cart.csv")

####### Make master sheet with final top_aisle_id.csv with suggested product ids (for each aisle Ids there will be 2 product ids)

####### suggested_cart contains 8 suggested product ids which might be in predicted cart 
data13 = pd.read_csv("possible_suggested_cart.csv")

temp23 = data12.merge(data13, left_on ='aisle_id', right_on='aisle_id')

temp23 = temp23.drop(temp23.columns[temp23.columns.str.contains('unnamed',case = False)],axis = 1)

temp23 = temp23.sort_values('user_id',ascending = True)

temp23.to_csv("suggested_cart.csv")


####### Remove the product_id which is already present in the cart
####### suggested_cart_product_id contains suggested product ids which are not common with predicted cart

data14 = pd.read_csv("suggested_cart.csv")

temp24 = data10.groupby(["user_id"])

temp25 = temp24.apply(lambda x: data14[ (data14['user_id'] == x.name) & ~(data14['product_id_y'].isin(x['product_id']))])

temp25.to_csv("suggested_cart_product_id.csv")

####### Show your suggested items list (merge product_name)
####### data6 contains products.csv

data15 = pd.read_csv("suggested_cart_product_id.csv")

temp26 = pd.merge(data15[['user_id','product_id_y']],data6[['product_name','product_id']],left_on='product_id_y', right_on='product_id')

temp26 = temp26.sort_values('user_id',ascending = True)

temp26.to_csv("final_suggested_cart.csv")


##################### NEW USER ################

## Merge against order_id - > Order_small and orders_products_prior_small

temp27 = pd.merge(data2[['order_id','product_id']],data[['order_id','order_dow','order_hour_of_day']],left_on='order_id', right_on='order_id')
## Group by dow and hod 


temp28 = temp27.groupby(["order_dow","order_hour_of_day","product_id"]).size().to_frame('count_dow_product_id')
temp28 = temp28.reset_index()
temp29 = temp28.groupby(["order_dow","order_hour_of_day"])

## pick top 8 product_id (max) from the last list

temp30 = temp29.apply(lambda x: x.sort_values(by=['count_dow_product_id'], ascending=False).head(8))

## merge the product table with last result

temp31 = pd.merge(temp30[['order_dow','order_hour_of_day','product_id','count_dow_product_id']],data6[['product_id','product_name']],left_on='product_id', right_on='product_id')

temp31.to_csv("final_suggsted_cart_new_user.csv")











