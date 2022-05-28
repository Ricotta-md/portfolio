import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist


class Order:
    '''
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    '''
    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """
        # Hint: Within this instance method, you have access to the instance of the class Order in the variable self, as well as all its attributes
        # YOUR CODE HERE
        ## only look at delivered orders
        tmp_order_df=self.data['orders']
        tmp_order_df=tmp_order_df[tmp_order_df['order_status']=='delivered']
        #tmp_order_df.shape

        #order_df.info()
        tmp_order_df.columns
        # Convert all times
        tmp_order_df[['order_approved_at','order_purchase_timestamp','order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date']]\
        =tmp_order_df[['order_approved_at','order_purchase_timestamp','order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date']].apply(lambda x : pd.to_datetime(x,errors='raise'))

        ########################################
        # Create a column to compute wait-time
        # !!!!!!!! The problem is HERE!!!!! We are supposed to subtrace the purchase time! NOT the order_approved_at time!!!
        #tmp_order_df['wait_time']=tmp_order_df['order_delivered_customer_date']-tmp_order_df['order_approved_at']




        tmp_order_df['wait_time']=tmp_order_df['order_delivered_customer_date']-tmp_order_df['order_purchase_timestamp']
        #tmp_order_df['wait_time'].dt.days
        ## don't use the above, because it will only gives you the days, not accurate, e.g we might want days like 15.99
        ## but dt.days would only gives you 15 days, use timedelta
        tmp_order_df['wait_time']=tmp_order_df['wait_time']/np.timedelta64(24,'h')
        #print(tmp_order_df['wait_time'])

        # Create a column to compute expected_wait_time
        # Create a column to compute expected_wait_time

        tmp_order_df['expected_wait_time']=tmp_order_df['order_estimated_delivery_date']-tmp_order_df['order_purchase_timestamp']

        tmp_order_df['expected_wait_time']=tmp_order_df['expected_wait_time']/np.timedelta64(24,'h')



        ####################################
        # The other problem is HERE !!!!! Since order_delivered_customer_date has missing data, so we need to return 0 for missing data
        tmp_order_df['delay_vs_expected']=(tmp_order_df['order_estimated_delivery_date']-tmp_order_df['order_delivered_customer_date'])/np.timedelta64(24,'h')
        tmp_order_df['delay_vs_expected']=tmp_order_df['delay_vs_expected'].apply(lambda x : x if x>0 else 0)
        #tmp_order_df.info()
        # Select the columns you want
        orders=tmp_order_df[['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected', 'order_status']]
        return orders

        #solution of get_wait_time
    def get_wait_time_soln(self, is_delivered=True):
        orders = self.data['orders'].copy() # make sure we don't create a "view" but a copy


        # filter delivered orders
        if is_delivered:
            orders = orders.query("order_status=='delivered'").copy()

        # handle datetime
        orders.loc[:, 'order_delivered_customer_date'] = \
            pd.to_datetime(orders['order_delivered_customer_date'])
        orders.loc[:, 'order_estimated_delivery_date'] = \
            pd.to_datetime(orders['order_estimated_delivery_date'])
        orders.loc[:, 'order_purchase_timestamp'] = \
            pd.to_datetime(orders['order_purchase_timestamp'])

        # compute delay vs expected
        orders.loc[:, 'delay_vs_expected'] = \
            (orders['order_estimated_delivery_date'] -
             orders['order_delivered_customer_date']) / np.timedelta64(24, 'h')

        def handle_delay(x):
            # We only want to keep delay where wait_time is longer than expected (not the other way around)
            # This is what drives customer dissatisfaction!
            if x < 0:
                return abs(x)
            else:
                return 0

        orders.loc[:, 'delay_vs_expected'] = \
            orders['delay_vs_expected'].apply(handle_delay)

        # compute wait time
        orders.loc[:, 'wait_time'] = \
            (orders['order_delivered_customer_date'] -
             orders['order_purchase_timestamp']) / np.timedelta64(24, 'h')

        # compute expected wait time
        orders.loc[:, 'expected_wait_time'] = \
            (orders['order_estimated_delivery_date'] -
             orders['order_purchase_timestamp']) / np.timedelta64(24, 'h')

        return orders[['order_id', 'wait_time', 'expected_wait_time',
                       'delay_vs_expected', 'order_status']]












    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        # YOUR CODE HERE
        reviews=self.data['order_reviews']
        reviews['dim_is_five_star']=reviews['review_score'].map({5:1}).map({1:1,np.nan:0})

        # Same to dim_is_one_star
        reviews['dim_is_one_star']=reviews['review_score'].map({1:1}).map({1:1,np.nan:0})

        # Extrate columns in need: order_id, dim_is_five_star, dim_is_one_star, review_score
        reviews_needed=reviews[['order_id', 'dim_is_five_star','dim_is_one_star', 'review_score']]
        return reviews_needed

    def get_number_products(self):
        """
        Returns a DataFrame with:
        order_id, number_of_products
        """
        # YOUR CODE HERE
        # YOUR CODE HERE
        # get data from 'order_items' dataset
        order_prod=self.data['order_items']

        # group by order and count the id
        grouped_order_prod=order_prod.groupby('order_id')[['product_id']].count()

        grouped_order_prod.rename(columns={'product_id':'number_of_products'},inplace=True)
        # Reset index to so the
        grouped_order_prod.reset_index(inplace=True)

        return grouped_order_prod

    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        # YOUR CODE HERE
        # get data from 'order_items' dataset
        tmp_order_seller=self.data['order_items']

        # groupby order_id and get the number of unique sellers
        order_seller=tmp_order_seller.groupby('order_id')[['seller_id']].nunique()
        order_seller.rename(columns={'seller_id':'number_of_sellers'},inplace=True)
        order_seller.reset_index(inplace=True)
        return order_seller

    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        # YOUR CODE HERE
        # get data from 'order_items' data frame
        order_item=self.data['order_items'][['order_id','price','freight_value']]

        # grouby by order_id
        grouped_order_item=order_item.groupby('order_id').sum()
        grouped_order_item.reset_index(inplace=True)
        return grouped_order_item

    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        pass  # YOUR CODE HERE

    def get_training_data(self,
                          is_delivered=True,
                          with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_products', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        # Hint: make sure to re-use your instance methods defined above

        feature_df=self.get_wait_time()
        print(feature_df.shape)
        feature_df=feature_df.merge(self.get_review_score(),on='order_id',how='left') #some might even have reviews
        #feature_df.columns
        feature_df=feature_df.merge(self.get_number_products(),on='order_id',how='left')
        print(feature_df.shape)
        feature_df=feature_df.merge(self.get_number_sellers(),on='order_id',how='left')
        print(feature_df.shape)
        feature_df=feature_df.merge(self.get_price_and_freight(),on='order_id',how='left')
        print(feature_df.shape)
        #feature_df.drop_duplicates()
        feature_df.dropna(inplace=True)
        return feature_df
