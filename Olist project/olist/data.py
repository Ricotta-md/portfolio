import os
import pandas as pd


class Olist:
    def get_data(self):
        """
        This function returns a Python dict.
        Its keys should be 'sellers', 'orders', 'order_items' etc...
        Its values should be pandas.DataFrames loaded from csv files
        """
        # Hints 1: Build csv_path as "absolute path" in order to call this method from anywhere.
            # Do not hardcode your path as it only works on your machine ('Users/username/code...')
            # Use __file__ instead as an absolute path anchor independant of your usename
            # Make extensive use of `breakpoint()` to investigate what `__file__` variable is really
        # Hint 2: Use os.path library to construct path independent of Mac vs. Unix vs. Windows specificities
        # YOUR CODE HERE
        csv_path=os.path.join('/Users/yayahuo/code/Ricotta-md/data-challenges/04-Decision-Science','data/csv')
        file_names=[]
        for file in os.listdir(csv_path):
            file_names.append(file)

        key_names=[]
        for name in file_names:
            new_name=name.replace('olist_','').replace('_dataset.csv','').replace('.csv','')

            key_names.append(new_name)

        data={}
        for key, file in zip(key_names,file_names):
            df=pd.read_csv(os.path.join(csv_path, file))
            data[key]=df
        return data



    def ping(self):
        """
        You call ping I print pong.
        """
        print("pong")
