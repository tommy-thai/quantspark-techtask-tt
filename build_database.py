# %%
# Use this script to write the python needed to complete this task
import sqlite3
import requests
import pandas as pd
import os
from string import ascii_lowercase

# %%

# Define the directory containing the data files
data_dir = 'data'
sqlite_db = 'bar_database.sqlite'

# Define the file names here along with the bar name and whether the file is headerless for each transactions file.
bar_file = 'bar_data.csv'
transaction_files = [
    {
        'name':'budapest',
        'file':'budapest.csv.gz',
        'headerless':False,
    }, 
    {
        'name':'london',
        'file':'london_transactions.csv.gz',
        'headerless':True,
    }, 
    {
        'name':'new york',
        'file':'ny.csv.gz',
        'headerless':False,
    },
]

def main():
    bar_data = get_bar_data()
    transaction_data = get_transaction_data()
    drink_data = get_drink_data(transaction_data) # the 'get_drink_data' function uses the transaction data to identify which drinks are needed to be pulled from the API
    load_data(bar_data,drink_data,transaction_data)
    poc_create()

def get_bar_data():
    # Reads in data from files stored in directory variable 'data_dir'. Starts with Bar data.
    bar_data_file = os.path.join(data_dir, bar_file)
    bar_data = pd.read_csv(bar_data_file)
    bar_data['stock'] = bar_data['stock'].str.extract('(\d+)') # Stock may be an open-ended entry, extracting the number where present
    bar_data['stock'] = pd.to_numeric(bar_data['stock'])
    return bar_data

def get_transaction_data():
    # Create a dictionary to store transactional data for each bar
    transaction_headers = ['timestamp','drink','amount','bar']
    transaction_data = pd.DataFrame(columns=transaction_headers)

    # Read transactional data files for each bar, uses 'transaction_files' as configuration
    for bar in transaction_files:
        bar_name = bar['name']
        file = bar['file']
        headerless = bar['headerless']

        file_path = os.path.join(data_dir, file)
        df_transaction = pd.read_csv(
            file_path, 
            sep=None, 
            engine='python', 
            usecols=[1,2,3],
            parse_dates=[1],
            header= None if headerless else 0,
            names= transaction_headers[:-1],
        )
        df_transaction['bar'] = bar_name

        transaction_data = pd.concat([transaction_data,df_transaction])
    
    return transaction_data

def get_drink_data(transaction_data):

    # %%
    letter = 'a'
    # Import data from cocktails database API, unfortunately I couldn't find a method to list the full catalogue of drinks.
    # The API can list drinks using the drink's starting letter. This loops through the alphabet to achieve this.
    drink_headers = ['drink_id','drink_name','glass_type']
    drink_rows = []

    for letter in ascii_lowercase:
        api_url = f'https://www.thecocktaildb.com/api/json/v1/1/search.php?f={letter}'
        api_response = requests.get(api_url)
        drinks_json = api_response.json()['drinks']
        if drinks_json is None: continue
        for drink in drinks_json:
            drink_rows.append((drink['idDrink'],drink['strDrink'],drink['strGlass']))

    drink_data = pd.DataFrame.from_records(columns=drink_headers,data=drink_rows)

    # %%
    # Somehow listing by letter doesn't return all drinks, possible limitation of API? Instead I make individual API calls
    # to specific drinks to fill in the rest. This methodology minimises the number of API calls required. If this isn't
    # an issue, this can replace the above API call. Either way, this function needs to be refactored.
    drinks_all = transaction_data.merge(drink_data.drop_duplicates(), right_on='drink_name',left_on='drink', 
        how='left', indicator=True)
    drinks_missing = drinks_all[drinks_all._merge=='left_only']

    for drink in drinks_missing['drink'].unique():#transaction_data['drink'].unique():
        api_url = f'https://www.thecocktaildb.com/api/json/v1/1/search.php?s={drink}'
        api_response = requests.get(api_url)
        drinks_json = api_response.json()['drinks']
        if drinks_json is None: continue
        for drink in drinks_json:
            drink_rows.append((drink['idDrink'],drink['strDrink'],drink['strGlass']))

    drink_data = pd.DataFrame.from_records(columns=drink_headers,data=drink_rows)

    return drink_data

    # %%

def load_data(bar_data,drink_data,transaction_data):
    # Create the database and tables
    with sqlite3.connect(sqlite_db) as conn:
        cursor = conn.cursor()
        with open('data_tables.sql') as sql_file:
            create_table_queries = sql_file.read()
            cursor.executescript(create_table_queries)

        conn.commit()

        # Import data to the database
        # Populate Bars table
        cursor.executemany("INSERT INTO Bars (glass_type, stock, bar) VALUES (?, ?, ?)", [tuple(r) for r in bar_data.to_numpy()])
        conn.commit()

        # Populate Drinks table, try catch needed as we only want to keep unique drink_ids here
        try:
            cursor.executemany("INSERT INTO Drinks (drink_id, drink_name, glass_type) VALUES (?, ?, ?)", [tuple(r) for r in drink_data.to_numpy()])
            conn.commit()
        except sqlite3.IntegrityError as e:
            pass

        # Populate Transactions table
        cursor.executemany("INSERT INTO Transactions (timestamp, drink, amount, bar) VALUES (?, ?, ?, ?)", [tuple(r) for r in transaction_data.to_numpy()])
        conn.commit()

def poc_create():
    # Run PoC table queries
    with sqlite3.connect(sqlite_db) as conn:
        cursor = conn.cursor()

        with open('poc_tables.sql') as sql_file:
            poc_table_queries = sql_file.read()
            cursor.executescript(poc_table_queries)

        conn.commit()

if __name__ == '__main__':
    main()
# %%
