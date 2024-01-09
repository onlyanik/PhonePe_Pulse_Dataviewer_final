import mysql.connector
import json
import streamlit as st
import os
import pandas as pd

def myFunc():

    # Your MySQL connection parameters
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'india123',
        'database': 'iitm_phonepe_pulse'
    }

    # Establish a connection to the MySQL server
    conn = mysql.connector.connect(**db_config)

    # Create a cursor object to interact with the database
    cursor = conn.cursor()

   
    directory_path = "D:/pulse-master/data/aggregated/insurance/country/india"

    directories = [d for d in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, d))]

    print(directories)

    for i in directories:
        if i == "state":
            continue
        path = directory_path + "/"+ i

        path2 = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

        for x in path2:
            m = path + "/" + x
            
            with open(m, 'r') as file2:
                data = json.load(file2)

                # Extract relevant data from the JSON
                from_timestamp = data['data']['from']
                to_timestamp = data['data']['to']
                transaction_name = data['data']['transactionData'][0]['name']
                transaction_count = data['data']['transactionData'][0]['paymentInstruments'][0]['count']
                transaction_amount = data['data']['transactionData'][0]['paymentInstruments'][0]['amount']

                # Data to be inserted into the table
                data_to_insert = (from_timestamp, to_timestamp, transaction_name, transaction_count, transaction_amount)

                # Execute the query
                cursor.execute(insert_query, data_to_insert)

                # Commit the changes to the database
                conn.commit()

      
    

    # Streamlit app
    st.title('Streamlit Dropdown ')

    # Display the dataframe
    df= pd.read_sql("select * from phonepe", conn)
    print(df)

    # Create a dropdown menu with dataframe columns as options
    selected_column = st.selectbox('Select a column:', df.columns)

    # Show the selected column
    st.write(f'You selected: {selected_column}')
    st.table(df[[selected_column]])


    # Close the cursor and connection
    cursor.close()
    conn.close()
myFunc()
    


