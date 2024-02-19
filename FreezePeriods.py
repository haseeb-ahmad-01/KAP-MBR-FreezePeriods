import streamlit as st
import pandas as pd
import base64
import json
import time
from snowflake.snowpark import Session
import snowflake.snowpark as snowpark
from itertools import product

st.set_page_config(
    page_title= 'Freeze Periods'
)
#st.title("**Freeze Periods Table**")
st.title(f" :grey[{'Freeze Periods Table'}]")

def cart_prod(l1, l2):
   return list(product(l1, l2))

def add_bg_from_url():
    st.markdown(
         f"""
         <style>
         .stApp {{
            background-color: white;
            margin: auto;
            width: 50%;
            padding: 10px;
            
             
         }}
         </style>
         """,
         unsafe_allow_html=True
     )


def get_freezePeriod_data():
    if 'snowflake_connection' not in st.session_state:
        #Connect to Snowflake
        with open('creds_dev.json') as f:
            connection_param = json.load(f)
        st.session_state.snowflake_connection = Session.builder.configs(connection_param).create()   
        session = st.session_state.snowflake_connection
    else: 
        session = st.session_state.snowflake_connection
    
    df = session.table("MBR_FREEZE_PERIODS").to_pandas()

    return df

add_bg_from_url() 


original_DF = get_freezePeriod_data()

if 'df' not in st.session_state:
    st.session_state['df'] = original_DF

df = original_DF

x= 'Filter MBR Scope'

MBR_SCOPE = st.multiselect(
    f":blue[{'Filter MBR Scope'}]",
    list(set(list(df["MBR Scope"]))),
    label_visibility="visible"
    )
MBR_MONTH = st.multiselect(
    f":blue[{'Filter MBR Month'}]",
    list(set(list(df["MBR Month"]))),
    label_visibility="visible"
    )


with st.form("freeze_periods_form"):
        if len(MBR_SCOPE)>0 and len(MBR_MONTH) == 0:
            df =  df[df['MBR Scope'].isin(MBR_SCOPE)]
        elif len(MBR_MONTH)>0 and len(MBR_SCOPE) == 0:
            df = df[df['MBR Month'].isin(MBR_MONTH)]
        elif len(MBR_MONTH)>0 and len(MBR_SCOPE) > 0:
            df = df[df.set_index(['MBR Scope','MBR Month']).index.isin(list(cart_prod(MBR_SCOPE, MBR_MONTH)))]
        else:
            df = df
        st.subheader(":blue[Unfreeze/ Refreeze by using IsFrozen column]")
        edited_data = st.data_editor(
            df,
            column_order = ("MBR Scope","MBR Month","Is Frozen"),
            column_config={
            "FREEZEID": st.column_config.NumberColumn("FREEZEID", disabled=True),
            "MBR Scope": st.column_config.Column("MBR Scope", disabled=True),
            "MBR Month": st.column_config.Column("MBR Month", disabled=True),
            "Is Frozen": st.column_config.CheckboxColumn("Is Frozen", disabled=False) 
            },
            use_container_width=True,
            hide_index=True
            )    
        submit_button = st.form_submit_button("Update record")

if submit_button:
    try:
        df = pd.concat([original_DF,edited_data]).drop_duplicates(['MBR Scope','MBR Month','FREEZEID'],keep='last').sort_values('FREEZEID')
        st.session_state.snowflake_connection.sql("TRUNCATE TABLE MBR_FREEZE_PERIODS").collect()
        st.session_state.snowflake_connection.write_pandas(df,'MBR_FREEZE_PERIODS', overwrite=False)
        st.success("Record(s) updated successfully")
    except Exception as e:
         st.warning(e)