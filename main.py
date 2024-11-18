from fastapi import FastAPI, HTTPException
from airtable import Airtable
import pandas as pd
import numpy as np
import os

app = FastAPI()

# Old Airtable Configuration
BASE_ID_OLD = 'app5s8zl7DsUaDmtx'
API_KEY = 'patELEdV0LAx6Aba3.393bf0e41eb59b4b80de15b94a3d122eab50035c7c34189b53ec561de590dff3'  # Replace with a secure method to fetch the key
TABLE_NAME_OLD = 'backup_linkedin_profile_data_171124'

# New Airtable Configuration
BASE_ID_NEW = 'app5s8zl7DsUaDmtx'
TABLE_NAME_NEW = 'cleaned_profile_information'
TABLE_NAME_NEW1 = 'campaign_input'
# base_id_new = os.getenv('AIRTABLE_BASE_ID', 'appTEXhgxahKgWLgx')
API_KEY_NEW = os.getenv('AIRTABLE_API_KEY', 'patELEdV0LAx6Aba3.393bf0e41eb59b4b80de15b94a3d122eab50035c7c34189b53ec561de590dff3')

airtable_old = Airtable(BASE_ID_OLD, TABLE_NAME_OLD, API_KEY)
airtable_new = Airtable(BASE_ID_NEW, TABLE_NAME_NEW, API_KEY_NEW)
airtable_new1 = Airtable(BASE_ID_NEW, TABLE_NAME_NEW1, API_KEY_NEW)

def record_exists_in_airtable(airtable_instance, record_data, unique_field):
    """
    Check if a record with the same unique identifier already exists in Airtable.
    """
    unique_value = record_data.get(unique_field)
    if not unique_value:
        return False

    # Search Airtable for existing records with the same unique identifier
    search_result = airtable_instance.search(unique_field, unique_value)
    return len(search_result) > 0

def send_to_airtable_if_new(df, airtable_instance, unique_field, desired_fields=None):
    """
    Inserts records into Airtable if they are not already present, based on a unique identifier.
    """
    for i, row in df.iterrows():
        record_data = row.dropna().to_dict()  # Convert each row to a dictionary format
         # Filter only the desired fields if specified
        
        if desired_fields:
            record_data = {field: row[field] for field in desired_fields if field in row and not pd.isna(row[field])}

        # Check if the record already exists based on the unique field
        if not record_exists_in_airtable(airtable_instance, record_data, unique_field):
            try:
                # Insert new record into Airtable
                airtable_instance.insert(record_data)
                print(f"Record {i} inserted successfully.")
            except Exception as e:
                print(f"Failed to insert record {i}: {e}")
        else:
            print(f"Record {i} already exists in Airtable. Skipping insertion.")

@app.get("/fetch_and_update_data")
async def fetch_and_update_data():
    try:
        all_records = airtable_old.get_all()  # Fetch all records

        # Extract fields and convert to DataFrame
        data = [record.get('fields', {}) for record in all_records]
        if not data:
            return {"message": "No data found in the old Airtable."}

        df = pd.DataFrame(data)

        # Replace problematic values
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.where(pd.notnull(df), None)
        
        # Handle missing values
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        df[numerical_cols] = df[numerical_cols].fillna(df[numerical_cols].mean())

        # for column in df.select_dtypes(include=['object']).columns:
        #     if not df[column].isnull().all():  # Avoid error if all values are NaN
        #         df[column].fillna(df[column].mode()[0], inplace=True)
        # Fill missing values in object columns with "unknown"

        for column in df.select_dtypes(include=['object']).columns:
            df[column].fillna("Unknown", inplace=True)


        # # Clean `phoneNumber` field
        # if 'phoneNumber' in df.columns:
        #     df['phoneNumber'] = df['phoneNumber'].apply(
        #         lambda x: "Unknown" if str(x).lower() == "Unknown" else ''.join(filter(str.isdigit, str(x)))
        #     )
        # Clean phoneNumber (remove non-digit characters, handle "Unknown")
        if 'phoneNumber' in df.columns:
            # Convert phoneNumber to string and clean it, keeping "Unknown" as is
            df['phoneNumber'] = df['phoneNumber'].apply(
                lambda x: "Unknown" if str(x).lower() == "unknown" else pd.Series(str(x)).str.replace(r'\D', '', regex=True).iloc[0]
            )

        # Clean `email` field
        if 'email' in df.columns:
            df['email'] = (
                df['email']
                .astype(str)
                .str.lower()               # Convert to lowercase
                .str.strip()               # Remove leading/trailing whitespace
                .apply(lambda x: "Unknown" if x == "," else x.split(',')[-1].strip() if ',' in x else x)  # Handle conditions
            )
            # df['email'] = df['email'].astype(str).str.lower()


        desired_fields = ['linkedinProfileUrl', 'firstName', 'lastName', 'email', 'Company', 'headline', 'linkedinProfile', 'description', 'location', 'imgUrl', 'fullName', 'phoneNumber', 'company', 'companyWebsite', 'timestamp']

        # Send data to the new Airtable
        send_to_airtable_if_new(df, airtable_new, unique_field='linkedinProfile')

        send_to_airtable_if_new(df, airtable_new1, unique_field='linkedinProfile', desired_fields=desired_fields)
        
        return {"message": "Data fetched and updated successfully."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching or processing data: {e}")
