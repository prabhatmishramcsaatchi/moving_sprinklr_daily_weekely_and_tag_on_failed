import boto3
import json
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO
import time   
# Define S3 client
s3_client = boto3.client('s3')
                
source_bucket='amazon-global-gsmc-sprinklr'
source_weekely_folder='RAWDATA/Fluency-Weekly/'
source_daily_folder='RAWDATA/Fluency-Monthly/'
destination_bucket='wikitablescrapexample'
destination_weekely_folder='amazon_sprinklr_pull/Fluency-Weekly/'
destination_daily_folder='amazon_sprinklr_pull/fluency/'
destination_tag_folder='amazon_sprinklr_pull/Tag-Pull/'
tag_destination_prefix = 'amazon_sprinklr_pull/latest_tag_mapping/'
paid_tag_file = "PAID_Tags_2024_13_FluencyWeekly.json"
organic_tag_file = "Organic_Tags_12_FluencyMonthly.json"
tagg_file_location = 'amazon_sprinklr_pull/latest_tag_mapping/'
follower_data_destination='amazon_sprinklr_pull/follower/'
follower_data_filename='Follower_Data_7_FluencyWeekly.json'    
def copy_specific_file(source_bucket, source_folder, destination_bucket, destination_prefix, source_filename, destination_filename):
    """Copy a specific file from the source folder to the destination prefix and rename it."""
    full_source_key = f"{source_folder}{source_filename}"
    full_destination_key = f"{destination_prefix}{destination_filename}"
    s3_client.copy_object(Bucket=destination_bucket, CopySource={'Bucket': source_bucket, 'Key': full_source_key}, Key=full_destination_key)
    print(f"Copied and renamed {source_filename} to {destination_bucket}/{full_destination_key}")
   
def folder_for_today(prefix):
    """Return the specific folder for today's date under the given prefix"""
    today = datetime.now().strftime('%Y-%m-%d')
    response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=prefix, Delimiter='/')
    
    potential_folders = []

    for folder in response.get('CommonPrefixes', []):
        if today in folder['Prefix']:
            potential_folders.append(folder['Prefix'])

    if potential_folders:
        # Extracting folder name for copying
        folder_name = potential_folders[0].split('/')[-2] + '/'
        return folder_name

    print(f"No folder for today found in prefix {prefix}")
    return None

def copy_folder(source_bucket, source_folder, destination_bucket, destination_folder):
    """
    Copy all objects from the source folder to the destination folder with pagination.
    """
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=source_bucket, Prefix=source_folder):
        for item in page.get('Contents', []):
            copy_source = {'Bucket': source_bucket, 'Key': item['Key']}
            destination_key = item['Key'].replace(source_folder, destination_folder, 1)
            s3_client.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
            
            
            
            
def read_json_lines_from_s3(bucket, key):
    """Fetches a JSON file from S3, reads lines as JSON objects, and returns a DataFrame."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    json_lines = response['Body'].read().decode('utf-8').splitlines()
    data = [json.loads(line) for line in json_lines]
    return pd.DataFrame(data)
    
 

def save_df_to_excel_in_s3(df, bucket, key):
    """Saves a DataFrame as an Excel file in S3."""
    with BytesIO() as output:
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        data = output.getvalue()
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    
 
def combine_and_upload_files(source_bucket, weekly_file_key, daily_file_key, destination_bucket, destination_key,  old_linkedin_key):
    # Read both files and convert to DataFrames
    df_weekly = read_json_lines_from_s3(source_bucket, weekly_file_key)
    df_daily = read_json_lines_from_s3(source_bucket, daily_file_key)
    
    df_linkedin=read_json_lines_from_s3(destination_bucket, old_linkedin_key)
    
    # Combine DataFrames
    combined_df = pd.concat([df_weekly, df_daily,df_linkedin])
     
     
    
    # Convert combined DataFrame back to JSON format
    combined_json = combined_df.to_json(orient='records', lines=True)
    
    # Upload the combined JSON to S3
    s3_client.put_object(Bucket=destination_bucket, Key=destination_key, Body=combined_json)
    print(f"Combined JSON uploaded to {destination_bucket}/{destination_key}")


def lambda_handler(event, context):
    # Process weekly folders
    weekly_folder_name = folder_for_today(source_weekely_folder)
    if weekly_folder_name:
        print("Copying weekly folder:", weekly_folder_name)
        copy_folder(source_bucket, source_weekely_folder + weekly_folder_name, destination_bucket, destination_weekely_folder + weekly_folder_name)
    # Process daily folders and copy Organic_Tags_12_TagPull.json if present
    # copy_specific_file(
    #         source_bucket, 
    #         source_weekely_folder + weekly_folder_name, 
    #         destination_bucket, 
    #         tag_destination_prefix, 
    #         "PAID_Tags_2024_13_FluencyWeekly.json", 
    #         "Paid_Tags_15_TagPull.json"
    #     )
         
    file_key1 = f"{source_weekely_folder}{weekly_folder_name}{paid_tag_file}"
    file_key2 = f"{source_weekely_folder}{weekly_folder_name}Paid_Tags_15_FluencyWeekly.json"  # Example name, adjust as needed
  
    # Read JSON files from S3
    df1 = read_json_lines_from_s3(source_bucket, file_key1)
    df2 = read_json_lines_from_s3(source_bucket, file_key2)
    
    columns_to_drop = [
        'GCCI_SOCIAL_MEDIA__REQUESTING_PR_ORG__OUTBOUND_MESSAGE',
        'GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___OUTBOUND_MESSAGE',
        'GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___PAID_INITIATIVE'
    ]
    df2 = df2.drop(columns=columns_to_drop, errors='ignore')  # 'ignore' to avoid errors if a column is missing
   
  
    # Merge the DataFrames on the 'AD_VARIANT_NAME' column
    merged_df = pd.merge(df1, df2, on='AD_VARIANT_NAME', how='left')

        # Convert the merged DataFrame to JSON Lines format and encode to UTF-8
    merged_json = merged_df.to_json(orient='records', lines=True)

    # Save the merged JSON to an S3 file
    json_key = f"{tagg_file_location}Paid_Tags_15_TagPull.json"
    s3_client.put_object(Bucket=destination_bucket, Key=json_key, Body=merged_json.encode('utf-8'))
      
    daily_folder_name = folder_for_today(source_daily_folder)

 
        # Copy follower data only on Monday
    if datetime.now().weekday() == 0:  # Monday
        print("Today is Monday. Copying follower data from weekly folder.")
            # Assuming the follower data file is named consistently and located in the weekly folder
        copy_specific_file(
                source_bucket,
                source_weekely_folder + weekly_folder_name,
                destination_bucket,
                follower_data_destination,
                follower_data_filename,  # Source filename
                follower_data_filename  # Destination filename can be the same or changed
            )

    copy_specific_file(source_bucket, source_daily_folder + daily_folder_name, destination_bucket, tagg_file_location, "Organic_Tags_12_FluencyMonthly.json", "Organic_Tags_12_TagPull.json")
    
        

    weekly_file_key = f"{source_weekely_folder}{weekly_folder_name}Target_Geography_17_FluencyWeekly.json"
    daily_file_key = f"{source_daily_folder}{daily_folder_name}Target_Geography_17_FluencyMonthly.json"
  
    old_linkedin_key = "amazon_sprinklr_pull/latest_tag_mapping/Target_Geography_17_TagPull.json"
    destination_key = "amazon_sprinklr_pull/latest_tag_mapping/Target_Geography_17_TagPull.json"

 
    combine_and_upload_files(source_bucket, weekly_file_key, daily_file_key, destination_bucket, destination_key,  old_linkedin_key)
    #copy_specific_file(source_bucket, source_daily_folder + daily_folder_name, destination_bucket, tagg_file_location, "Target_Geography_17_FluencyMonthly.json", "Target_Geography_17_TagPull.json")
    if daily_folder_name:
        print("Copying daily folder:", daily_folder_name)
        copy_folder(source_bucket, source_daily_folder + daily_folder_name, destination_bucket, destination_daily_folder + daily_folder_name)
       
    return {
        'statusCode': 200,
        'body': 'Files copied successfully!'
    }          
