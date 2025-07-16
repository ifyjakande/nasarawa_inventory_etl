import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gspread
import os
from typing import Tuple, Dict, List, Any
from datetime import datetime

SHEET_NAMES = {
    'STOCK_INFLOW': 'stock_inflow',
    'RELEASE': 'release',
    'STOCK_INFLOW_CLEAN': 'stock_inflow_clean',
    'RELEASE_CLEAN': 'release_clean',
    'SUMMARY': 'summary'
}

PRODUCT_TYPES = {
    'WHOLE CHICKEN': 'whole chicken',
    'GIZZARD': 'gizzard'
}

DATE_FORMATS = ['%d %b %Y', '%d/%m/%y', '%d-%b-%Y']
GOOGLE_SHEETS_SCOPE = ['https://www.googleapis.com/auth/spreadsheets']

class DataProcessingError(Exception):
    """Custom exception for data processing errors"""
    pass

def get_credentials(credentials_file: str) -> service_account.Credentials:
    """Create and return credentials for Google Sheets access"""
    try:
        return service_account.Credentials.from_service_account_file(
            credentials_file,
            scopes=GOOGLE_SHEETS_SCOPE
        )
    except Exception as e:
        raise DataProcessingError(f"Failed to create credentials: {str(e)}")

def connect_to_sheets(credentials: service_account.Credentials, spreadsheet_id: str) -> gspread.Spreadsheet:
    try:
        gc = gspread.authorize(credentials)
        return gc.open_by_key(spreadsheet_id)
    except Exception as e:
        raise DataProcessingError(f"Failed to connect to Google Sheets: {str(e)}")

def read_worksheet_to_df(spreadsheet: gspread.Spreadsheet, worksheet_name: str) -> pd.DataFrame:
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        all_values = worksheet.get_all_values()
        if not all_values:
            raise DataProcessingError(f"No data found in worksheet {worksheet_name}")
        
        headers = all_values[0]
        data = all_values[1:]
        df = pd.DataFrame(data, columns=headers)
        
        if 'date' in df.columns:
            print(f"\nUnique date values in {worksheet_name}:")
        
        return df
    except Exception as e:
        raise DataProcessingError(f"Failed to read worksheet {worksheet_name}: {str(e)}")

def standardize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    try:
        print("\nStandardizing dataframe...")
        
        df_clean = df.copy()
        
        # Standardize column names
        df_clean.columns = (df_clean.columns.str.lower()
                          .str.strip()
                          .str.replace(' ', '_')
                          .str.replace('-', '_'))
        
        # Handle the weight_in_kg to weight rename
        if 'weight_in_kg' in df_clean.columns:
            df_clean = df_clean.rename(columns={'weight_in_kg': 'weight'})
        
        for column in df_clean.columns:
            df_clean[column] = df_clean[column].astype(str).str.strip().str.lower()
            try:
                numeric_values = pd.to_numeric(df_clean[column].str.replace(',', ''))
                df_clean[column] = numeric_values
            except (ValueError, TypeError):
                pass
        
        return df_clean
    except Exception as e:
        raise DataProcessingError(f"Failed to standardize dataframe: {str(e)}")

def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    
    try:
        print("\nStandardizing dates...")
        df = df.copy()
        
        date_parsed = False
        for format in DATE_FORMATS:
            try:
                print(f"Trying date format: {format}")
                df['date'] = pd.to_datetime(df['date'], format=format)
                date_parsed = True
                print("Successfully parsed dates using format:", format)
                break
            except ValueError as e:
                print(f"Failed to parse with format {format}: {str(e)}")
                continue
        
        if not date_parsed:
            print("Falling back to mixed format parsing")
            df['date'] = pd.to_datetime(df['date'], format='mixed', dayfirst=True)
        
        if df['date'].isna().any():
            problematic_dates = df[df['date'].isna()]['date'].unique()
            print("Warning: Failed to parse these dates:", problematic_dates)
            raise DataProcessingError(f"Failed to parse dates: {problematic_dates}")
        
        df['month'] = df['date'].dt.strftime('%b').str.lower()
        df['year_month'] = df['date'].dt.strftime('%Y-%b')
        
        return df
    except Exception as e:
        raise DataProcessingError(f"Failed to standardize dates: {str(e)}")


def create_summary_df(stock_inflow_df: pd.DataFrame, release_df: pd.DataFrame) -> pd.DataFrame:
    try:
        print("\nCreating summary dataframe...")
        
        all_year_months = sorted(list(set(stock_inflow_df['year_month'].unique()) | 
                                    set(release_df['year_month'].unique())))
        
        summary_df = pd.DataFrame({'year_month': all_year_months})
        summary_df['month'] = summary_df['year_month'].str.split('-').str[1].str.lower()
        summary_df = summary_df[['month', 'year_month']]
        
        product_summaries = {
            'chicken_inflow': stock_inflow_df[
                stock_inflow_df['product_type'] == PRODUCT_TYPES['WHOLE CHICKEN']
            ].groupby('year_month').agg({
                'quantity': 'sum',
                'weight': 'sum'
            }),
            'chicken_release': release_df[
                release_df['product'] == PRODUCT_TYPES['WHOLE CHICKEN']
            ].groupby('year_month').agg({
                'quantity': 'sum',
                'weight': 'sum'
            }),
            'gizzard_inflow': stock_inflow_df[
                stock_inflow_df['product_type'] == PRODUCT_TYPES['GIZZARD']
            ].groupby('year_month').agg({
                'weight': 'sum'
            }),
            'gizzard_release': release_df[
                release_df['product'] == PRODUCT_TYPES['GIZZARD']
            ].groupby('year_month').agg({
                'weight': 'sum'
            })
        }
        
        summary_columns = {
            'total_chicken_inflow_quantity': ('chicken_inflow', 'quantity'),
            'total_chicken_inflow_weight': ('chicken_inflow', 'weight'),
            'total_chicken_release_quantity': ('chicken_release', 'quantity'),
            'total_chicken_release_weight': ('chicken_release', 'weight'),
            'total_gizzard_inflow_weight': ('gizzard_inflow', 'weight'),
            'total_gizzard_release_weight': ('gizzard_release', 'weight')
        }
        
        for col_name, (summary_key, metric) in summary_columns.items():
            if metric in product_summaries[summary_key].columns:
                summary_df[col_name] = summary_df['year_month'].map(
                    product_summaries[summary_key][metric]).fillna(0)
            else:
                summary_df[col_name] = 0

        # Sort by year_month in ascending order to process chronologically
        summary_df['sort_date'] = pd.to_datetime(summary_df['year_month'], format='%Y-%b')
        summary_df = summary_df.sort_values('sort_date')

        # Initialize opening stock and stock balance columns
        opening_stock_columns = [
            'chicken_quantity_opening_stock',
            'chicken_weight_opening_stock',
            'gizzard_weight_opening_stock'
        ]
        
        stock_balance_columns = [
            'chicken_quantity_stock_balance',
            'chicken_weight_stock_balance',
            'gizzard_weight_stock_balance'
        ]
        
        for column in opening_stock_columns + stock_balance_columns:
            summary_df[column] = 0.0

        # Calculate running balances for each month
        for i in range(len(summary_df)):
            if i == 0:
                # For the first month, opening stock is 0
                summary_df.iloc[i, summary_df.columns.get_loc('chicken_quantity_opening_stock')] = 0
                summary_df.iloc[i, summary_df.columns.get_loc('chicken_weight_opening_stock')] = 0
                summary_df.iloc[i, summary_df.columns.get_loc('gizzard_weight_opening_stock')] = 0
            else:
                # For subsequent months, opening stock is previous month's balance
                summary_df.iloc[i, summary_df.columns.get_loc('chicken_quantity_opening_stock')] = \
                    summary_df.iloc[i-1, summary_df.columns.get_loc('chicken_quantity_stock_balance')]
                summary_df.iloc[i, summary_df.columns.get_loc('chicken_weight_opening_stock')] = \
                    summary_df.iloc[i-1, summary_df.columns.get_loc('chicken_weight_stock_balance')]
                summary_df.iloc[i, summary_df.columns.get_loc('gizzard_weight_opening_stock')] = \
                    summary_df.iloc[i-1, summary_df.columns.get_loc('gizzard_weight_stock_balance')]

            # Calculate stock balances for current month
            summary_df.iloc[i, summary_df.columns.get_loc('chicken_quantity_stock_balance')] = (
                summary_df.iloc[i, summary_df.columns.get_loc('chicken_quantity_opening_stock')] +
                summary_df.iloc[i, summary_df.columns.get_loc('total_chicken_inflow_quantity')] -
                summary_df.iloc[i, summary_df.columns.get_loc('total_chicken_release_quantity')]
            )

            summary_df.iloc[i, summary_df.columns.get_loc('chicken_weight_stock_balance')] = (
                summary_df.iloc[i, summary_df.columns.get_loc('chicken_weight_opening_stock')] +
                summary_df.iloc[i, summary_df.columns.get_loc('total_chicken_inflow_weight')] -
                summary_df.iloc[i, summary_df.columns.get_loc('total_chicken_release_weight')]
            )

            summary_df.iloc[i, summary_df.columns.get_loc('gizzard_weight_stock_balance')] = (
                summary_df.iloc[i, summary_df.columns.get_loc('gizzard_weight_opening_stock')] +
                summary_df.iloc[i, summary_df.columns.get_loc('total_gizzard_inflow_weight')] -
                summary_df.iloc[i, summary_df.columns.get_loc('total_gizzard_release_weight')]
            )

        # Sort in descending order (newest first) and clean up
        summary_df = summary_df.sort_values('sort_date', ascending=False)
        summary_df['year_month'] = summary_df['sort_date'].dt.strftime('%Y-%m')
        summary_df = summary_df.drop('sort_date', axis=1)
        
        return summary_df
    except Exception as e:
        raise DataProcessingError(f"Failed to create summary: {str(e)}")

def prepare_df_for_upload(df: pd.DataFrame) -> pd.DataFrame:
    print("\nPreparing dataframe for upload...")
    df_copy = df.copy()
    
    date_columns = df_copy.select_dtypes(include=['datetime64']).columns
    for col in date_columns:
        df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')
    
    for col in df_copy.columns:
        df_copy[col] = df_copy[col].fillna('')
        df_copy[col] = df_copy[col].astype(str)
        df_copy[col] = df_copy[col].replace('nan', '')
    
    return df_copy

def upload_df_to_gsheet(df: pd.DataFrame, 
                       spreadsheet_id: str, 
                       sheet_name: str, 
                       service: Any) -> bool:
    try:
        print(f"\nUploading data to sheet: {sheet_name}")
        df_to_upload = prepare_df_for_upload(df)
        
        values = [df_to_upload.columns.tolist()]
        values.extend([[str(cell) if cell is not None and cell == cell else '' 
                       for cell in row] for row in df_to_upload.values.tolist()])
        
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_name}!A1:ZZ'
        ).execute()
        
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_name}!A1',
            valueInputOption='RAW',
            body={'values': values}
        ).execute()
        
        print(f"Updated {result.get('updatedCells')} cells in {sheet_name}")
        return True
        
    except Exception as e:
        print(f"Failed to upload to {sheet_name}: {str(e)}")
        return False

def process_sheets_data(stock_inflow_df: pd.DataFrame, 
                       release_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    try:
        print("\nProcessing sheets data...")
        
        stock_inflow_df = standardize_dataframe(stock_inflow_df)
        
        # Handle the weight_at_delivery to weight rename for stock_inflow only
        if 'weight_at_delivery' in stock_inflow_df.columns:
            stock_inflow_df = stock_inflow_df.rename(columns={'weight_at_delivery': 'weight'})
        
        release_df = standardize_dataframe(release_df)
        
        # Filter out rows with empty dates since date is required
        stock_inflow_df = stock_inflow_df[stock_inflow_df['date'].notna() & (stock_inflow_df['date'] != '')]
        release_df = release_df[release_df['date'].notna() & (release_df['date'] != '')]
        
        stock_inflow_df = standardize_dates(stock_inflow_df)
        release_df = standardize_dates(release_df)
        
        release_df.loc[
            release_df['product'].str.contains(PRODUCT_TYPES['GIZZARD'], 
                                             case=False, na=False), 
            'quantity'
        ] = 0
        
        summary_df = create_summary_df(stock_inflow_df, release_df)
        
        return stock_inflow_df, release_df, summary_df
    
    except Exception as e:
        raise DataProcessingError(f"Failed to process sheets data: {str(e)}")

def main():
    CREDENTIALS_FILE = 'credentials.json'
    
    try:
        print("\nStarting data processing...")
        
        source_spreadsheet_id = os.getenv('SOURCE_SPREADSHEET_ID')
        output_spreadsheet_id = os.getenv('OUTPUT_SPREADSHEET_ID')
        
        if not source_spreadsheet_id:
            raise DataProcessingError("SOURCE_SPREADSHEET_ID environment variable not set")
        if not output_spreadsheet_id:
            raise DataProcessingError("OUTPUT_SPREADSHEET_ID environment variable not set")
            
        # Create credentials and services once
        credentials = get_credentials(CREDENTIALS_FILE)
        source_spreadsheet = connect_to_sheets(credentials, source_spreadsheet_id)
        sheets_service = build('sheets', 'v4', credentials=credentials)
        
        # Read the worksheets from source
        stock_inflow_df = read_worksheet_to_df(source_spreadsheet, SHEET_NAMES['STOCK_INFLOW'])
        release_df = read_worksheet_to_df(source_spreadsheet, SHEET_NAMES['RELEASE'])
        
        # Process the data
        stock_inflow_df, release_df, summary_df = process_sheets_data(
            stock_inflow_df, release_df)
        
        # Define upload tasks
        upload_tasks = [
            (stock_inflow_df, SHEET_NAMES['STOCK_INFLOW_CLEAN']),
            (release_df, SHEET_NAMES['RELEASE_CLEAN']),
            (summary_df, SHEET_NAMES['SUMMARY'])
        ]
        
        # Upload all datasets
        success = True
        for df, sheet_name in upload_tasks:
            if not upload_df_to_gsheet(df, output_spreadsheet_id, sheet_name, sheets_service):
                success = False
                print(f"Failed to upload {sheet_name}")
        
        if success:
            print("\nData processing and upload completed successfully!")
        else:
            raise DataProcessingError("Failed to upload one or more datasets")
            
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()