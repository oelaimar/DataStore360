from statistics import correlation

from airflow.sdk import dag, task
from narwhals import read_csv

ROOT_DIR = "/opt/airflow"
ffill_bfill = lambda x : x.ffill().bfill()
INPUT_PATH =  f"{ROOT_DIR}/data/raw/store-data.csv"
OUTPUT_PATH = f"{ROOT_DIR}/data/processed/data_cleaned.csv"

@dag(description="extract -> cleaning -> transform -> loading",schedule=None)

def etl_pipline():
    @task
    def extract():

        import pandas as pd
        df = pd.read_csv(INPUT_PATH)
        df.to_csv(OUTPUT_PATH, index=False)

        return OUTPUT_PATH

    @task
    def profiling(file_path):
        import pandas as pd
        from ydata_profiling import ProfileReport
        df = pd.read_csv(file_path)
        profile = ProfileReport(df, title="Data Profiling Report")
        profile.to_file(f"{ROOT_DIR}/reports/rapport_profiling.html")
        return file_path

    @task
    def cleaning(file_path):
        import pandas as pd
        import numpy as np
        #change data format
        df = pd.read_csv(file_path)

        df['Order Date'] = pd.to_datetime(
            df['Order Date'],
            format='mixed',
            errors='coerce'
        )

        df['Ship Date'] = pd.to_datetime(
            df['Ship Date'],
            format='mixed',
            errors='coerce'
        )
        df[['Order Date', 'Ship Date']].dtypes
        # fill the sip mode
        for col in ['Ship Mode', 'Ship Date']:
            df[col] = df.groupby('Order ID')[col].transform(ffill_bfill)
        # calculate actual shipping date

        df['Shipping Days'] = (df['Ship Date'] - df['Order Date']).dt.days

        # Calculate typical shipping time for each Ship Mode
        estimated_days = df.groupby('Ship Mode')['Shipping Days'].median()

        # Assign the estimated number of days based on Ship Mode
        df['Estimated Days'] = df['Ship Mode'].map(estimated_days)

        # Fill missing Ship Date

        df['Ship Date'] = df['Ship Date'].fillna(
            df['Order Date'] + pd.to_timedelta(df['Estimated Days'], unit='D')
        )

        # Recalculate Shipping Days AFTER filling Ship Date
        df['Shipping Days'] = (
                df['Ship Date'] - df['Order Date']
        ).dt.days

        # Estimate missing Ship Mode from Shipping Days
        def estimate_ship_mode(days):
            if pd.isna(days):
                return pd.NA
            return (estimated_days - days).abs().idxmin()

        df['Ship Mode'] = df['Ship Mode'].fillna(
            df['Shipping Days'].apply(estimate_ship_mode)
        )

        # Update the estimated days
        df['Estimated Days'] = df['Ship Mode'].map(estimated_days)

        # Fill Customer Name using the same Customer ID

        df['Customer Name'] = df.groupby('Customer ID')['Customer Name'].transform(lambda x: x.ffill().bfill())

        # Fill remaining missing names
        df['Customer Name'] = df['Customer Name'].fillna('Unknown')

        # normalize names
        df['Customer Name'] = (
            df['Customer Name']
            .str.lower()
            .str.split()
            .apply(lambda x: ' '.join(sorted(x)) if isinstance(x, list) else x)
        )

        segment_mapping = {
            'Home Ofice': 'Home Office',
            'Consumerr': 'Consumer',
            'Corporrate': 'Corporate'
        }

        df['Segment'] = df['Segment'].replace(segment_mapping)

        print("Missing Customer Name:")
        print(df['Customer Name'].isna().sum())

        print("\nSegment values:")
        print(df['Segment'].value_counts(dropna=False))

        # titled the cites
        df['City'] = df['City'].str.strip().str.title()
        # states was good
        # post code
        df['Postal Code'] = df.groupby(['City', 'State'])['Postal Code'].transform(lambda x: x.ffill().bfill())
        # region was good
        print("Postal Code:")
        print(df['Postal Code'].isna().sum())

        # correct the product name with the most frequent one

        df['Product Name'] = (
            df.groupby('Product ID')['Product Name'].transform(lambda x: x.mode()[0] if not x.mode().empty else x)
        )
        # Category
        # the category name
        df['Category'] = df['Category'].str.strip().str.title()

        # sub_category was good
        print("Product Name : ")
        print(df['Product Name'].isna().sum())

        # sales = Price * (1 - Discount) * Quantity
        # product_price = sales / ( (1 - Discount) * Quantity )
        # also we need product cost

        # Remove invalid values BEFORE calculations

        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
        df['Discount'] = pd.to_numeric(df['Discount'], errors='coerce')
        df['Sales'] = pd.to_numeric(df['Sales'], errors='coerce')
        df['Profit'] = pd.to_numeric(df['Profit'], errors='coerce')

        df = df.drop(
            df[(df['Quantity'] <= 0) | pd.isna(df['Quantity'])].index
        )

        df = df.drop(
            df[(df['Discount'] < 0) | (df['Discount'] > 1) | pd.isna(df['Discount'])].index
        )

        def _mode(x):
            m = x.dropna().mode()
            return m.iloc[0] if len(m) else float('nan')

        df['Price'] = df['Sales'] / ((1 - df['Discount']) * df['Quantity'])
        df['Price'] = df['Price'].replace([np.inf, -np.inf], np.nan)
        # fix the prices
        df['Price'] = df['Price'].fillna(df.groupby('Product ID')['Price'].transform(_mode))
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')

        # fix the sales
        df['Sales'] = df['Price'] * (1 - df['Discount']) * df['Quantity']

        # fix the quantity
        df['Quantity'] = (
                    df['Sales'] / ((1 - df['Discount']) * df['Price']).replace(0, np.nan).replace([np.inf, -np.inf],
                                                                                                  np.nan)).round().astype(
            'Int64')

        # we will fix the quantity and sales trough profit
        # fixing the profit
        # profit = sales - (product cost * Quantity)
        # product cost = (sales - profit) / Quantity

        df['Product Cost'] = (df['Sales'] - df['Profit']) / df['Quantity']
        df['Product Cost'] = df['Product Cost'].replace([np.inf, -np.inf], np.nan)

        # fix the Product cost
        df['Product Cost'] = df.groupby('Product ID')['Product Cost'].transform(_mode)
        df['Product Cost'] = pd.to_numeric(df['Product Cost'], errors='coerce')

        # fix the profit

        df['Profit'] = df['Sales'] - (df['Product Cost'] * df['Quantity'])

        # the ones who have the same profit ad the same discount must be have the same quantity and the same price
        cols = ['Product ID', df['Discount'].round(4), df['Profit'].round(4)]

        for col in ['Sales', 'Quantity']:
            fill = df.groupby(cols)[col].transform('first')
            df[col] = df[col].fillna(fill)

        # calculate Quantity from Profit
        denom = df['Price'] * (1 - df['Discount']) - df['Product Cost']
        df['Quantity'] = (df['Profit'] / denom).replace([np.inf, -np.inf], np.nan).round().astype('Int64').fillna(
            df['Quantity'])
        # calculate Sales from Quantity
        df['Sales'] = df['Price'] * (1 - df['Discount']) * df['Quantity']

        # calcul the quantity and price related to subb_categorty
        for col in ['Price', 'Quantity']:
            df[col] = df[col].astype('Float64').fillna(df.groupby('Sub-Category')[col].transform('median'))
        df['Quantity'] = df['Quantity'].round().astype('Int64')

        # recalculate Sales
        df['Sales'] = df['Price'] * (1 - df['Discount']) * df['Quantity']

        # the product cost
        df['Product Cost'] = (df['Sales'] - df['Profit']) / df['Quantity']

        df.to_csv(OUTPUT_PATH)
        return OUTPUT_PATH

    @task
    def transformation(file_path):
        import pandas as pd
        import numpy as np
        import hashlib
        df = pd.read_csv(file_path)

        hashing = lambda x: hashlib.sha256(x.encode()).hexdigest()
        df['Customer ID'] = df['Customer ID'].astype(str).apply(hashing)

        df['Customer Name'] = df['Customer Name'].astype(str).apply(hashing)

        df.to_csv(OUTPUT_PATH)

        return OUTPUT_PATH

    transformation(cleaning(profiling(extract())))
etl_pipline()