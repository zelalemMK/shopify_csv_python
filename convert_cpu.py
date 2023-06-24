import time
import requests
from multiprocessing import Pool, cpu_count
import argparse

import pandas as pd
import boto3
from botocore.exceptions import ClientError

"""
functions to create
1. read file name from terminal. return a dataframe from the csv file.
2. create bucket if it doesn't exist
3. upload images to s3 bucket

reorgnize file, add more comments

"""


# use terminal input as csv file
parser = argparse.ArgumentParser(description='Process a CSV file.')
parser.add_argument('csv_file', type=str, help='CSV file to process')
args = parser.parse_args()
csv_file = args.csv_file

def create_bucket(s3, bucket_name, retries=3, delay=10):
    for attempt in range(retries):
        try:
            s3.create_bucket(Bucket=bucket_name)
            return
        except ClientError as e:
            if e.response['Error']['Code'] == 'OperationAborted':
                print(f'Bucket creation failed due to a conflicting operation. Retrying in {delay} seconds...')
                time.sleep(delay)
            elif e.response['Error']['Code'] in ('BucketAlreadyOwnedByYou', 'BucketAlreadyExists'):
                print(f'Bucket {bucket_name} already exists. No action needed.')
                return
            else:
                raise e
    raise Exception(f'Bucket creation failed after {retries} attempts.')

def split_dataframe(df, chunk_size=2000):
        num_chunks = len(df) // chunk_size
        if len(df) % chunk_size:
            num_chunks += 1
        return (df[i*chunk_size:(i+1)*chunk_size] for i in range(num_chunks))

s3 = boto3.client('s3')
df = pd.read_csv(args.csv_file)
cc = len(df.index)

# bucket name for testing 
bucket_name = "shopify-product-img-delete"


def uploadimg(params):
    url, imgname = params
    imgrequest = requests.get(url, stream=True)
    imgobject = imgrequest.raw
    imgdata = imgobject.read()
    s3.put_object(Bucket=bucket_name, Key=imgname, Body=imgdata)
    print("current img: " + imgname)

def main():
    create_bucket(s3, bucket_name)
    sku = []
    price = []
    title = []
    description = []
    vendor = []
    imgurl = []
    category = []

    args = []
    for i in range(cc):
        df.iloc[i, 2] = df.iloc[i, 2].replace("https://www.jrrshop.com/media/catalog/product", f"https://{bucket_name}.s3.amazonaws.com/")
        category.append(df.iloc[i, 1])
        description.append(df.iloc[i, 2])
        sku.append(str(df.iloc[i, 0]))
        price.append(df.iloc[i, 12])
        title.append(str(df.iloc[i, 9]))
        vendor.append("East West")
        imgurl.append("https://www.jrrshop.com/media/catalog/product" + str(df.iloc[i, 22]))
        imgname = str(df.iloc[i, 22])
        imgurltmp = ("https://www.jrrshop.com/media/catalog/product" + str(df.iloc[i, 22]))
        args.append((imgurltmp,imgname))
    
    with Pool(processes=cpu_count()) as pool:
        pool.map(uploadimg, args)

    output = pd.DataFrame({
        'Handle': sku,
        'Title': title,
        'Body (HTML)': description,
        'Vendor': vendor,
        'Product Category': ["4360"] * cc,
        'Type': category,
        'Tags': [""] * cc,
        'Published': ["TRUE"] * cc,
        'Option1 Name': [""] * cc,
        'Option1 Value': [""] * cc,
        'Option2 Name': [""] * cc,
        'Option2 Value': [""] * cc,
        'Option3 Name': [""] * cc,
        'Option3 Value': [""] * cc,
        'Variant SKU': sku,
        'Variant Grams': [""] * cc,
        'Variant Inventory Tracker': [""] * cc,
        'Variant Inventory Qty': [""] * cc,
        'Variant Inventory Policy': ["continue"]*cc,
        'Variant Fulfillment Service': ["manual"]*cc,
        'Variant Price': price,
        'Variant Compare At Price': [""] * cc,
        'Variant Requires Shipping': ["FALSE"]*cc,
        'Variant Taxable': ["TRUE"]*cc,
        'Variant Barcode': [""] * cc,
        'Image Src': imgurl,
        'Image Position': [""] * cc,
        'Image Alt Text': [""] * cc,
        'Gift Card': "FALSE",
        'SEO Title': [""] * cc,
        'SEO Description': [""] * cc,
        'Google Shopping / Google Product Category': [""] * cc,
        'Google Shopping / Gender': [""] * cc,
        'Google Shopping / Age Group': [""] * cc,
        'Google Shopping / MPN': [""] * cc,
        'Google Shopping / AdWords Grouping': [""] * cc,
        'Google Shopping / AdWords Labels': [""] * cc,
        'Google Shopping / Condition': [""] * cc,
        'Google Shopping / Custom Product': [""] * cc,
        'Google Shopping / Custom Label 0': [""] * cc,
        'Google Shopping / Custom Label 1': [""] * cc,
        'Google Shopping / Custom Label 2': [""] * cc,
        'Google Shopping / Custom Label 3': [""] * cc,
        'Google Shopping / Custom Label 4': [""] * cc,
        'Variant Image': [""] * cc,
        'Variant Weight Unit': [""] * cc,
        'Variant Tax Code': [""] * cc,
        'Cost per item': [""] * cc,
        'Price / International': [""] * cc,
        'Compare At Price / International': [""] * cc,
        'Status': ["active"]*cc,
        })

    output_path = '/Users/zola/Downloads/shopifycsv'
    for i, chunk in enumerate(split_dataframe(output)):
        chunk.to_csv(f'{output_path}/products_{i+1}.csv', index=False)

    # test the shopify.csv file matches with the product_template.csv from shopify
    df1 = pd.read_csv('products_1.csv')
    df2 = pd.read_csv('product_template.csv')
    if df1.columns.equals(df2.columns):
        print("ready for shopify upload")
    else:
        print("something not matching between shopify.csv and product_template.csv")

if __name__ == "__main__":
    main()
