import pandas as pd
import boto3
import requests
import argparse

parser = argparse.ArgumentParser(description='Process a CSV file.')
parser.add_argument('csv_file', type=str, help='CSV file to process')
args = parser.parse_args()





s3 = boto3.client('s3')

csv_file = args.csv_file
df = pd.read_csv(csv_file)
cc = (len(df.index) - 1)

# bucket name for testing 
bucket_name = "shopify-product-img"

def uploadimg(url,imgname):
    # s3.create_bucket(Bucket=bucket_name)
    # get image from url and make variable
    imgrequest = requests.get(url, stream=True)
    # turn into object
    imgobject = imgrequest.raw
    # read object
    imgdata = imgobject.read()
    # Do the actual upload to s3
    # s3.Bucket("shopifytestbucketjrr").put_object(Key=imgname, Body=imgdata)
    s3.put_object(Bucket=bucket_name, Key=imgname, Body=imgdata)
    # verbose logging
    print("current img: " + imgname)

sku = []
price = []
title = []
description = []
vendor = []
imgurl = []
catagory = []
for i in range(cc):
    df.iloc[i, 2] = df.iloc[i, 2].replace("https://www.jrrshop.com/media/catalog/product", f"https://{bucket_name}.s3.amazonaws.com/")
    catagory.append(df.iloc[i, 1])
    description.append(df.iloc[i, 2])
    sku.append(str(df.iloc[i, 0]))
    price.append(df.iloc[i, 12])
    title.append(str(df.iloc[i, 9]))
    vendor.append("East West")
    imgurl.append("https://www.jrrshop.com/media/catalog/product" + str(df.iloc[i, 22]))
    imgname = str(df.iloc[i, 22])
    imgurltmp = ("https://www.jrrshop.com/media/catalog/product" + str(df.iloc[i, 22]))
    # uploadimg(imgurltmp,imgname)
    print(imgname)

output = pd.DataFrame({
    'Handle': sku,
    'Title': title,
    'Body (HTML)': description,
    'Vendor': vendor,
    'Product Category': ["4360"] * cc,
    'Type': catagory,
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


def split_dataframe(df, chunk_size=2000):
    # determine the number of chunks
    num_chunks = len(df) // chunk_size
    if len(df) % chunk_size:
        num_chunks += 1

    return (df[i*chunk_size:(i+1)*chunk_size] for i in range(num_chunks))

output_path = '/Users/zola/Downloads/shopifycsv'
for i, chunk in enumerate(split_dataframe(output)):
    chunk.to_csv(f'{output_path}/products_{i+1}.csv', index=False)


# write to csv
# output.to_csv('shopify.csv', index=False)

# test the shopify.csv file matches with the product_template.csv from shopify
df1 = pd.read_csv('products_1.csv')
df2 = pd.read_csv('product_template.csv')
if df1.columns.equals(df2.columns):
    print("ready for shopify upload")
else:
    print("something not matching between shopify.csv and product_template.csv")
