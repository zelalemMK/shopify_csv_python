import pandas as pd
import boto3
import requests
s3 = boto3.resource("s3")
df = pd.read_csv('ew.csv')
c = ","
rc = (len(df.columns))
cc = (len(df.index) - 1)
i = 0

def uploadimg(url,imgname):
    # get image from url and make variable
    imgrequest = requests.get(url, stream=True)
    # turn into object
    imgobject = imgrequest.raw
    # read object
    imgdata = imgobject.read()
    # Do the actual upload to s3
    s3.Bucket("shopifytestbucketjrr").put_object(Key=imgname, Body=imgdata)

sku = []
price = []
title = []
description = []
vendor = []
imgurl = []
catagory = []
for i in range(cc):
    df.iloc[i, 2] = df.iloc[i, 2].replace("https://www.jrrshop.com/media/catalog/product", "https://shopifytestbucketjrr.s3.us-west-1.amazonaws.com/")
    catagory.append(df.iloc[i, 1])
    description.append(df.iloc[i, 2])
    sku.append(str(df.iloc[i, 0]))
    price.append(df.iloc[i, 12])
    title.append(str(df.iloc[i, 9]))
    vendor.append("East West")
    imgurl.append("https://www.jrrshop.com/media/catalog/product" + (df.iloc[i, 22]))
    imgname = str(df.iloc[i, 22])
    imgurltmp = ("https://www.jrrshop.com/media/catalog/product" + (df.iloc[i, 22]))
    uploadimg(imgurltmp,imgname)
output = pd.DataFrame(
    {'Handle': sku,
    'Title': title,
    'Body (HTML)': description,
    'Vendor': vendor,
    'Product Category': "4360",
    'Type': catagory,
    'Tags': "",
    'Published': "TRUE",
    'Option1 Name': "",
    'Option1 Value': "",
    'Option2 Name': "",
    'Option2 Value': "",
    'Option3 Name': "",
    'Option3 Value': "",
    'Variant SKU': sku,
    'Variant Grams': "",
    'Variant Inventory Tracker': "",
    'Variant Inventory Qty': "",
    'Variant Inventory Policy': "continue",
    'Variant Fulfillment Service': "manual",
    'Variant Price': price,
    'Variant Compare At Price': "",
    'Variant Requires Shipping': "FALSE",
    'Variant Taxable': "TRUE",
    'Variant Barcode': "",
    'Image Src': imgurl,
    'Image Position': "",
    'Image Alt Text': "",
    'Gift Card': "FALSE",
    'SEO Title': "",
    'SEO Description': "",
    'Google Shopping / Google Product Category': "",
    'Google Shopping / Gender': "",
    'Google Shopping / Age Group': "",
    'Google Shopping / MPN': "",
    'Google Shopping / AdWords Grouping': "",
    'Google Shopping / AdWords Labels': "",
    'Google Shopping / Condition': "",
    'Google Shopping / Custom Product': "",
    'Google Shopping / Custom Label 0': "",
    'Google Shopping / Custom Label 1': "",
    'Google Shopping / Custom Label 2': "",
    'Google Shopping / Custom Label 3': "",
    'Google Shopping / Custom Label 4': "",
    'Variant Image': "",
    'Variant Weight Unit': "",
    'Variant Tax Code': "",
    'Cost per item': "",
    'Price / International': "",
    'Compare At Price / International': "",
    'Status': "active",
    })
# write to csv
output.to_csv('shopify.csv', index=False)
