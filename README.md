# Ecommerce Data Management Script

This repository contains a Python script designed to process and handle large CSV files of eCommerce product data. The primary function of the script is to reformat data to be compliant with Shopify's product data format.

## Key Features

- **Data Processing**: Efficient handling and manipulation of large CSV files using pandas.
- **Image Management**: Downloads product images from specified URLs and securely uploads them to an Amazon S3 bucket.
- **File Segmentation**: Splits large dataframes into smaller, manageable CSV files for enhanced data management.
- **Command-Line Interface**: Allows users to specify input CSV files directly from the terminal.
- **Parallel Programming**: Improves script performance by utilizing parallel computing concepts.
- **Data Integrity Checks**: Verifies the output data for compliance with Shopify's product data format.

## Usage

To run the script, use the following command in your terminal:

```bash
sh run10.sh /path/to/dir
