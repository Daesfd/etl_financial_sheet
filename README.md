# Architeture
<img src="https://github.com/Daesfd/etl_financial_sheet/blob/main/docs/images/Flowchart%20Template.jpg" width="800" height="500">

1. Extraction of multiples stocks using BeautifulSoup.
2. Load of the extracted uncleaned data into a bucket in GCS.
3. Transformation the unclean data into a clean data.
4. Load of the clean data into a bucket in GCS.
5. Creation of the a new dataframe with financial ratios data.
6. Load of the financial ratio data into a bucket in GCS.
7. Creation of images, which compares the industry average with a chosen stock. 
8. Load of the images into a bucket in GCS.

Scheduling made using Docker and Apache Airflow.

Infraestructure made using Terraform to create GCP resources.

# Output
<img src="https://github.com/Daesfd/etl_financial_sheet/blob/main/docs/images/Liquidez_Geral.png" width="300" height="300"> <img src="https://github.com/Daesfd/etl_financial_sheet/blob/main/docs/images/Composicao_do_Endividamento.png" width="300" height="300">
<img src="https://github.com/Daesfd/etl_financial_sheet/blob/main/docs/images/ROE-RSPL.png" width="300" height="300"> <img src="https://github.com/Daesfd/etl_financial_sheet/blob/main/docs/images/NIG.png" width="300" height="300">
<img src="https://github.com/Daesfd/etl_financial_sheet/blob/main/docs/images/PMRV.png" width="300" height="300"> <img src="https://github.com/Daesfd/etl_financial_sheet/blob/main/docs/images/CF.png" width="300" height="300">

For the others [financial ratios](https://github.com/Daesfd/etl_financial_sheet/blob/main/docs/images/)

