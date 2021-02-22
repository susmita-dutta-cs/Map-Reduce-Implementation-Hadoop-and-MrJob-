# MapReduce-Python
2. Online Retail
The RETAIL folder contains the retail sales of one individual company for two years. Original datasets are on the archive of ICS UCI.
1. retail0910.csv: CSV dataset of retail sales for 2009-2010. Parameters include: [Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer, ID, Country]
2. retail1011.csv: CSV dataset of retail sales for 2010-2011. Parameters include: [Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID Country]
2.1 retail.py: Top 10 best customers for both years combined Using MapReduce and the top 10 customers of each retail year that bought the most in terms of total revenue (quantity * price).
2.2 retail.py : Best selling product using MapReduce and the best selling product, once in terms of total quantity, and once in terms of total revenue for both retail years.

3.Text-Similarity
1. arxivData.json: A JSON archive of meta-data on 20.000 scientific papers
on Arxiv
3.1 : Jaccard Similarity Coefcient
Implementing the Jaccard Distance function .
3.2 TASK 6: Cosine Similarity
Implementing the Cosine Similarity function for text.To vectorize the texts TF-IDF algorithm was used.

4.Matrix Multiplication
1. A.txt: A matrix of 1000 rows and 50 columns
2. B.txt: A matrix of 50 rows and 2000 columns
3. C.txt: Dot product of A and B
Implementing  the matrix dot product in MapReduce. You can use the provided matrices to validate your code. The files are loaded using numpy.
