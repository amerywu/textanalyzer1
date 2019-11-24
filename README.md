# Text Analyzer

### The Text Analyzer Project
1. Retrieves data from ElasticSearch
2. Analyzes the data 
3. Reorganizes ES indices and content based on the analysis

### Three Step Process:

1. Extract data from ElasticSearch
2. Pipeline performs multiple highly configurable steps to process the data analysis
3. Post process applies the analysis to ElasticSearchn documents and updates ElasticSearch accordingly

### Process

1. Launches with TextAnalyzerLaunch.py
2. Reads process instructions from ```resources.mermtools.ini```
3. Extracts data from ElasticSearch (ES) and converts to Pandas DataFrame
4. DataFrame enters into processing pipeline.
5. Pipeline prepares data for analysis (e.g., tokenization and lemmatization). Each function in the pipeline is performed in its own class. Classes with related functions may be in the same script.
6. Pipeline performs analyzes (e.g., TF-IDF, LDA, k-means etc.)
7. Post Pipeline processes the analysis results and reorganize ElasticSearch data according to results.

### Deployment
This project is dockerized.