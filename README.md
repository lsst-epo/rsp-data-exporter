# rsp-data-exporter

This repo contains the backend service for processing Rubin citizen science data before hosting it publicly for reference via a Zooniverse workflow page. By itself, this service does not do much and works in concert with Rubin citizen science notebooks, the `rubin.citsci` PyPI package, and the Zooniverse platform.

## Data Panel Review

Projects must be approved by the Rubin Data Rights Panel before a Zooniverse project goes live. Your initial curated data will be reviewed by the data panel and must not exceed 100 objects (100 distinct astro image cutouts, etc.). Any data in excess of 100 objects will be truncated such that only 100 objects are processed.

In order for the Data Rights Panel to review your code, complete the following steps:

1. Curate your data using a citizen science notebook
2. Leave all cell output visible, do not clear it out
3. With the citizen science notebook tab in focus in the browser, go to File->Download, this will download the notebook with all of the cell output
4. Attach the downloaded notebook to an email
5. Email the notebook with a message explaining the nature of the data and how you curated to (data rights panel email forthcoming)