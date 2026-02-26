\# Analytics Workbench (Demo)



\## Overview

Analytics Workbench is a lightweight web application for running preset analytics queries against Parquet datasets and exporting results to Excel.



\## Requirements

\- Docker Desktop installed and running



\## Quick Start



1\. Unzip this folder.

2\. Place Parquet datasets into:

&nbsp;  data/datasets/<dataset\_name>/

3\. Run:



&nbsp;  docker compose up --build



4\. Open your browser:

&nbsp;  http://localhost:8000/ui/



\## Data Plug-in Model



Drop Parquet files into:



data/datasets/<dataset\_name>/



Example:

data/datasets/doge/BIG.parquet



\## Exports



Generated Excel files are written to:

exports/



