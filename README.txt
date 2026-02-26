Analytics Workbench — Demo Package

What this is
- A local analytics tool that queries Parquet files with DuckDB and exports results to Excel.
- Runs on your computer (no internet required).
- UI opens in your browser.

How to Run
1) Unzip the folder to a location on your PC (recommended: Desktop or Documents).
2) Double-click START_HERE.bat
3) Your browser will open to:
   http://127.0.0.1:8000/ui/

Where to put your own Parquet files
- You do NOT need to copy your Parquet into this folder.
- Keep your Parquet wherever it lives on your computer (recommended).
- Add it through the UI using “Add dataset” (scan + register).

Add your Parquet via the UI (recommended workflow)
1) In the UI, go to the “Add dataset” panel.
2) In “Folder to scan”, paste the folder that contains your .parquet file(s)
   Example:
     C:\Data\Parquet
3) Click Scan (enable Recursive if needed).
4) Click Select on the parquet file you want.
5) Give it a Dataset name.
6) Choose Storage mode:
   - Reference (recommended): the app remembers the file path (does not copy the parquet).
   - Copy: copies parquet into this app folder under data\datasets\<dataset>\

Run a Query
1) Choose a Dataset and Preset
2) Click Run
3) Click Download Excel to export results

Exports
- Excel files are saved in:
  exports\

Important Notes
- Do not move AnalyticsWorkbench.exe out of this folder.
- If you move the whole folder, that’s fine.
- If you used Reference mode and later move/rename the parquet file, re-register it.

Troubleshooting
- If Windows SmartScreen appears: click “More info” → “Run anyway”.
- If the UI doesn’t open automatically, open:
  http://127.0.0.1:8000/ui/
