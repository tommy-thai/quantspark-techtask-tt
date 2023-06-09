# Bar Database

### Assumptions
- Analysts taking over have a basic understanding of Python and SQL
- Input files cannot be modified at source
- Transaction files are programatically generated and will not change formats
- Transaction data extraction is available incrementally, i.e. bars will only provide new data always 

### How to Use
0. Connect your IDE to a branch of this repository or download all the files contained (`README.md` isn't needed)
1. Open `build_database.py`, edit the configuration variables as needed. You may need this if any file locations/names change.
2. Open `poc_tables.SQL`, you may edit the SQL here to add similar tables for the other bars or change the time period to measure the transactions by. The provided SQL tables shows the number of drinks by the hour for London only. The `_hourly_recommended` table averages across each hour and shows an average and upper bound recommendation for how many glasses are needed.
3. Ensure all your input files are in a child directory. The default directory name is `data`.
4. Run `build_database.py`, this will create your database and load in all the data from the provided files.
5. Using SQLiteStudio or similar GUI, you can now freely query the outputted tables!
6. Move the loaded input files into an archive folder or separate location. This is needed as the current code version does not distinguish between files so it will load the next time you run `build_database.py`.
7. *(Incremental load)* Follow steps 1 through 6 with any new files. The existing database will append the new data to the tables 

### Needed additions
- Add logic in code to detect loaded files and not to load them again. At the moment, the code will repeatedly load detected files. An intermediary update may just archive loaded files.
- Adding dev columns to each table to track changes and file loaded from. This will help monitor incremental loads
- The `get_drink_data()` function doesn't always need to run, a condition can be used to check whether the database already has all drinks logged from 'transaction_data' already.
- The `get_drink_data()` function will benefit from refactoring! It currently makes many API calls and has 2 separate code blocks handling this extraction. More detail can be found in the code comments.
- `poc_tables.SQL`, these tables get recreated every run which is suboptimal. This can be modified to create the table the 1st run and incrementally load in subsequent runs.
- Other changes can be made to remove the applied assumptions above.
