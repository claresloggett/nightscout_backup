
# nightscout_backup.py

This script is intended for retrieving data from a personal NightScout
server via the command line, using NightScout's web API. Data will be 
stored as CSV and JSON files on the local machine.

This script does NOT perform a database dump and does not store data in a
format convenient for restoring a lost server.

The most likely use for this script is if you want a local copy of your 
data to play with.

To see usage, run

```
python nightscout_backup.py -h
```

At a minimum, you will need to enter the URL of your nightscout server, either
on the command line with
```
python nightscout_backup.py -u <YOUR-URL>
```
or by editing the hard-coded default `default_base_url` value in the script.