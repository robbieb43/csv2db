# csv2db
This is designed to capture piped CSV data (from stdin) and write it to a mysql database.  You will need the mysql C libraries installed therefore.  It is based on the usual borrowed chunks of code (the good bits) and bits that I have cobbled together (the bad bits) - I am not an experienced C coder.

It has been tailored to work with the output from the excellent rtl_433 software project https://github.com/merbanan/rtl_433 and so may need changes to support other CSV types or sources.

Usage:

./csv2db-v2 [OPTIONS]
  -b, --bulk    for bulk mode (leaves DB open for session)
  -d <name>, --database <name>  selects specific database (default is weather)
  -h, --help    print this help and exit
  -i, --ignore  ignore the first line of the CSV (assumed header)
  -l, --log             for log mode (creates second output stream to stdout)
  -s <name>, --server <name>    selects specific database server (default is localhost)
  -t <name>, --table <name>     selects specific database table (default is )
  -u <name>, --user <name>      selects specific database user id (default is )
  -p <name>, --password <name>  selects specific database password (default is )
  -v, --verbose         provides more output to allow debugging bad csv streams (outputs stream to stderr)


