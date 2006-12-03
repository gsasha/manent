usage: 
	python Manent.py <action> <params>

<action>: [create|backup|restore|reconstruct|info]

Working with Manent backup system involves several basic action. First,
a backup rule must be created with "create" command, specifying the location
that is backed up and the storage that will contain the backup files. Any number
of rules can be created.

The "create" command is used to create a live database for the backup rule. The
database is used for restoring and to detect changes in case of incremental backup.

After creating the rule, issuing a "backup" command updates the backup of the 
specified rule. It is safe to kill "backup" during its execution. Restarting it
will continue from where the backup has stopped (i.e., from the last successfully
completed file in the backup storage).

Executing "backup" several times is incremental: only the changed data will be re-sent.
Files are split into chunks of configurable size (256K by default), and the incrementality
works on a per-chunk basis, i.e., if a part of a large file was changed, only the changed
chunks will be uploaded to backup.

If the live database was not lost, a backup rule can be restored to a specified location
with a "restore" command. If the live database was lost as well, it can be reconstructed
from the backup using "reconstruct" command. The "reconstruct" command receives the same
parameters as "create", but re-scans the backup storage instead of creating an empty one.

The "info" command prints out the contents of the backup rule (currently useful mostly for
debugging purposes).

create:
-------
	
Manent.py create <label> <srcpath> <trgtype> <trgtype_params>

Creates a new backup rule called "<label>" that backs up the contents of 
directory <srcpath>. Parameters <trgtype> and <trgtype_params> specify
the storage location of the backup.
	
trgtype can be either of: [ftp|directory]

The parameters for "directory" are: [<path>]
The parameters for "ftp" are: [<server> <username> <password> <path>]

backup:
-------

Manent.py backup <label>

Updates the data of backup rule <label>

restore:
--------

Manent.py restore <label> <trgpath>
	
restores the backup rule <label> into the path specified by trgpath

general:
--------

<label> is the name of the backup. The label is used to generate file names,
so avoid using characters such as "/", "\", whitespace etc. It is advised to
use only alphanumeric characters and the underscore.

