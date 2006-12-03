usage: 
	python Manent.py <action> <params>

action: [create|backup|restore]

params for create:
	Manent.py create <label> <srcpath> <trgtype> <trgtype_params>
	
	creates special entity labelled and accessed via <label> value.
	it includes all the parameters for target and source
	
	trgtype can be either of: [ftp|directory]

params for backup:
	Manent.py backup <label>

	uses an already created label and backs it up incrementally
params for restore:
	Manent.py restore <label> <trgpath>
	
	restores the backup into the path specified by trgpath

	Enjoy.
