# Fluree ledger with STASH Storage 

## Usage

The STASH storage component of the Fluree ledger requires an 
active STASH account.  You must have a valid username, password
and API credentials.

Edit the ledger properties file (resources/fluree_sample.properties)
and add the values for the various 'fdb-storage-stash-'-prefixed properties. 

Set the 'fdb-storage-type' = stash

In your STASH account, create 'ledger' and 'group' directories, create a
snapshots directory in the group directory, and update the fdb-storage-stash-group-prefix
and fdb-storage-stash-ledger-prefix properties accordingly. For example, if you 
create a My Home/ledger directory, then the fdb-storage-stash-ledger-prefix would be 
'My Home/ledger'.
