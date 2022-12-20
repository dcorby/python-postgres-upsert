## Upsert with Python/Postgres  

A Python module for use with the psycopg2 adapter and a custom database class  
Upsert rows with arbitrary keys as lists of dicts  

### args  

db : db  
  &nbsp;&nbsp;&nbsp;&nbsp;database class with methods get(), get_fields(), sql(), and insert()  
table : str  
  &nbsp;&nbsp;&nbsp;&nbsp;table name as string  
keys : list  
  &nbsp;&nbsp;&nbsp;&nbsp;list of tuples representing unique keys  
upserts : list  
  &nbsp;&nbsp;&nbsp;&nbsp;rows to upsert as a list of dicts  

### kwargs  

where : str  
  &nbsp;&nbsp;&nbsp;&nbsp;limiting clause for current rows  
delete : bool  
  &nbsp;&nbsp;&nbsp;&nbsp;delete unmatched rows  
ignore : list  
  &nbsp;&nbsp;&nbsp;&nbsp;list of fields to ignore on update  
default : dict  
  &nbsp;&nbsp;&nbsp;&nbsp;dict of values and defaults to set on insert  
noinsert : bool  
  &nbsp;&nbsp;&nbsp;&nbsp;do not insert unmatched rows  
get_unmatched : bool  
  &nbsp;&nbsp;&nbsp;&nbsp;return unmatched rows  
overwrite : bool  
  &nbsp;&nbsp;&nbsp;&nbsp;set field to null if field not contained in upsert  
ignorenull : bool  
  &nbsp;&nbsp;&nbsp;&nbsp;do not set fields to null  
before_insert : list  
  &nbsp;&nbsp;&nbsp;&nbsp;hook to run function prior to insert ([func, \*args])  
nonullkeys : bool  
  &nbsp;&nbsp;&nbsp;&nbsp;prevent match on keys will null field values  
keymaps : dict  
  &nbsp;&nbsp;&nbsp;&nbsp;dict of key mappings to make on match  
dryrun : bool  
  &nbsp;&nbsp;&nbsp;&nbsp;print planned database operations to stdout  

