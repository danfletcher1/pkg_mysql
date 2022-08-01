New Enhancements
================

- Schema Mangement
    Ablility to maintain a consistence schema version. We need to input a file of schema changes. Starting with the origional DB schema, appending all the changes over time in a sequencal and reversable way, with regression methods. 
    
    This can be applied to every copy of the database and everone will have consistent schema for the version

- Split read/write
    Split the read and write functions to seporate channels, this will give us the ability to maintain masters and slaves, and split the workload

- DB failover
    Keep a list of IP's for read and write, and try to failover where things go wrong