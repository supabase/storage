alter table
    "storage"."buckets"
add constraint 
    "buckets_name_length_check" 
check
    (length(name) > 0 AND length(name) < 101);