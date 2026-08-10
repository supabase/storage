CREATE OR REPLACE FUNCTION storage.get_size_by_bucket()
 RETURNS TABLE(
    size bigint,
    bucket_id text
)
 LANGUAGE plpgsql
 STABLE
AS $function$
BEGIN
    return query
        select sum((metadata->>'size')::bigint)::bigint as size, obj.bucket_id
        from "storage".objects as obj
        group by obj.bucket_id;
END
$function$

