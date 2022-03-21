drop function storage.search;

create or replace function storage.search (
  prefix text,
  bucketname text,
  limits int default 100,
  levels int default 1,
  offsets int default 0,
  search text default '',
  sortcolumn text default 'name',
  sortorder text default 'asc'
) returns table (
    name text,
    id uuid,
    updated_at timestamptz,
    created_at timestamptz,
    last_accessed_at timestamptz,
    metadata jsonb
  )
as $$
declare
  v_order_by text;
begin
  case
    when sortcolumn = 'name' then
      v_order_by = 'name';
    when sortcolumn = 'updated_at' then
      v_order_by = 'updated_at';
    when sortcolumn = 'created_at' then
      v_order_by = 'created_at';
    when sortcolumn = 'last_accessed_at' then
      v_order_by = 'last_accessed_at';
    else
      v_order_by = 'name';
  end case;

  case
    when sortorder = 'asc' then
      v_order_by = v_order_by || ' asc';
    when sortorder = 'desc' then
      v_order_by = v_order_by || ' desc';
    else
      v_order_by = v_order_by || ' asc';
  end case;

  return query execute
    'select p.name,
            p.id,
            p.updated_at,
            p.created_at,
            p.last_accessed_at,
            p.metadata
     from (
       select o.path_tokens[$1] as "name",
              o.id,
              o.updated_at,
              o.created_at,
              o.last_accessed_at,
              o.metadata,
              array_length(regexp_split_to_array(o.name, ''/''), 1) = $1 as is_file
       from storage.objects o
       where o.name ilike $2 || $3 || ''%''
       and o.bucket_id = $4
       order by is_file, ' || v_order_by || '
       limit $5
       offset $6
     ) p;' using levels, prefix, search, bucketname, limits, offsets;
end;
$$ language plpgsql stable;
