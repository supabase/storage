DROP TABLE IF EXISTS "public"."buckets";
CREATE TABLE "public"."buckets" (
    "id" uuid NOT NULL DEFAULT extensions.uuid_generate_v4(),
    "name" text,
    "owner" uuid,
    "created_at" timestamptz DEFAULT now(),
    "updated_at" timestamptz DEFAULT now(),
    CONSTRAINT "buckets_owner_fkey" FOREIGN KEY ("owner") REFERENCES "auth"."users"("id"),
    PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "bname" ON "public"."buckets" USING BTREE ("name");

DROP TABLE IF EXISTS "public"."objects";
CREATE TABLE "public"."objects" (
    "id" uuid NOT NULL DEFAULT extensions.uuid_generate_v4(),
    "bucket_id" uuid,
    "name" text,
    "owner" uuid,
    "created_at" timestamptz DEFAULT now(),
    "updated_at" timestamptz DEFAULT now(),
    "last_accessed_at" timestamptz DEFAULT now(),
    "metadata" jsonb,
    CONSTRAINT "objects_bucketId_fkey" FOREIGN KEY ("bucket_id") REFERENCES "public"."buckets"("id"),
    CONSTRAINT "objects_owner_fkey" FOREIGN KEY ("owner") REFERENCES "auth"."users"("id"),
    PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "bucketid_objname" ON "public"."objects" USING BTREE ("bucket_id","name");
CREATE INDEX name_prefix_search ON objects(name text_pattern_ops);

ALTER TABLE objects ENABLE ROW LEVEL SECURITY;
-- @todo enable RLS only for buckets table

CREATE OR REPLACE FUNCTION public.foldername(name text)
 RETURNS text[]
 LANGUAGE plpgsql
AS $function$
DECLARE
_parts text[];
BEGIN
	select string_to_array(name, '/') into _parts;
	return _parts[1:array_length(_parts,1)-1];
END
$function$;

CREATE OR REPLACE FUNCTION public.filename(name text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
_parts text[];
BEGIN
	select string_to_array(name, '/') into _parts;
	return _parts[array_length(_parts,1)];
END
$function$;

CREATE OR REPLACE FUNCTION public.extension(name text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
_parts text[];
_filename text;
BEGIN
	select string_to_array(name, '/') into _parts;
	select _parts[array_length(_parts,1)] into _filename;
	-- @todo return the last part instead of 2
	return split_part(_filename, '.', 2);
END
$function$;

-- @todo can this query be optimised further?
-- @todo is this vulnerable to sqli
CREATE OR REPLACE FUNCTION public.search(prefix text, bucketname text, limits int DEFAULT 100, levels int DEFAULT 1, offsets int DEFAULT 0)
 RETURNS TABLE (
    name text,
    id uuid,
    updated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    last_accessed_at TIMESTAMPTZ,
    metadata jsonb
  )
 LANGUAGE plpgsql
AS $function$
DECLARE
_bucketId uuid;
BEGIN
    select buckets."id" from buckets where buckets.name=bucketname limit 1 into _bucketId;
	return query 
		with files_folders as (
			select ((string_to_array(objects.name, '/'))[levels]) as folder
			from objects
			where objects.name ilike prefix || '%'
			and bucket_id = _bucketId
			GROUP by folder
			order by folder
			limit limits
			offset offsets
		) 
		select files_folders.folder as name, objects.id, objects.updated_at, objects.created_at, objects.last_accessed_at, objects.metadata from files_folders 
		left join objects
		on prefix || files_folders.folder = objects.name
        where objects.id is null or objects.bucket_id=_bucketId
        order by name asc;
END
$function$;
