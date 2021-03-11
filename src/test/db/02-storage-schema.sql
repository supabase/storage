-- @todo add sql for indexes
DROP TABLE IF EXISTS "public"."buckets";
CREATE TABLE "public"."buckets" (
    "id" uuid NOT NULL DEFAULT extensions.uuid_generate_v4(),
    "name" varchar,
    "owner" uuid,
    "createdAt" timestamptz DEFAULT now(),
    "updatedAt" timestamptz DEFAULT now(),
    CONSTRAINT "buckets_owner_fkey" FOREIGN KEY ("owner") REFERENCES "auth"."users"("id"),
    PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "bname" ON "public"."buckets" USING BTREE ("name");

DROP TABLE IF EXISTS "public"."objects";
CREATE TABLE "public"."objects" (
    "id" uuid NOT NULL DEFAULT extensions.uuid_generate_v4(),
    "bucketId" uuid,
    "name" varchar,
    "owner" uuid,
    "createdAt" timestamptz DEFAULT now(),
    "updatedAt" timestamptz DEFAULT now(),
    "lastAccessedAt" timestamptz DEFAULT now(),
    "metadata" jsonb,
    CONSTRAINT "objects_bucketId_fkey" FOREIGN KEY ("bucketId") REFERENCES "public"."buckets"("id"),
    CONSTRAINT "objects_owner_fkey" FOREIGN KEY ("owner") REFERENCES "auth"."users"("id"),
    PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "bucketid_objname" ON "public"."objects" USING BTREE ("bucketId","name");

ALTER TABLE objects ENABLE ROW LEVEL SECURITY;

CREATE OR REPLACE FUNCTION public.foldername(name varchar)
 RETURNS varchar[]
 LANGUAGE plpgsql
AS $function$
DECLARE
_parts varchar[];
BEGIN
	select string_to_array(name, '/') into _parts;
	return _parts[1:array_length(_parts,1)-1];
END
$function$;

CREATE OR REPLACE FUNCTION public.filename(name varchar)
 RETURNS varchar
 LANGUAGE plpgsql
AS $function$
DECLARE
_parts varchar[];
BEGIN
	select string_to_array(name, '/') into _parts;
	return _parts[array_length(_parts,1)];
END
$function$;

CREATE OR REPLACE FUNCTION public.extension(name varchar)
 RETURNS varchar
 LANGUAGE plpgsql
AS $function$
DECLARE
_parts varchar[];
_filename varchar;
BEGIN
	select string_to_array(name, '/') into _parts;
	select _parts[array_length(_parts,1)] into _filename;
	-- @todo return the last part instead of 2
	return split_part(_filename, '.', 2);
END
$function$;

-- @todo can this query be optimised further?
CREATE OR REPLACE FUNCTION public.search(prefix text, bucketname text, limits int DEFAULT 100, levels int DEFAULT 1, offsets int DEFAULT 0)
 RETURNS TABLE (
    name text,
    id uuid,
    "updatedAt" TIMESTAMPTZ,
    "createdAt" TIMESTAMPTZ,
    "lastAccessedAt" TIMESTAMPTZ,
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
			where objects.name like prefix || '%'
			and "bucketId" = _bucketId
			GROUP by folder
			order by folder
			limit limits
			offset offsets
		) 
		select files_folders.folder as name, objects.id, objects."updatedAt", objects."createdAt", objects."lastAccessedAt", objects.metadata from files_folders 
		left join objects
		on prefix || files_folders.folder = objects.name
        where objects.id is null or objects."bucketId"=_bucketId;
END
$function$;
