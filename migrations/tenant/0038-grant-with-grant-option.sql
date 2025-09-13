do $$
declare
  super_user text = coalesce(current_setting('storage.super_user', true), 'postgres');
begin
  execute 'grant all on storage.buckets, storage.objects to ' || super_user || ' with grant option';
end $$;
