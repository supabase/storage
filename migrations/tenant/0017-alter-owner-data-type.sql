do $$
begin
    ALTER TABLE storage.objects ALTER COLUMN owner TYPE text;
exception
    -- SQLSTATE errcodes https://www.postgresql.org/docs/current/errcodes-appendix.html
    when SQLSTATE '0A000' then
        raise notice 'Unable to change data type of owner column due to use by a view or rule';
    when SQLSTATE '2BP01' then
        raise notice 'Unable to change data type of owner column due to dependent objects';
end $$;