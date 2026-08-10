CREATE OR REPLACE FUNCTION storage.get_common_prefix(
    p_key text,
    p_prefix text,
    p_delimiter text
)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
SELECT CASE
    WHEN position(p_delimiter IN substring(p_key FROM length(p_prefix) + 1)) > 0
    THEN left(p_key, length(p_prefix) + position(p_delimiter IN substring(p_key FROM length(p_prefix) + 1)))
    ELSE NULL
END;
$$
