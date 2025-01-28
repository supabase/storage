ALTER TABLE "storage"."objects"
    ADD CONSTRAINT objects_name_check
    CHECK (name SIMILAR TO '[\x09\x0A\x0D\x20-\xD7FF\xE000-\xFFFD\x00010000-\x0010ffff]+');

