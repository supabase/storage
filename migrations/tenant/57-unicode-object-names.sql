ALTER TABLE "storage"."objects"
  ADD CONSTRAINT objects_name_check
  CHECK (
    name !~ E'[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]'
    AND POSITION(U&'\FFFE' IN name) = 0
    AND POSITION(U&'\FFFF' IN name) = 0
  );
