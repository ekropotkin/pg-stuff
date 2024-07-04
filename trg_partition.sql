-- FUNCTION: public.trg_partition()

-- DROP FUNCTION IF EXISTS public.trg_partition();

CREATE OR REPLACE FUNCTION public.trg_partition()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
prefix text := 'public.';
timeformat text;
selector text;
_interval INTERVAL;
tablename text;
startdate text;
enddate text;
create_table_part text;
create_index_part1 text;
create_index_part2 text;
create_index_part3 text;
create_index_part4 text;
create_index_part5 text;
create_index_part6 text;
create_index_part7 text;
create_index_part8 text;
namepart text;
datepart text;

BEGIN
 
-- RAISE NOTICE 'NEW Value: %', NEW;

-- namepart := pg_typeof(NEW);
-- RAISE NOTICE 'NEW type: %', namepart;

-- namepart := substring((string_to_array('(443252907971,14759221917,"2023-02-22 08:00:00+00","2023-02-22 09:00:00+00"', ','))[3],2,7);

namepart := substring((string_to_array(NEW::text, ','))[3],2,7);
-- RAISE NOTICE 'NEW table: %', namepart;

tablename :=  replace(TG_TABLE_NAME || '_p' || namepart,'-','_');
-- RAISE NOTICE 'table name: %', tablename;

EXECUTE 'INSERT INTO ' || prefix || quote_ident(tablename) || ' SELECT ($1).*' USING NEW;
RETURN NULL;
 
EXCEPTION
WHEN undefined_table THEN
  
create_table_part:= 'CREATE TABLE IF NOT EXISTS '|| prefix || tablename || '() INHERITS ('|| TG_TABLE_NAME || ');';
create_index_part1:= 'CREATE INDEX '|| quote_ident(tablename) || '_1 on ' || prefix || quote_ident(tablename) || '(identity_timeinterval1)';
create_index_part2:= 'CREATE INDEX '|| quote_ident(tablename) || '_2 on ' || prefix || quote_ident(tablename) || '(lineitem_usageaccountid1)';
create_index_part3:= 'CREATE INDEX '|| quote_ident(tablename) || '_3 on ' || prefix || quote_ident(tablename) || '(lineitem_productcode)';
create_index_part4:= 'CREATE INDEX '|| quote_ident(tablename) || '_4 on ' || prefix || quote_ident(tablename) || '(lineitem_usagetype)';
create_index_part5:= 'CREATE INDEX '|| quote_ident(tablename) || '_5 on ' || prefix || quote_ident(tablename) || '(lineitem_operation)';
create_index_part6:= 'CREATE INDEX '|| quote_ident(tablename) || '_6 on ' || prefix || quote_ident(tablename) || '(lineitem_resourceid)';
create_index_part7:= 'CREATE INDEX '|| quote_ident(tablename) || '_7 on ' || prefix || quote_ident(tablename) || '(usertag0)';
create_index_part8:= 'CREATE INDEX '|| quote_ident(tablename) || '_8 on ' || prefix || quote_ident(tablename) || '(usertag1)';
 
EXECUTE create_table_part;
EXECUTE create_index_part1;
EXECUTE create_index_part2;
EXECUTE create_index_part3;
EXECUTE create_index_part4;
EXECUTE create_index_part5;
EXECUTE create_index_part6;
EXECUTE create_index_part7;
EXECUTE create_index_part8;

 
EXECUTE 'INSERT INTO ' || prefix || quote_ident(tablename) || ' SELECT ($1).*' USING NEW;
RETURN NULL;
 
END;
$BODY$;

ALTER FUNCTION public.trg_partition()
    OWNER TO postgres;

