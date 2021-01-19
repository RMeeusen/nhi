create schema if not exists temp;
--create extension if not exists tablefunc;

create table if not exists temp._xml_dwarsprofiel (
	xml_element_id int8 null,
	xml_parent_element_id int8 null,
	xml_element_level int8 null,
	xml_data_name varchar(1024) null,
	xml_data_value varchar(1024) null
);
truncate table temp._xml_dwarsprofiel;

drop view if exists temp._xml_dwarsprofiel_view cascade;
create or replace view temp._xml_dwarsprofiel_view as
with x as (
	select *
	from (
		select *, row_number() over (partition by xml_element_id order by xml_data_value) _nr 
		from temp._xml_dwarsprofiel
		where xml_element_id > 4 -- skip first lines containing schema, bbox, etc
	) x1
	where _nr=1
)
select *
from (
	select 
	x.xml_parent_element_id, x.xml_data_name, x.xml_data_value
	from x 
	where x.xml_element_level = 4 
		union all
	select x.xml_parent_element_id, x5.xml_data_name, x5.xml_data_value
	from x join x as x5 on x5.xml_parent_element_id=x.xml_element_id and x5.xml_element_level=5
		union all
	select x.xml_parent_element_id, x6.xml_data_name, x6.xml_data_value
	from x
	join x as x5 on x5.xml_parent_element_id=x.xml_element_id and x5.xml_element_level=5
	join x as x6 on x6.xml_parent_element_id=x5.xml_element_id and x6.xml_element_level=6
	) q
order by 1
;

drop view if exists temp._cols_dwarsprofiel cascade;
create or replace view temp._cols_dwarsprofiel as
	select 'administratiefgebied'::text as c
	union all
	select 'code'::text as c
	union all
	select 'codevolgnummer'::text as c
	union all
	select 'coordinates'::text as c
	union all
	select 'pointProperty'::text as c
	union all
	select 'profielcode'::text as c
	union all
	select 'ruwheidstypecode'::text as c
	union all
	select 'ruwheidswaardehoog'::text as c
	union all
	select 'ruwheidswaardelaag'::text as c
	union all
	select 'srsName'::text as c
	union all
	select 'statusobjectcode'::text as c
	union all
	select 'typeprofielcode'::text as c
;

