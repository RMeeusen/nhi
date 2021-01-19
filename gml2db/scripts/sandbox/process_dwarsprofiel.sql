with x as (
	select *
	from crosstab('select * from temp._xml_dwarsprofiel_view order by 1,2', 'select distinct c from temp._cols_dwarsprofiel order by 1')
	as ct(
		rowid int
		,administratiefgebied int
		,code text
		,codevolgnummer int
		,coordinates text
		,pointproperty text
		,profielcode text
		,ruwheidstypecode int
		,ruwheidswaardehoog decimal
		,ruwheidswaardelaag decimal
		,srsName text
		,statusobjectcode int
		,typeprofielcode int
	) 
)
INSERT INTO public.dwarsprofiel (dwarsprofielid, code, statusobjectid, codevolgnummer, typeprofielid, ruwheidstypeid, ruwheidswaardelaag, ruwheidswaardehoog, profielcode, administratiefgebiedid, geometriepunt)
select nextval('dwarsprofiel_id_seq'), code, coalesce(statusobjectcode, '98')::int as statusobjectid, codevolgnummer::int, typeprofielcode::int, ruwheidstypecode::int, ruwheidswaardelaag::decimal, ruwheidswaardehoog::decimal, profielcode
, ${administratiefgebiedid}::int
, st_pointfromtext('POINT('||replace(coordinates, ',',' ')||')', replace(srsname, 'epsg:','')::int) 
from x
;

