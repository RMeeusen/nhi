--select * into public._backup_administratiefgebied from public.administratiefgebied a ;

alter table public.administratiefgebied add if not exists importdir varchar;

delete from public.administratiefgebied where administratiefgebiedid in (15,23);

update public.administratiefgebied set administratiefgebied = 'limburg' where administratiefgebiedid =11;

update public.administratiefgebied set importdir = case 
 when administratiefgebiedid = 1 then 'aaenmaas'
 when administratiefgebiedid = 2 then 'amstelgooienvecht'
 when administratiefgebiedid = 3 then 'brabantsedelta'
 when administratiefgebiedid = 4 then 'delfland'
 when administratiefgebiedid = 5 then 'dedommel'
 when administratiefgebiedid = 6 then 'wetterskipfryslan'
 when administratiefgebiedid = 7 then 'hollandsnoorderkwartier'
 when administratiefgebiedid = 8 then 'hollandsedelta'
 when administratiefgebiedid = 9 then 'hunzeenaas'
 when administratiefgebiedid = 10 then 'noorderzijlvest'
 when administratiefgebiedid = 11 then 'limburg'
 when administratiefgebiedid = 12 then 'rijnenijssel'
 when administratiefgebiedid = 13 then 'rijnland'
 when administratiefgebiedid = 14 then 'rivierenland'
 when administratiefgebiedid = 16 then 'schielandendekrimepenerwaard'
 when administratiefgebiedid = 17 then 'stichtserijnlanden'
 when administratiefgebiedid = 18 then 'zuiderzeeland'
 when administratiefgebiedid = 19 then 'scheldestromen'
 when administratiefgebiedid = 20 then 'valleienveluwe'
 when administratiefgebiedid = 21 then 'vechtstromen'
 when administratiefgebiedid = 22 then 'drentsoverijsselsedelta'
 end;

alter table administratiefgebied add if not exists active bool;

update administratiefgebied  set active = true where administratiefgebiedid in (20);
--update administratiefgebied  set active = true where administratiefgebiedid in (1,5,11,14,17,20);

