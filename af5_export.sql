select
*
from (
with
  -- таблицы для экспорта
table_names as (
  select column1 tname from (values('tn\_%')/*, ('cg\_%'), ('cat\_%')*/) v
),
  -- исключения
not_table_names as (
  select column1 tname from (values('tn\_field%')) v
),

inserttables as (
  select rel.oid, rel.relname from pg_catalog.pg_class rel
  join pg_catalog.pg_namespace nsp on nsp.oid = relnamespace
  where rel.relname::text like any(select tname from table_names)
        and rel.relname::text not like all(select tname from not_table_names)
        and rel.relname::text not ilike all (select column1 from (values('%\_al'), ('%\_acl'), ('%\_read')) v) and nsp.nspname = 'public'
),

tables as (
  select rel.oid, rel.relname from pg_catalog.pg_class rel
  join pg_catalog.pg_namespace nsp on nsp.oid = relnamespace
  where rel.relname::text like any(select tname from table_names) and
        rel.relname::text not like all(select tname from not_table_names) and nsp.nspname = 'public'
),

inds as (
  select
  conrelid::regclass::text table_name,
  conname constraint_name,
  confrelid::oid as oid,
  conrelid::oid as targetoid,
  pg_catalog.pg_get_constraintdef(r.oid, true) definition,
  othatts,
  othattsprefix
  from pg_catalog.pg_constraint r
  join tables on tables.oid = confrelid
  left join tables tablesnull on tablesnull.oid = conrelid
  join lateral (
    select
           string_agg(attname, ', ') as othatts,
           string_agg(conrelid::regclass::text || '.' || attname, ', ') as othattsprefix
    from pg_catalog.pg_attribute att where att.attrelid = conrelid and attnum = any(conkey)
  ) tmp on 1 = 1
  where r.contype = 'f' and tablesnull.oid is null
  and exists (
    select 1 from pg_catalog.pg_attribute where attnum = any(confkey) and attrelid = tables.oid
    and attname !=
      all(array['group_id', 'id_type', 'created_by', 'created_by_type', 'updated_by', 'updated_by_type', 'status', 'status_type'])
  )
),

hiersbase as (
  with recursive recs as (
  select
  tables.oid as table_oid,
  tables.oid as base_oid,
  tables.relname table_name,
  tables.relname base_name,
  1 as level
  from inserttables tables

  where not exists (
    select 1 from pg_catalog.pg_constraint nullconst
    join inserttables othtables on nullconst.confrelid = othtables.oid
    join pg_catalog.pg_attribute nullatt on
    nullatt.attnum = nullconst.confkey[1] and nullatt.attrelid = tables.oid
    and cardinality(nullconst.confkey) = 1 and nullatt.attname = 'id'
    where
    nullconst.conrelid = tables.oid and nullconst.contype = 'f'
    )

  union all
  select
  tables.oid,
  base_oid,
  tables.relname,
  base_name,
  recs.level + 1
  from pg_catalog.pg_constraint r
  join inserttables tables on tables.oid = r.conrelid
  join recs on recs.table_oid = r.confrelid
  join pg_catalog.pg_attribute att on att.attnum = r.conkey[1] and att.attrelid = tables.oid
  where r.contype = 'f' and cardinality(r.conkey) = 1 and att.attname = 'id'
  )
  select
         *,
         not coalesce(lead(base_oid, 1) over (partition by base_oid order by level) = base_oid, false) as endgroup,
         case when level = 1 then string_agg('''' || table_name || '''', ', ') over (partition by base_oid) end as linkedtables,
         row_number() over (order by base_oid, level) as rnum
  from recs
),

fields as (
  select
  oid,
  relname,
  attname,
  case
    when ismodule and istype then quote_literal('(select id from domain_object_type_id where lower(name) = ''ss_module'')')
    when ismoduletype and istype then quote_literal('(select id from domain_object_type_id where lower(name) = ''ss_moduletype'')')
    when ismodule and not istype then '''(select mod.id from ss_module mod join ss_moduletype mtype on mtype.id = mod.type where alias = ''|| ' ||
    'coalesce((select quote_literal(alias) from ss_module mod join ss_moduletype mtype on mtype.id = mod.type where mod.id = ' || attname::text || ' limit 1), ''null'') || '' order by mod.id limit 1)'''
    when ismoduletype and not istype then '''(select mtype.id from ss_moduletype mtype where alias = ''|| ' ||
    'coalesce((select quote_literal(alias) from ss_moduletype where id = ' || attname::text || '), ''null'') ||'')'''
    when coltype ~ '^(text|character varying)'
      then 'replace(replace(quote_nullable(' || attname::text || ')::text, chr(10), '''''' || chr(10) || ''''''), chr(13), '''''' || chr(13) || '''''')'
    when coltype ~ '^(int|bigint)' then 'coalesce(' || attname::text || '::text, ''null'')'
    else 'quote_nullable(' || attname::text || ')::text'
  end fld, attnum, coalesce(ismodule, false) or coalesce(ismoduletype, false) as isspecial, istype, ismaintype
  from (
    select
           tables.oid as oid, tables.relname as relname,
           case when attname::text like '%\_type' then
             substring(attname, 1, length(attname) - 5) = any(array_agg(attname::text) over (partition by tables.oid))
             else false
           end as istype,
           case when attname::text not like '%\_type' then
             attname || '_type' = any(array_agg(attname::text) over (partition by tables.oid))
             else false
           end as ismaintype,
           attnum, attname::text as attname, coltype, ismodule, ismoduletype
    from inserttables tables
    join pg_catalog.pg_attribute att on att.attrelid = tables.oid
    join lateral (select pg_catalog.format_type(att.atttypid, att.atttypmod) as coltype) t on true
    left join lateral (
    select (frel.relname = 'ss_module') as ismodule, (frel.relname = 'ss_moduletype') as ismoduletype
    from pg_catalog.pg_constraint const
    join pg_catalog.pg_class frel on confrelid = frel.oid
    where const.conrelid = tables.oid and attnum = any(conkey) and frel.relname in ('ss_module', 'ss_moduletype')
    ) t3 on true
    where attnum > 0 and not attisdropped and attname not in
    ('id', 'id_type', 'status', 'status_type', 'created_date', 'updated_date', 'created_by',
     'updated_by', 'created_by_type', 'updated_by_type', 'access_object_id')
  ) tmp
),

fieldsstr as (
  select oid, string_agg(fld, ' || '','' || ' order by attnum) as str
  from fields group by oid
),

uniqs as (
  with recursive rec as (
    select distinct rel.relname, rel.oid as oid, targetoid, attname, attnum, istype, ismaintype, linkedoid, conkey, 1 as level
    from pg_catalog.pg_constraint con
    join pg_catalog.pg_class rel on rel.oid = con.conrelid
    join hiersbase tmp on tmp.table_oid = rel.oid
    join hiersbase on hiersbase.base_oid = tmp.base_oid
    join inds on hiersbase.table_oid = inds.oid
    join fields on fields.oid = rel.oid and fields.attnum = any(conkey) and fields.attname != 'migrationid'
    left join lateral (
      select confrelid linkedoid from pg_catalog.pg_constraint const
      where conrelid = rel.oid and attnum = any(conkey) and contype = 'f'
      ) t on (istype or ismaintype) and not isspecial
    where contype = 'u'
    union
    select rel.relname, hiersbase.table_oid, targetoid, fields.attname, fields.attnum, fields.istype, fields.ismaintype, t.linkedoid, con.conkey, rec.level + 1
    from pg_catalog.pg_constraint con
    join pg_catalog.pg_class rel on rel.oid = con.conrelid
    join hiersbase tmp on tmp.table_oid = rel.oid
    join hiersbase on hiersbase.base_oid = tmp.base_oid
    join rec on hiersbase.table_oid = linkedoid
    join fields on fields.oid = rel.oid and fields.attnum = any(con.conkey) and fields.attname != 'migrationid'
    left join lateral (
      select confrelid linkedoid from pg_catalog.pg_constraint const
      where conrelid = rel.oid and fields.attnum = any(con.conkey) and contype = 'f'
      ) t on (fields.istype or fields.ismaintype) and not isspecial
    where contype = 'u'
  ),
  recstr as (
    select
      distinct 'select ''select id, id_type from ' || relname || ' where '' || ' || (string_agg('''' || attname || ' = '' || quote_literal(' || attname || ')', ' || '' and '' || ')
        over (partition by oid, conkey order by attnum rows between unbounded preceding and unbounded following)) || ' from ' || relname as str,
      level, oid, linkedoid, targetoid, relname
      from rec where level = (select max(level) from rec)
    union
    select
       'select ''' ||
       (case when rec.level = 1 then '%s' else '' end) ||
       'select id, id_type from ' || rec.relname || ' where '' || ' ||
       (string_agg('''' || case when rec.ismaintype and rec.linkedoid = recstr.oid then '(' || attname || ', ' || attname ||  '_type)' else attname end || ' = ' ||
          (case when rec.istype then null when rec.linkedoid = recstr.oid then ''' || ''('' || (' || recstr.str || ' where (id, id_type) = ' ||
           '(' || rec.relname || '.' || attname || ', ' || rec.relname || '.' || attname ||  '_type)) || '')'''
            else ''' || quote_literal(' || attname || ')' end), ' || '' and '' || ')
       over (partition by rec.oid, conkey order by attnum rows between unbounded preceding and unbounded following)) ||
       (case when rec.level = 1 then ' %s ' else '' end) ||
       ' from ' || rec.relname  as str,
           rec.level, rec.oid, rec.linkedoid, rec.targetoid, rec.relname
    from recstr join rec on rec.level = recstr.level - 1
  )
  select distinct format(str, 'update ' || inds.table_name || ' set (' || inds.othatts ||
    ') = (', '|| '') where (id, id_type) = ('' || depid || '', '' || depidtype || '')''') ||
  ' join lateral (select id depid, id_type as depidtype from ' || inds.table_name ||
  ' where (' || recstr.relname || '.id, ' || recstr.relname || '.id_type) = (' || inds.othattsprefix ||
  ')) tmp on true' as str, targetoid from recstr join inds using (targetoid) where level = 1
),

uniqblockbegin as (select str, row_number() over () as ord from (
  select 'select ' || quote_literal('create temporary table uniq_cmds
  (cmd text) on commit drop; ') as str
  union all
  select 'select ' || quote_literal('insert into uniq_cmds ' || str || ';') from uniqs
) t
),

uniqblockend as (
  select 'select ' ||
    quote_literal(
      'for trecord in select * from uniq_cmds where cmd is not null loop
        execute trecord.cmd;
      end loop;'
    ) as str
),

dropblockbegin as (select str, row_number() over () as ord from (
  select
  'select ' || quote_literal('create temporary table old_fkey
  (fkey text) on commit drop;
  for trecord in
  with tableslist as (select tname from unnest(string_to_array(''' || (select string_agg(relname, ',') from tables) || ''', '','')::text[]) as tname(tname))
  select
  conrelid::regclass::text table_name,
  conname constraint_name,
  pg_catalog.pg_get_constraintdef(r.oid, true) definition,
         (trg.tname is not null and notsrc.tname is null)
  from pg_catalog.pg_constraint r
  join pg_catalog.pg_class rel on conrelid = rel.oid
  join pg_catalog.pg_class frel on confrelid = frel.oid

  left join tableslist self on self.tname = rel.relname::text

  left join tableslist trg on trg.tname = frel.relname::text
  left join tableslist notsrc on notsrc.tname = rel.relname::text

  where
  r.contype = ''f''
  and (self.tname is not null or
        (trg.tname is not null and notsrc.tname is null))
  and exists (
    select 1 from pg_catalog.pg_attribute where attnum = any(conkey) and attrelid = rel.oid
    and attname !=
      all(array[''group_id'', ''id_type'', ''created_by'', ''created_by_type'', ''updated_by'', ''updated_by_type'', ''status'', ''status_type''])
  )
  loop
  insert into old_fkey values (
    format(''alter table %s add constraint %s %s'',
    quote_ident(trecord.table_name), quote_ident(trecord.constraint_name), trecord.definition
    ));
  execute format(''alter table %s drop constraint %s;'',
  quote_ident(trecord.table_name), quote_ident(trecord.constraint_name));
  end loop;') as str
  union all
  select 'select ' || quote_literal(format('delete from %s;',
  quote_ident(tables.relname))) from tables
) tt),

dropblockend as (
  select 'select ' ||
    quote_literal(
      'for trecord in select * from old_fkey loop
        execute trecord.fkey;
      end loop;'
    ) as str
),

startblock as (
  select str, row_number() over () as ord from (
    select 'select ' || quote_literal('do $$ declare ') as str
    union all
    select 'select ' || quote_literal(' curpersid bigint := (select id from person order by id limit 1);')
    union all
    select 'select ' || quote_literal(' curperstype int := (select id_type from person limit 1);')
    union all
    select 'select ' || quote_literal(' trecord record;')
    union all
    select 'select ' || quote_literal(' tmp int;')
    union all
    select 'select ' || quote_literal('begin')
  ) t
),

fixrefsblock as (
  select 'select ' ||
   quote_literal(format('update %s set (%s, %s) = (select dstid, dsttype from temp_ids_map where (srcid, srctype) = (%s, %s));'
    , relname, lname, attname, lname, attname
    )) str from fields
  join lateral (select substring(attname, 1, length(attname) - 5) as lname) t on true
  where istype and not isspecial
),

insertblock as
(select str, row_number() over () as ord from (
  select 'select ' || quote_literal('create temporary table temp_ids_map ' ||
  '(srcid bigint not null, srctype int not null, dstid bigint not null, dsttype int not null, primary key (srcid, srctype)) ' ||
  'on commit drop;') as str
  union all
  select * from (select str from hiersbase
        join lateral (
          with cntstr as (select '(select %s where (select count(*) from '|| table_name || ') != 0)' v)
          select 1 as ord, format((select v from cntstr),
            '''with type_map as (select id, substring(srctables.name, 1, 4)::int as origid, lower(dtid.name) as name from domain_object_type_id dtid join' ||
            ' unnest(string_to_array(''|| (select quote_literal(string_agg(id::text || name, '','')) from domain_object_type_id where lower(name) in (' ||
            linkedtables || ')) ||'', '''','''')) srctables(name) on substring(srctables.name, 5) = dtid.name), ''') str
          where level = 1
                union all
          select 3 as ord, format((select v from cntstr),
            '''cur_ids_map as (insert into temp_ids_map select substring(id, 5)::bigint, substring(id, 1, 4)::int, nextval((select id::text from type_map where name = ''''' || base_name || ''''') || ''''_sq''''), '
            || '(select id from type_map where origid = substring(tids.id, 1, 4)::int) from unnest(string_to_array('' || (select quote_literal(string_agg(id_type::text || id::text, '','')) from ' || base_name || ') || '','''','''')) as tids(id) '
            || ' returning *),''')
          where level = 1
                union all
          select 4, format((select v from cntstr), quote_literal('inst_' || table_name ||' as (insert into ' || table_name || ' values '))
                union all
          select 5, format((select v from cntstr), '(case when row_number() over () = 1 then '''' else '','' end) || ' ||
                '''((select dstid from cur_ids_map where srcid = ''|| id || ''), (select dsttype from cur_ids_map where srcid = ''|| id || '')'' || ' ||
                case when level = 1 then (''','' || ''now()'' || '','' || ''now()'' || ' ||
                ''', curpersid, curperstype, curpersid, curperstype, null, null, (select dstid from cur_ids_map where srcid = ''|| id || '')'' || ')
                else '' end || coalesce((select ''','' || ' || str || ' || ' from fieldsstr where oid = table_oid), '') ||
                ''')'' from ' || table_name)
          union all
          select 7, format((select v from cntstr), quote_literal(' returning 1),'))
          union all
          select 8, format('(select %s where (select count(*) from '|| base_name || ') != 0)', quote_literal('tmp_end as (select 1) select 1 into tmp;')) where endgroup
          ) tt on true
        order by rnum, ord
  ) t
) t2),
mainblock as (
  select case when row_number() over () = 1 then '' else ' union all ' end || str
  || case when row_number() over () = 1 then ' as str' else '' end from (
    select 0 as blockord, ord, str from startblock
    union all
    select 1, ord, str from uniqblockbegin
    union all
    select 2, ord, str from dropblockbegin
    union all
    select 3, ord, str from insertblock
    union all
    select 4, 0, str from fixrefsblock
    union all
    select 5, 0, str from uniqblockend
    union all
    select 6, 0, str from dropblockend
    union all
    select 999, 0, 'select ''end; $$'''
    order by blockord, ord
  ) tt
)
select * from mainblock
) t;


