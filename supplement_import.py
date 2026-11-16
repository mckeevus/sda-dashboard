"""Validated Supplement P ingestion, with semantic table detection and atomic writes."""
import collections
import contextlib
import datetime as dt
import gzip
import hashlib
import json
import math
import os
import re
import sqlite3
from pathlib import Path

import openpyxl

VERSION = 'supplement-p-2026-11-v1'
REPAIR_PAYLOAD_SHA256 = 'af0ad3a41b1be3aa4c6ee1be26d5298ae1aca58f41580606b9facf9155165e68'
STATES = {'ACT','NSW','NT','QLD','SA','TAS','VIC','WA','OT'}
TOTAL_PARTICIPANTS = 'Total participants with SDA need'
IN_USE = 'Participants with SDA in use'
ELIGIBLE = 'Participants SDA eligible, not yet using SDA'
DC_NAMES = {'improved liveability':'IL','high physical support':'HPS','robust':'Robust',
            'fully accessible':'FA','missing':'Missing','basic':'Basic'}


class ImportValidationError(ValueError):
    pass


def normalize(text):
    text = re.sub(r'specialist\s+disability\s+accommodation\s*\(sda\)', 'SDA', str(text), flags=re.I)
    text = re.sub(r'supported\s+independent\s+living\s*\(sil\)', 'SIL', text, flags=re.I)
    text = re.sub(r'building\s+type', 'build type', text, flags=re.I)
    return re.sub(r'\s+', ' ', text.replace('\xa0',' ')).strip().casefold()


def identify_title(title):
    # Titles, not table numbers, define concepts: older releases renumbered them.
    from load_supplement_p_v4 import SHEET_SIGNATURES
    text = normalize(title)
    if all(x in text for x in ['active participants','sda','sil','funding']):
        return 'Table P.1', 'SDA and SIL participants by service district'
    def contains(phrase):
        phrase=normalize(phrase)
        suffix='' if phrase.endswith('s') else 's?'
        return re.search(r'(?<!\w)'+re.escape(phrase)+suffix+r'(?!\w)',text) is not None
    for sig in SHEET_SIGNATURES:
        if all(contains(x) for x in sig['must_have']) and not any(contains(x) for x in sig['must_not']):
            return sig['canonical'], sig['desc']
    return None, None


def _sheet_number(text):
    match = re.match(r'^Table\s*(?:P\.)?\s*(\d+)\b', str(text), re.I)
    return match.group(1) if match else None


def detect_workbook(workbook):
    tables, warnings = {}, []
    for ws in workbook:
        if not _sheet_number(ws.title):
            continue
        candidates = [str(row[0]).strip() for row in ws.iter_rows(min_row=1,max_row=8,max_col=1,values_only=True)
                      if row[0] and len(str(row[0])) > 20]
        own = _sheet_number(ws.title)
        ranked = sorted(enumerate(candidates), key=lambda pair: (_sheet_number(pair[1]) != own, pair[0]))
        chosen = next(((title,*identify_title(title)) for _,title in ranked if identify_title(title)[0]),None)
        if not chosen:
            raise ImportValidationError(f'{ws.title}: unrecognised table title; no data was changed.')
        title, canonical, description = chosen
        if canonical in tables:
            raise ImportValidationError(f'Ambiguous mapping: {tables[canonical]["sheet"]} and {ws.title} both map to {canonical}.')
        tables[canonical] = {'sheet':ws.title,'title':title,'description':description}
        if candidates and chosen[0] != candidates[0]:
            warnings.append(f'{ws.title}: ignored an unrelated leading title and used the matching table title.')
    return tables, warnings


def detect_sheets(filepath):
    with contextlib.closing(openpyxl.load_workbook(filepath,read_only=True,data_only=True)) as wb:
        return {key:value['sheet'] for key,value in detect_workbook(wb)[0].items()}


def table_rows(ws):
    # Some releases format 16,383 empty columns. Read the actual contiguous header.
    for index,row in enumerate(ws.iter_rows(min_row=1,max_row=12,max_col=256,values_only=True),1):
        first = normalize(row[0] or '')
        if first not in {'sa4 region','sa3 region','state/territory','state','territory','service district','cohort'}:
            continue
        headers=[]
        for value in row:
            if value is None or not str(value).strip():break
            if re.fullmatch(r'Column\d+',str(value).strip()) and 'Total' in headers:break
            headers.append(str(value).strip())
        if len(headers)<2:continue
        if len(headers)==256:raise ImportValidationError(f'{ws.title}: header exceeds the supported column limit.')
        return headers, list(ws.iter_rows(min_row=index+1,max_col=len(headers),values_only=True))
    raise ImportValidationError(f'{ws.title}: could not find a supported geography header.')


def geography(value, level):
    if value is None:return None
    value=str(value).strip()
    if value.casefold() in {'total','national','australia'}:return 'Australia',None,'Australia'
    if value in STATES:return 'State',value,value
    if value.casefold()=='other territories':return 'State','OT','OT'
    if ' - ' in value:
        state,name=value.split(' - ',1)
        if state in STATES:return level,state,name.strip()
    if value.casefold() in {'other','missing','unknown'}:return 'Other',None,value
    return None


def numeric(value):
    if value is None or str(value).strip()=='':return None, None
    token=str(value).strip()
    if token.startswith('<') or token.casefold() in {'n/a','na','np','not published','..','-'}:
        return None,token
    try:
        number=float(token.replace('$','').replace(',',''))
    except (ValueError,TypeError):
        raise ImportValidationError(f'Unrecognised numeric value: {token!r}')
    if not math.isfinite(number):raise ImportValidationError(f'Non-finite numeric value: {token!r}')
    return number,None


def _column(headers,predicate):
    return next((i for i,h in enumerate(headers) if i and predicate(normalize(h))),None)


def parse_table(canonical,headers,rows):
    level='SA3' if canonical in {'Table P.13','Table P.14','Table P.15','Table P.17','Table P.18'} else 'SA4'
    if canonical=='Table P.1':level='Service District' if normalize(headers[0])=='service district' else 'SA4'
    if level=='SA3' and normalize(headers[0])!='sa3 region':
        raise ImportValidationError(f'{canonical}: expected SA3 geography, got {headers[0]!r}.')
    if canonical in {'Table P.9','Table P.17'}:
        columns={}
        for i,h in enumerate(headers[1:],1):
            text=normalize(h)
            if text.startswith('total') and 'sda' in text:columns[i]=TOTAL_PARTICIPANTS
            elif 'with sda in use' in text:columns[i]=IN_USE
            elif 'sda eligible' in text:columns[i]=ELIGIBLE
        if TOTAL_PARTICIPANTS not in columns.values():raise ImportValidationError(f'{canonical}: total participant column is missing.')
    elif canonical in {'Table P.10','Table P.18'}:
        columns={}
        for i,h in enumerate(headers[1:],1):
            text=normalize(h)
            if text.startswith('total') and 'sda' in text:columns[i]='DC: Total'
            else:
                for name,key in DC_NAMES.items():
                    if name in text:columns[i]='DC: '+key;break
        if 'DC: Total' not in columns.values() or len(columns)<2:raise ImportValidationError(f'{canonical}: design-category columns are incomplete.')
    elif canonical=='Table P.3':
        columns={i:'Active providers' for i,h in enumerate(headers[1:],1)
                 if 'provider' in normalize(h) and 'ever' not in normalize(h)}
        if len(columns)!=1:raise ImportValidationError('P.3: expected one current active-provider column.')
    elif canonical=='Table P.2':
        columns={}
        for i,h in enumerate(headers[1:],1):
            text=normalize(h)
            if 'percentage' in text or '%' in text:continue
            if text.startswith('total'):columns[i]='Total Committed ($)'
            elif 'sda' in text:columns[i]='SDA Committed ($)'
            elif 'sil' in text:columns[i]='SIL Committed ($)'
        if len(columns)!=3:raise ImportValidationError('P.2: expected SDA, SIL and total dollar columns.')
    elif canonical=='Table P.1':
        columns={i:h for i,h in enumerate(headers[1:],1) if 'percentage' not in normalize(h) and '%' not in h}
    else:
        columns={i:h for i,h in enumerate(headers[1:],1)}
        if 'Total' not in columns.values():raise ImportValidationError(f'{canonical}: Total column is missing.')
        if canonical in {'Table P.11','Table P.12','Table P.16'} and not any(',' in h for h in columns.values()):
            raise ImportValidationError(f'{canonical}: detailed building-type columns are missing.')
    records, suppressed = [], []
    keys=set();seen_states=set()
    for row in rows:
        geo=geography(row[0],level)
        if not geo:continue  # Source notes and navigation links are not data.
        # P.1 lists the ACT state total and its single ACT service district with
        # the same visible label. Preserve the two geographic levels separately.
        if level=='Service District' and geo[0]=='State':
            if geo[1] in seen_states:geo=('Service District',geo[1],geo[2])
            else:seen_states.add(geo[1])
        geo_type,state,name=geo
        for index,measure in columns.items():
            if row[index] is None or str(row[index]).strip()=='':continue
            value,token=numeric(row[index])
            key=(geo_type,state,name,measure)
            if key in keys:raise ImportValidationError(f'{canonical}: duplicate geography/measure {key}.')
            keys.add(key)
            records.append([geo_type,state,name,measure,value])
            if token:suppressed.append([geo_type,state,name,measure,token])
    if not records:raise ImportValidationError(f'{canonical}: no usable records.')
    if not any(r[0]=='Australia' for r in records):raise ImportValidationError(f'{canonical}: national row is missing.')
    return records,suppressed


def validate_tables(tables):
    lookup={}
    for table in tables:
        source=table['canonical'];groups=collections.defaultdict(dict)
        for geo,state,name,measure,value in table['records']:
            lookup[source,geo,state,name,measure]=value
            groups[geo,state,name][measure]=value
        if source in {'Table P.1','Table P.2','Table P.3'}:continue
        total_name=TOTAL_PARTICIPANTS if source in {'Table P.9','Table P.17'} else 'DC: Total' if source in {'Table P.10','Table P.18'} else 'Total'
        for geo,values in groups.items():
            if total_name not in values:raise ImportValidationError(f'{source}: missing total for {geo}.')
            components=[v for k,v in values.items() if k!=total_name]
            if components and all(v is not None for v in components) and values[total_name] is not None:
                if not math.isclose(sum(components),values[total_name],abs_tol=0.05):
                    raise ImportValidationError(f'{source}: components do not match the published total for {geo}.')
    def get(table,measure='Total',geo=('Australia',None,'Australia')):
        return lookup.get((table,*geo,measure))
    for geo in {(r[0],r[1],r[2]) for t in tables for r in t['records'] if r[0] in {'Australia','State'}}:
        for group in [('Table P.4','Table P.5','Table P.6','Table P.13','Table P.14','Table P.15'),('Table P.8','Table P.16')]:
            values=[get(t,geo=geo) for t in group if get(t,geo=geo) is not None]
            if values and max(values)!=min(values):raise ImportValidationError(f'Cross-table totals disagree for {group}, {geo}.')
        for detail,parts in [('Table P.11',['New Build','New Build (refurbished)']),('Table P.12',['Existing','Legacy'])]:
            actual=get(detail,geo=geo);expected=[get('Table P.4',measure,geo) for measure in parts]
            if actual is not None and all(v is not None for v in expected) and actual!=sum(expected):
                raise ImportValidationError(f'{detail} does not match stock types for {geo}.')
        participant=[get('Table P.9',TOTAL_PARTICIPANTS,geo),get('Table P.17',TOTAL_PARTICIPANTS,geo),
                     get('Table P.10','DC: Total',geo),get('Table P.18','DC: Total',geo)]
        participant=[v for v in participant if v is not None]
        if participant and max(participant)!=min(participant):raise ImportValidationError(f'Participant tables disagree for {geo}.')
    return lookup


def _title_quarter(title):
    match=re.search(r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d\d)',title,re.I)
    if match:
        date=dt.datetime.strptime(' '.join(match.groups()).title(),'%d %B %Y')
        return f'{date.year}-Q{(date.month-1)//3+1}'
    match=re.search(r'quarter\s*([1-4])\s*,\s*(20\d\d)-(\d{2})',title,re.I)
    if match:
        q,start,_=match.groups();start=int(start)
        return {1:f'{start}-Q3',2:f'{start}-Q4',3:f'{start+1}-Q1',4:f'{start+1}-Q2'}[int(q)]
    return None


def prepare_workbook(filepath,quarter):
    if not re.fullmatch(r'\d{4}-Q[1-4]',quarter):raise ImportValidationError('Expected calendar quarter YYYY-QN.')
    with contextlib.closing(openpyxl.load_workbook(filepath,read_only=True,data_only=True)) as wb:
        mapping,warnings=detect_workbook(wb)
        if not mapping:raise ImportValidationError('No Supplement P tables were recognised.')
        dated=collections.Counter(_title_quarter(m['title']) for key,m in mapping.items() if key!='Table P.3')
        dated.pop(None,None)
        if dated and dated.most_common(1)[0][0]!=quarter:
            raise ImportValidationError(f'Workbook dates identify {dated.most_common(1)[0][0]}, not selected quarter {quarter}.')
        if quarter>='2024-Q4':
            expected={f'Table P.{i}' for i in range(1,19)}
            missing=expected-set(mapping)
            if missing:raise ImportValidationError('Incomplete release: missing mappings for '+', '.join(sorted(missing))+'. No data was changed.')
        tables=[]
        for canonical,info in sorted(mapping.items(),key=lambda p:int(p[0].split('.')[-1])):
            headers,rows=table_rows(wb[info['sheet']])
            records,suppressed=parse_table(canonical,headers,rows)
            if quarter>='2024-Q4':
                present={r[1] for r in records if r[0]=='State'}
                missing=(STATES-{'OT'})-present
                if missing:raise ImportValidationError(f'{canonical}: missing state/territory coverage: {", ".join(sorted(missing))}.')
                measures={r[3] for r in records}
                if canonical in {'Table P.9','Table P.17'} and not {IN_USE,ELIGIBLE,TOTAL_PARTICIPANTS}<=measures:
                    raise ImportValidationError(f'{canonical}: modern participant cohort columns are incomplete.')
            table={'canonical':canonical,**info,'headers':headers,'records':records,'suppressed':suppressed}
            if canonical=='Table P.3':
                table['title_quarter']=_title_quarter(info['title'])
                if table['title_quarter'] and table['title_quarter']!=quarter:
                    warnings.append(f'Provider source title refers to {table["title_quarter"]}, while this workbook is {quarter}; provider counts are associated with this release and the source-date discrepancy is retained.')
            if canonical=='Table P.16':
                table['notes']=[str(r[0]) for r in rows if r[0] and str(r[0]).startswith(('SDA pipeline data','The NDIA assesses'))]
            tables.append(table)
    validate_tables(tables)
    return {'schema_version':1,'parser_version':VERSION,'quarter':quarter,'workbook_sha256':hashlib.sha256(Path(filepath).read_bytes()).hexdigest(),
            'source_filename':Path(filepath).name,'tables':tables,'warnings':warnings}


def ensure_audit_schema(conn):
    conn.execute('CREATE TABLE IF NOT EXISTS import_runs (run_id INTEGER PRIMARY KEY, quarter TEXT NOT NULL, workbook_sha256 TEXT NOT NULL, parser_version TEXT NOT NULL, imported_at TEXT NOT NULL, report_json TEXT NOT NULL)')
    conn.execute('CREATE TABLE IF NOT EXISTS import_suppressed (quarter_id INTEGER, dataset_id INTEGER, geo_id INTEGER, measure_name TEXT, source_token TEXT, PRIMARY KEY(quarter_id,dataset_id,geo_id,measure_name))')
    conn.execute('CREATE TABLE IF NOT EXISTS applied_data_repairs (repair_id TEXT PRIMARY KEY, applied_at TEXT NOT NULL, detail_json TEXT NOT NULL)')


def backup_database(conn,db_path,tag):
    folder=Path(str(db_path)+'.backups');folder.mkdir(parents=True,exist_ok=True)
    path=folder/(dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'-'+tag+'.db')
    with sqlite3.connect(path) as backup:conn.backup(backup)
    return str(path)


def write_prepared(conn,prepared,mode):
    quarter=prepared['quarter']
    if mode not in {'new','replace'}:raise ImportValidationError('Mode must be new or replace.')
    ensure_audit_schema(conn)
    row=conn.execute('SELECT quarter_id FROM quarters WHERE quarter_str=?',(quarter,)).fetchone()
    if row and mode=='new':raise ImportValidationError(f'{quarter} already exists; select replace to update it.')
    if row:q_id=row[0]
    else:
        year,q=quarter.split('-Q')
        q_id=conn.execute('INSERT INTO quarters(year,quarter_num,quarter_str) VALUES(?,?,?)',(int(year),int(q),quarter)).lastrowid
    conn.execute('DELETE FROM import_suppressed WHERE quarter_id=?',(q_id,))
    conn.execute('DELETE FROM measures WHERE quarter_id=?',(q_id,))
    conn.execute('DELETE FROM sda_capacity WHERE quarter=?',(quarter,))
    geo_cache={};count=0;report=[]
    for table in prepared['tables']:
        name=table['canonical']
        conn.execute('INSERT OR IGNORE INTO datasets(source_table,description) VALUES(?,?)',(name,table['description']))
        dataset_id=conn.execute('SELECT dataset_id FROM datasets WHERE source_table=?',(name,)).fetchone()[0]
        for geo,state,region,measure,value in table['records']:
            key=(geo,state,region)
            if key not in geo_cache:
                existing=conn.execute('SELECT geo_id FROM geographies WHERE geo_type=? AND state IS ? AND name=?',(geo,state,region)).fetchone()
                if not existing:
                    parent=conn.execute("SELECT geo_id FROM geographies WHERE geo_type='State' AND state=? LIMIT 1",(state,)).fetchone() if state and geo not in {'State','Australia'} else None
                    geo_cache[key]=conn.execute('INSERT INTO geographies(geo_type,name,state,parent_id) VALUES(?,?,?,?)',(geo,region,state,parent[0] if parent else None)).lastrowid
                else:geo_cache[key]=existing[0]
            conn.execute('INSERT INTO measures(quarter_id,dataset_id,geo_id,measure_name,value) VALUES(?,?,?,?,?)',(q_id,dataset_id,geo_cache[key],measure,value));count+=1
        for geo,state,region,measure,token in table['suppressed']:
            conn.execute('INSERT INTO import_suppressed VALUES(?,?,?,?,?)',(q_id,dataset_id,geo_cache[geo,state,region],measure,token))
        national={r[3]:r[4] for r in table['records'] if r[0]=='Australia'}
        report.append({k:table[k] for k in ['canonical','sheet','title']}|{'rows':len(table['records']),'suppressed_cells':len(table['suppressed']),'national':national,'status':'validated',
                      **({'title_quarter':table['title_quarter']} if 'title_quarter' in table else {}),**({'notes':table['notes']} if 'notes' in table else {})})
        if name=='Table P.6':
            places=sum(national.get(label,0)*n for label,n in [('1 Resident',1),('2 Residents',2),('3 Residents',3),('4 Residents',4),('5 Residents',5),('6+ Residents',6)])
            conn.execute('INSERT INTO sda_capacity VALUES(?,?,?)',(quarter,int(national['Total']),int(places)))
    result={'success':True,'quarter':quarter,'rows_loaded':count,'parser_version':VERSION,'tables':report,'warnings':prepared['warnings'],
            'workbook_sha256':prepared['workbook_sha256'],'source_filename':prepared['source_filename'],
            'message':f'Validated and loaded {len(report)} tables ({count:,} measures) for {quarter}.',
            'timestamp':dt.datetime.now(dt.timezone.utc).isoformat()}
    conn.execute('INSERT INTO import_runs(quarter,workbook_sha256,parser_version,imported_at,report_json) VALUES(?,?,?,?,?)',
                 (quarter,prepared['workbook_sha256'],VERSION,result['timestamp'],json.dumps(result)))
    actual=conn.execute('SELECT COUNT(*) FROM measures WHERE quarter_id=?',(q_id,)).fetchone()[0]
    if actual!=count:raise ImportValidationError(f'Post-write row count mismatch: expected {count}, found {actual}.')
    return result


def run_loader(filepath,quarter_str,mode,db_path=None,source_filename=None):
    db_path=db_path or os.environ.get('DB_PATH',str(Path(__file__).with_name('sda_dashboard_clean.db')))
    try:
        if not Path(db_path).is_file():raise ImportValidationError('Database not found.')
        # Parse and reconcile the entire workbook before opening a write transaction.
        prepared=prepare_workbook(filepath,quarter_str)
        if source_filename:prepared['source_filename']=Path(source_filename).name
        with sqlite3.connect(db_path,timeout=60) as conn:
            backup_database(conn,db_path,quarter_str)
            conn.execute('BEGIN IMMEDIATE')
            result=write_prepared(conn,prepared,mode)
        return result
    except Exception as exc:
        return {'success':False,'quarter':quarter_str,'rows_loaded':0,'message':str(exc),'tables':[],
                'warnings':[],'parser_version':VERSION}


def repair_sa3_geographies(conn):
    """Correct geography metadata for SA3 tables without changing measure values."""
    rows=conn.execute("""SELECT m.measure_id,m.quarter_id,m.dataset_id,m.measure_name,m.value,
        g.name,g.state FROM measures m JOIN datasets d USING(dataset_id)
        JOIN geographies g USING(geo_id)
        WHERE d.source_table IN ('Table P.13','Table P.14','Table P.15') AND g.geo_type='SA4'""").fetchall()
    old_ids={r[0] for r in conn.execute("""SELECT DISTINCT g.geo_id FROM measures m JOIN datasets d USING(dataset_id)
        JOIN geographies g USING(geo_id) WHERE d.source_table IN ('Table P.13','Table P.14','Table P.15') AND g.geo_type='SA4'""")}
    mapped={};merged=0
    for measure_id,quarter_id,dataset_id,measure,value,name,state in rows:
        key=(name,state)
        if key not in mapped:
            geo=conn.execute("SELECT geo_id FROM geographies WHERE geo_type='SA3' AND name=? AND state IS ?",key).fetchone()
            if not geo:
                parent=conn.execute("SELECT geo_id FROM geographies WHERE geo_type='State' AND state=? LIMIT 1",(state,)).fetchone()
                mapped[key]=conn.execute("INSERT INTO geographies(geo_type,name,state,parent_id) VALUES('SA3',?,?,?)",(name,state,parent[0] if parent else None)).lastrowid
            else:mapped[key]=geo[0]
        target=mapped[key]
        duplicate=conn.execute('SELECT value FROM measures WHERE quarter_id=? AND dataset_id=? AND geo_id=? AND measure_name=?',
            (quarter_id,dataset_id,target,measure)).fetchone()
        if duplicate:
            if duplicate[0]!=value:raise ImportValidationError(f'Conflicting SA3 records for {name}, {measure}; repair rolled back.')
            conn.execute('DELETE FROM measures WHERE measure_id=?',(measure_id,));merged+=1
        else:conn.execute('UPDATE measures SET geo_id=? WHERE measure_id=?',(target,measure_id))
    removed=0
    for old in old_ids:
        removed+=conn.execute('DELETE FROM geographies WHERE geo_id=? AND NOT EXISTS(SELECT 1 FROM measures WHERE geo_id=?) AND NOT EXISTS(SELECT 1 FROM geographies WHERE parent_id=?)',(old,old,old)).rowcount
    return {'measures_reclassified':len(rows),'identical_duplicates_merged':merged,'geographies':len(mapped),'orphan_geographies_removed':removed}


def apply_bundled_repairs(db_path):
    """One-time data migrations: backup first, all changes in one transaction."""
    payload_path=Path(__file__).parent/'data'/'repairs'/'2026-Q3.json.gz'
    repair_id='supplement-2026-q3-mapping-v1'
    geo_id='supplement-sa3-geography-v1'
    with sqlite3.connect(db_path,timeout=120) as conn:
        ensure_audit_schema(conn);conn.commit()
        done={r[0] for r in conn.execute('SELECT repair_id FROM applied_data_repairs')}
        has_quarter=conn.execute("SELECT 1 FROM quarters WHERE quarter_str='2026-Q3'").fetchone() is not None
        pending=geo_id not in done or (has_quarter and repair_id not in done)
        if not pending:return []
        payload=None
        if has_quarter and repair_id not in done:
            content=payload_path.read_bytes()
            digest=hashlib.sha256(content).hexdigest()
            if digest!=REPAIR_PAYLOAD_SHA256:raise ImportValidationError('Repair payload checksum does not match; no data changed.')
            payload=json.loads(gzip.decompress(content));validate_tables(payload['tables'])
            if payload['quarter']!='2026-Q3' or len(payload['tables'])!=18:raise ImportValidationError('Invalid repair payload.')
        backup_database(conn,db_path,'before-supplement-repairs')
        conn.execute('BEGIN IMMEDIATE')
        # Re-check under the write lock, allowing safe starts with multiple workers.
        done={r[0] for r in conn.execute('SELECT repair_id FROM applied_data_repairs')}
        results=[]
        if geo_id not in done:
            detail=repair_sa3_geographies(conn)
            conn.execute('INSERT INTO applied_data_repairs VALUES(?,?,?)',(geo_id,dt.datetime.now(dt.timezone.utc).isoformat(),json.dumps(detail)))
            results.append({'repair':geo_id,**detail})
        if payload and repair_id not in done:
            current=conn.execute("SELECT COUNT(*) FROM measures m JOIN quarters q USING(quarter_id) WHERE q.quarter_str='2026-Q3'").fetchone()[0]
            expected_count=sum(len(t['records']) for t in payload['tables'])
            if current not in {20969,expected_count}:
                raise ImportValidationError('September data changed since the audited upload; automatic replacement was stopped.')
            total=conn.execute("""SELECT SUM(m.value) FROM measures m JOIN quarters q USING(quarter_id)
                JOIN datasets d USING(dataset_id) JOIN geographies g USING(geo_id)
                WHERE q.quarter_str='2026-Q3' AND d.source_table='Table P.4' AND g.geo_type='Australia' AND m.measure_name='Total'""").fetchone()[0]
            if total!=14537:raise ImportValidationError('September enrolled stock differs from the audited workbook; repair stopped.')
            result=write_prepared(conn,payload,'replace')
            conn.execute('INSERT INTO applied_data_repairs VALUES(?,?,?)',(repair_id,result['timestamp'],json.dumps({'rows':result['rows_loaded'],'workbook_sha256':payload['workbook_sha256']})))
            results.append({'repair':repair_id,'rows_loaded':result['rows_loaded']})
        return results
