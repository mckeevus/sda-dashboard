"""Versioned SDA aggregate cubes. Each fact stays at its published grain."""
import csv
import gzip
import hashlib
import io
import json
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from flask import Blueprint, jsonify, request, Response, send_from_directory

COMMON = {'State': 'state', 'SA4Rgn': 'sa4', 'SA3Rgn': 'sa3', 'RmtnsDsc': 'remoteness', 'SDASts': 'sda_status'}
DEFINITIONS = {
    'demand_usage': {'name': 'Participant need and usage', 'unit': 'Participants', 'measure': 'PrtcpntCnt',
        'fields': dict(COMMON, SDAElgblDsgnCatgry='design_category', SDAElgblBldTyp='building_type'),
        'note': 'Participant snapshots, not individual transitions or dwelling occupancy. Eligibility is not a count of vacant dwellings.'},
    'characteristics': {'name': 'Participant characteristics', 'unit': 'Participants', 'measure': 'PrtcpntCnt',
        'fields': {'State': 'state', 'AgeBnd': 'age_band', 'DsbltyGrp': 'disability_group', 'SDASts': 'sda_status'},
        'note': 'State-level age and disability only. Source age bands are retained. Suppressed or unpublished contributions never become zero.'},
    'demographics': {'name': 'Participant demographics', 'unit': 'Participants', 'measure': 'PrtcpntCnt',
        'fields': {'State': 'state', 'RmtnsDsc': 'remoteness', 'FrstNtnsSts': 'first_nations', 'SDASts': 'sda_status', 'SILSts': 'sil_status'},
        'note': 'Published First Nations and remoteness categories. SIL values retain source codes pending definitions. Counts are not population prevalence.'},
    'enrolled': {'name': 'Enrolled dwellings', 'unit': 'Dwellings', 'measure': 'DwellingCnt',
        'fields': dict({k:v for k,v in COMMON.items() if k != 'SDASts'}, DsnCtgry='design_category', BldngTyp='building_type', MxRsdnts='max_residents', SDATyp='sda_type', DwlgnAgeBnd='enrolment_band', EnrlmntYr='enrolment_year', EnrldLastQtr='new_last_quarter'),
        'note': 'Net stock growth and the enrolled-last-quarter flag are different measures. Maximum residents is capacity, not occupancy. Enrolment vintage is not building age.'},
    'pipeline': {'name': 'Pipeline dwellings', 'unit': 'Dwellings', 'measure': 'DwellingCnt',
        'fields': dict({k:v for k,v in COMMON.items() if k != 'SDASts'}, DsnCtgry='design_category', BldngTyp='building_type', MxRsdnts='max_residents', Rms='rooms', EntrdPplnYear='entry_year', NewLastQtr='new_last_quarter'),
        'note': 'Reported pipeline is not confirmed future supply. Entry year is not a completion date; year-only records do not establish exact months in pipeline. Room codes retain their published meaning.'},
}
LABELS = {'report_date': 'Quarter end', 'state': 'State / territory', 'sa4': 'SA4 region', 'sa3': 'SA3 region',
    'remoteness': 'Published remoteness category', 'sda_status': 'SDA status', 'design_category': 'Design category',
    'building_type': 'Detailed building configuration', 'building_group': 'Building type', 'max_residents': 'Maximum residents',
    'sda_type': 'SDA type', 'enrolment_band': 'Published enrolment band', 'enrolment_year': 'Enrolment year',
    'new_last_quarter': 'Flagged new last quarter (source code)', 'rooms': 'Rooms (source code)', 'entry_year': 'Recorded entry year',
    'entry_cohort': 'Recorded entry year (reviewed)', 'age_band': 'Published age band', 'disability_group': 'Disability group',
    'first_nations': 'Published First Nations status', 'sil_status': 'SIL status (source code)'}
STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA', 'Missing']
AGE_BANDS = ['Under 18', '19 to 34', '35 to 64', '65+']
ROOT = Path(__file__).resolve().parent


def dimensions(cube):
    result = ['report_date'] + list(DEFINITIONS[cube]['fields'].values())
    if 'building_type' in result: result.append('building_group')
    if cube == 'pipeline': result.append('entry_cohort')
    return result


def canonical(field, value):
    value = str(value).strip() or 'Not published'
    if field == 'building_type':
        value = re.sub(r'\s*/\s*', '/', value)
        value = re.sub(r'\b(\d+)\s+(resident|bedroom)s?\b', lambda m: m[1]+' '+m[2]+('s' if m[1]!='1' else ''), value, flags=re.I)
    if field == 'design_category' and value == 'Multi Design Category': value = 'Multi-Design Category'
    return value


def ordered(values, dimension):
    order = STATES if dimension == 'state' else AGE_BANDS if dimension == 'age_band' else []
    return sorted(values, key=lambda v: (order.index(v) if v in order else len(order), str(v))) if order else sorted(values)


def create_schema(conn):
    conn.executescript('''CREATE TABLE IF NOT EXISTS cube_releases (
        release_id TEXT PRIMARY KEY, label TEXT NOT NULL, ingested_at TEXT NOT NULL, manifest_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS cube_sources (
        release_id TEXT NOT NULL, cube TEXT NOT NULL, filename TEXT NOT NULL, sha256 TEXT NOT NULL,
        row_count INTEGER NOT NULL, suppressed_cells INTEGER NOT NULL, dates_json TEXT NOT NULL,
        PRIMARY KEY(release_id,cube));
        CREATE TABLE IF NOT EXISTS cube_raw_files (release_id TEXT NOT NULL, cube TEXT NOT NULL, raw_gzip BLOB NOT NULL, PRIMARY KEY(release_id,cube));''')
    for cube in DEFINITIONS:
        cols = ','.join(f'"{d}" TEXT NOT NULL' for d in dimensions(cube))
        conn.execute(f'''CREATE TABLE IF NOT EXISTS cube_{cube} (
            release_id TEXT NOT NULL, cell_key TEXT NOT NULL, {cols}, quarter_str TEXT NOT NULL,
            value INTEGER, value_status TEXT NOT NULL, raw_value TEXT NOT NULL, source_dimensions TEXT NOT NULL,
            PRIMARY KEY(release_id,cell_key))''')
        conn.execute(f'CREATE INDEX IF NOT EXISTS cube_{cube}_scope ON cube_{cube}(release_id,quarter_str,state)')


def prepare_file(cube, raw, expected_hash=None):
    definition = DEFINITIONS[cube]
    sha = hashlib.sha256(raw).hexdigest()
    if expected_hash and expected_hash != sha: raise ValueError(f'{cube}: file checksum does not match the release manifest')
    reader = csv.DictReader(io.StringIO(raw.decode('utf-8-sig'), newline=''))
    required = {'RprtDt', definition['measure'], *definition['fields']}
    if not reader.fieldnames or len(reader.fieldnames)!=len(required) or set(reader.fieldnames) != required:
        raise ValueError(f'{cube}: unexpected CSV columns; expected '+', '.join(sorted(required)))
    records=[]; seen=set(); dates=set(); suppressed=0
    for line, row in enumerate(reader, 2):
        try:
            date=datetime.strptime(row['RprtDt'], '%d%b%Y').date()
            if date.month not in (3,6,9,12) or date.day != (30 if date.month in (6,9) else 31): raise ValueError('not a quarter-end date')
            raw_dims={key: row[key] for key in ['RprtDt', *definition['fields']]}
            key=hashlib.sha256(json.dumps(raw_dims, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            if key in seen: raise ValueError('duplicate published dimension key')
            seen.add(key); dates.add(date.isoformat())
            values={field:canonical(field,row[source]) for source,field in definition['fields'].items()}
            if values['state'] not in STATES: raise ValueError('unrecognised state / territory')
            values['report_date']=date.isoformat()
            if 'building_type' in values: values['building_group']=values['building_type'].split(',')[0]
            if cube == 'pipeline':
                year=values['entry_year']
                values['entry_cohort']=year if re.fullmatch(r'20\d{2}',year) and int(year)<=date.year else 'Unverified entry year'
            if 'new_last_quarter' in values and values['new_last_quarter'] not in ('0','1'): raise ValueError('unexpected last-quarter flag')
            token=row[definition['measure']].strip()
            if token=='<5': value=None; status='suppressed'; suppressed+=1
            elif token in ('', '.', '-'): value=None; status='not_published'
            elif re.fullmatch(r'\d+',token): value=int(token); status='exact'
            else: raise ValueError('invalid count or suppression token')
            records.append((key, *(values[d] for d in dimensions(cube)), f'{date.year}-Q{(date.month+2)//3}', value,status,token,json.dumps(raw_dims,ensure_ascii=False)))
        except (ValueError, TypeError) as exc: raise ValueError(f'{cube}, CSV row {line}: {exc}') from exc
    if not records: raise ValueError(f'{cube}: empty cube')
    return records, {'sha256':sha, 'rows':len(records), 'suppressed_cells':suppressed, 'dates':sorted(dates)}


def install_release(db_path, manifest, files):
    """Validate all five cubes before one atomic insert; never replace historical facts."""
    if set(files) != set(DEFINITIONS): raise ValueError('A cube release must include all five files')
    release_id=manifest['release_id']
    if not re.fullmatch(r'[a-zA-Z0-9_.-]{1,100}',release_id): raise ValueError('Invalid release identifier')
    prepared={cube:prepare_file(cube, files[cube], manifest['files'][cube]['sha256']) for cube in DEFINITIONS}
    canonical_manifest=json.dumps(manifest,sort_keys=True)
    with closing(sqlite3.connect(db_path,timeout=60)) as conn, conn:
        create_schema(conn)
        conn.execute('BEGIN IMMEDIATE')
        existing=conn.execute('SELECT manifest_json FROM cube_releases WHERE release_id=?',(release_id,)).fetchone()
        if existing:
            if json.loads(existing[0]) != manifest: raise ValueError('Release ID already exists with different content; use a new release ID')
            return False
        conn.execute('INSERT INTO cube_releases VALUES (?,?,?,?)',(release_id,manifest['label'],datetime.now(timezone.utc).isoformat(),canonical_manifest))
        for cube,(records,info) in prepared.items():
            columns=['release_id','cell_key',*dimensions(cube),'quarter_str','value','value_status','raw_value','source_dimensions']
            conn.executemany(f'INSERT INTO cube_{cube} ('+','.join(f'"{c}"' for c in columns)+') VALUES ('+','.join('?' for _ in columns)+')',[(release_id,*r) for r in records])
            conn.execute('INSERT INTO cube_raw_files VALUES (?,?,?)',(release_id,cube,gzip.compress(files[cube],mtime=0)))
            conn.execute('INSERT INTO cube_sources VALUES (?,?,?,?,?,?,?)',(release_id,cube,manifest['files'][cube]['filename'],info['sha256'],info['rows'],info['suppressed_cells'],json.dumps(info['dates'])))
    return True


def install_bundled(db_path):
    folder=ROOT/'data/cubes'
    manifest_path=folder/'manifest.json'
    with closing(sqlite3.connect(db_path,timeout=60)) as conn, conn: create_schema(conn)
    if not manifest_path.exists(): return
    manifest=json.loads(manifest_path.read_text())
    with closing(sqlite3.connect(db_path,timeout=60)) as conn, conn:
        existing=conn.execute('SELECT manifest_json FROM cube_releases WHERE release_id=?',(manifest['release_id'],)).fetchone()
    if existing and json.loads(existing[0]) == manifest: return
    files={cube:gzip.decompress((folder/file['path']).read_bytes()) for cube,file in manifest['files'].items()}
    install_release(db_path,manifest,files)


def selected_release(conn, release_id=None):
    if release_id:
        row=conn.execute('SELECT release_id FROM cube_releases WHERE release_id=?',(release_id,)).fetchone()
    else: row=conn.execute('SELECT release_id FROM cube_releases ORDER BY ingested_at DESC,release_id DESC LIMIT 1').fetchone()
    if not row: raise ValueError('Cube release is unavailable')
    return row[0]


def query_cube(conn, cube, params):
    if cube not in DEFINITIONS: raise ValueError('Unknown data cube')
    if not isinstance(params,dict): raise ValueError('Expected a JSON object')
    release=selected_release(conn,params.get('release_id'))
    dims=dimensions(cube); group=params.get('group_by', ['state'])
    if not isinstance(group,list) or not 1<=len(group)<=2 or any(not isinstance(d,str) or d not in dims for d in group) or len(set(group))!=len(group):
        raise ValueError('Choose one or two supported dimensions')
    filters=params.get('filters',{})
    if not isinstance(filters,dict) or any(d not in dims or d=='report_date' for d in filters): raise ValueError('Unsupported cube filter')
    quarter=params.get('quarter')
    if not isinstance(quarter,str) or not re.fullmatch(r'20\d{2}-Q[1-4]',quarter): raise ValueError('Choose a calendar reporting quarter')
    where=['release_id=?']; args=[release]
    if 'report_date' in group: where.append('quarter_str<=?')
    else: where.append('quarter_str=?')
    args.append(quarter)
    for dim, raw in filters.items():
        values=raw if isinstance(raw,list) else [raw]
        if not values or len(values)>500 or any(not isinstance(v,str) or len(v)>300 for v in values): raise ValueError('Invalid filter values')
        where.append(f'"{dim}" IN ('+','.join('?' for _ in values)+')'); args.extend(values)
    needs_sil=cube=='demographics' and 'sil_status' not in group and 'sil_status' not in filters
    incomplete_sil=" + CASE WHEN COUNT(DISTINCT sil_status)<2 THEN 1 ELSE 0 END" if needs_sil else ''
    key_cols=','.join(f'"{d}"' for d in group)
    rows=conn.execute(f'''SELECT {key_cols}, SUM(value) AS known_value,
        SUM(value_status!='exact'){incomplete_sil} AS unavailable, SUM(value_status='suppressed') AS suppressed,
        COUNT(*) AS contributing_cells FROM cube_{cube} WHERE '''+' AND '.join(where)+f' GROUP BY {key_cols} ORDER BY {key_cols}',args).fetchall()
    if len(rows)>5000: raise ValueError('Selection is too large; choose a narrower geography or dimension')
    result=[]
    for row in rows:
        unavailable=row['unavailable']; value=row['known_value'] if not unavailable else None
        result.append({**{d:row[d] for d in group},'value':value,'value_status':'suppressed' if row['suppressed'] else 'not_published' if unavailable else 'exact',
            'contributing_cells':row['contributing_cells'],'suppressed_cells':row['suppressed']})
    # Fixed historical colour domain for the same dimensions and filters. The
    # selected reporting quarter is deliberately excluded from this scale query.
    historical_where=['release_id=?']; historical_args=[release]
    for dim,raw in filters.items():
        vals=raw if isinstance(raw,list) else [raw]
        historical_where.append(f'"{dim}" IN ('+','.join('?' for _ in vals)+')');historical_args.extend(vals)
    scale_max=conn.execute(f'''SELECT MAX(total) FROM (SELECT SUM(value) AS total
        FROM cube_{cube} WHERE '''+' AND '.join(historical_where)+f''' GROUP BY report_date,{key_cols}
        HAVING SUM(value_status!='exact'){incomplete_sil}=0)''',historical_args).fetchone()[0]
    available=conn.execute(f'SELECT 1 FROM cube_{cube} WHERE release_id=? AND quarter_str=? LIMIT 1',(release,quarter)).fetchone() is not None
    source=dict(conn.execute('SELECT * FROM cube_sources WHERE release_id=? AND cube=?',(release,cube)).fetchone())
    source['dates']=json.loads(source.pop('dates_json'))
    return {'cube':cube,'name':DEFINITIONS[cube]['name'],'unit':DEFINITIONS[cube]['unit'],'release_id':release,'quarter':quarter,
        'quarter_available':available,'group_by':group,'filters':filters,'data':result,'source':source,'note':DEFINITIONS[cube]['note'],
        'scale_max':scale_max, 'missing_rule':'Absent combinations are not observed zeros. Aggregates containing suppressed or unpublished inputs have no numeric value.'}


def release_changes(conn, release):
    """Describe source revisions without exposing protected cell values."""
    previous=conn.execute("SELECT release_id FROM cube_releases WHERE ingested_at < (SELECT ingested_at FROM cube_releases WHERE release_id=?) ORDER BY ingested_at DESC,release_id DESC LIMIT 1",(release,)).fetchone()
    if not previous:return []
    previous=previous[0]; changes=[]
    for cube in DEFINITIONS:
        table='cube_'+cube
        current_dates={r[0] for r in conn.execute(f'SELECT DISTINCT report_date FROM {table} WHERE release_id=?',(release,))}
        old_dates={r[0] for r in conn.execute(f'SELECT DISTINCT report_date FROM {table} WHERE release_id=?',(previous,))}
        overlap=sorted(current_dates & old_dates)
        changed=conn.execute(f"SELECT COUNT(*) FROM {table} a JOIN {table} b ON a.cell_key=b.cell_key AND b.release_id=? WHERE a.release_id=? AND (a.value IS NOT b.value OR a.value_status!=b.value_status)",(previous,release)).fetchone()[0]
        added=removed=0
        if overlap:
            marks=','.join('?' for _ in overlap)
            added=conn.execute(f"SELECT COUNT(*) FROM {table} a LEFT JOIN {table} b ON a.cell_key=b.cell_key AND b.release_id=? WHERE a.release_id=? AND b.cell_key IS NULL AND a.report_date IN ({marks})",(previous,release,*overlap)).fetchone()[0]
            removed=conn.execute(f"SELECT COUNT(*) FROM {table} b LEFT JOIN {table} a ON a.cell_key=b.cell_key AND a.release_id=? WHERE b.release_id=? AND a.cell_key IS NULL AND b.report_date IN ({marks})",(release,previous,*overlap)).fetchone()[0]
        changes.append({'cube':cube,'compared_with':previous,'new_dates':sorted(current_dates-old_dates),'dates_not_in_current_file':sorted(old_dates-current_dates),
            'changed_existing_cells':changed,'added_historical_cells':added,'removed_historical_cells':removed})
    return changes


def register_cubes(app, db_path, login_required, admin_required):
    install_bundled(db_path())
    bp=Blueprint('cubes',__name__)
    @bp.errorhandler(ValueError)
    def invalid_request(exc):
        return jsonify({'error':str(exc)}),400

    def connect():
        conn=sqlite3.connect(db_path(),timeout=30);conn.row_factory=sqlite3.Row;return conn

    @bp.route('/assets/<filename>')
    @login_required
    def assets(filename):
        if filename not in ('charts.js','charts.css','cube-explorer.js','cube-explorer.css'): return '',404
        return send_from_directory(ROOT/'static',filename,max_age=0)

    @bp.route('/api/cubes')
    @login_required
    def catalogue():
        with closing(connect()) as conn:
            releases=[dict(row) for row in conn.execute('SELECT release_id,label,ingested_at FROM cube_releases ORDER BY ingested_at DESC,release_id DESC')]
            if not releases:return jsonify({'releases':[],'cubes':[]})
            release=selected_release(conn,request.args.get('release_id'))
            sources={row['cube']:dict(row) for row in conn.execute('SELECT * FROM cube_sources WHERE release_id=?',(release,))}
            cubes=[]
            for cube,definition in DEFINITIONS.items():
                source=sources[cube]; dates=json.loads(source['dates_json'])
                cubes.append({'id':cube,'name':definition['name'],'unit':definition['unit'],'dimensions':{d:LABELS[d] for d in dimensions(cube)},
                    'dates':dates,'quarters':[f'{d[:4]}-Q{(int(d[5:7])+2)//3}' for d in dates],'note':definition['note'],
                    'row_count':source['row_count'],'suppressed_cells':source['suppressed_cells'],'sha256':source['sha256'],'filename':source['filename']})
            return jsonify({'release_id':release,'releases':releases,'cubes':cubes,'revisions':release_changes(conn,release),'participant_rule':'The three participant cubes are separate breakdowns of the same population; do not add or join their raw counts.'})

    @bp.route('/api/cubes/<cube>/filters')
    @login_required
    def cube_filters(cube):
        if cube not in DEFINITIONS:return jsonify({'error':'Unknown data cube'}),404
        with closing(connect()) as conn:
            release=selected_release(conn,request.args.get('release_id')); result={}
            for dim in dimensions(cube):
                if dim=='report_date':continue
                values=[r[0] for r in conn.execute(f'SELECT DISTINCT "{dim}" FROM cube_{cube} WHERE release_id=?',(release,))]
                result[dim]=ordered(values,dim)
            return jsonify({'release_id':release,'filters':result})

    @bp.route('/api/cubes/<cube>/query',methods=['POST'])
    @login_required
    def query(cube):
        try:
            with closing(connect()) as conn:return jsonify(query_cube(conn,cube,request.get_json(silent=True) or {}))
        except ValueError as exc:return jsonify({'error':str(exc)}),400

    @bp.route('/api/cubes/<cube>/export',methods=['POST'])
    @login_required
    def export(cube):
        try:
            with closing(connect()) as conn: result=query_cube(conn,cube,request.get_json(silent=True) or {})
            out=io.StringIO(newline=''); writer=csv.writer(out)
            headers=[LABELS[d] for d in result['group_by']]+['Value','Value status','Unit','Selected calendar quarter','Release','Source file','SHA256','Filters']
            writer.writerow(headers)
            for row in result['data']:
                writer.writerow([*[row[d] for d in result['group_by']],row['value'] if row['value'] is not None else '',row['value_status'],result['unit'],result['quarter'],result['release_id'],result['source']['filename'],result['source']['sha256'],json.dumps(result['filters'],sort_keys=True)])
            return Response('\ufeff'+out.getvalue(),mimetype='text/csv',headers={'Content-Disposition':f'attachment; filename="SDA_{cube}_{result["quarter"]}.csv"'})
        except ValueError as exc:return jsonify({'error':str(exc)}),400

    @bp.route('/admin/cubes/upload',methods=['POST'])
    @admin_required
    def upload_release():
        try:
            uploads=request.files.getlist('files')
            if len(uploads)!=5: raise ValueError('Choose all five CSV files for this release')
            label=request.form.get('label','').strip()
            if not 1<=len(label)<=150: raise ValueError('Enter a release label (up to 150 characters)')
            files={}; manifest_files={}; total=0
            for upload in uploads:
                if not upload.filename.lower().endswith('.csv'): raise ValueError('Upload CSV files only')
                raw=upload.read(16*1024*1024+1);total+=len(raw)
                if len(raw)>16*1024*1024 or total>40*1024*1024: raise ValueError('Release exceeds the upload size limit')
                header=next(csv.reader(io.StringIO(raw.decode('utf-8-sig'))),[])
                matched=[cube for cube,d in DEFINITIONS.items() if set(header)=={'RprtDt',d['measure'],*d['fields']}]
                if len(matched)!=1 or matched[0] in files: raise ValueError('Files must contain each of the five recognised cube schemas exactly once')
                cube=matched[0];files[cube]=raw
                manifest_files[cube]={'filename':Path(upload.filename).name,'sha256':hashlib.sha256(raw).hexdigest()}
            fingerprint=hashlib.sha256(json.dumps(manifest_files,sort_keys=True).encode()).hexdigest()
            manifest={'release_id':'upload-'+fingerprint[:24], 'label':label,'files':manifest_files}
            inserted=install_release(db_path(),manifest,files)
            return jsonify({'success':True,'release_id':manifest['release_id'],'inserted':inserted,
                'message':'Five cubes validated and stored as a new immutable release.' if inserted else 'This identical release is already loaded; no data was changed.'})
        except (ValueError,UnicodeError,csv.Error) as exc:return jsonify({'success':False,'error':str(exc)}),400

    app.register_blueprint(bp)
