import copy,csv,gzip,hashlib,importlib,io,json,os,shutil,sqlite3,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import cube_store as cubes
from load_supplement_p_v4 import create_fresh_database
from supplement_import import write_prepared

class CubeTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.temp=tempfile.TemporaryDirectory();cls.master=Path(cls.temp.name)/'master.db'
  create_fresh_database(cls.master)
  with sqlite3.connect(cls.master) as conn:
   write_prepared(conn,json.loads(gzip.decompress((ROOT/'data/repairs/2026-Q3.json.gz').read_bytes())),'new')
  cls.manifest=json.loads((ROOT/'data/cubes/manifest.json').read_text())
  cls.files={k:gzip.decompress((ROOT/'data/cubes'/v['path']).read_bytes()) for k,v in cls.manifest['files'].items()}
  cubes.install_release(cls.master,cls.manifest,cls.files)
  with patch.dict(os.environ,{'DB_PATH':str(cls.master),'ADMIN_PASSWORD':'test-cube-admin'}):cls.module=importlib.import_module('app')
  cls.module.app.config.update(TESTING=True,SECRET_KEY='cube-test-session')
 @classmethod
 def tearDownClass(cls):cls.temp.cleanup()
 def setUp(self):
  self.db=Path(self.temp.name)/'working.db';shutil.copy2(self.master,self.db)
  self.patch=patch.object(self.module,'DB_PATH',str(self.db));self.patch.start();self.addCleanup(self.patch.stop)
  self.client=self.module.app.test_client()
  with self.client.session_transaction() as session:session['authenticated']=True;session['admin_authenticated']=True
 def query(self,cube,**params):
  return self.client.post('/api/cubes/'+cube+'/query',json={'quarter':'2026-Q3',**params})
 def totals(self,cube,quarter='2026-Q3',release_id=None):
  payload={'quarter':quarter,'group_by':['state']}
  if release_id:payload['release_id']=release_id
  result=self.query(cube,**payload);self.assertEqual(result.status_code,200,result.json)
  return sum(r['value'] for r in result.json['data'])
 def signature(self):
  with sqlite3.connect(self.db) as conn:return [conn.execute(f'SELECT COUNT(*),SUM(value) FROM cube_{c}').fetchone() for c in cubes.DEFINITIONS]
 def test_exact_totals_and_participant_status(self):
  self.assertEqual(self.totals('enrolled'),14537);self.assertEqual(self.totals('pipeline'),5374);self.assertEqual(self.totals('pipeline','2026-Q2'),6544)
  self.assertEqual(self.totals('demand_usage'),25584)
  rows=self.query('demand_usage',group_by=['sda_status']).json['data']
  self.assertEqual({r['sda_status']:r['value'] for r in rows},{'1. SDA in use':16896,'2. SDA eligible - not yet using':8688})
 def test_raw_file_checksums_and_original_dimensions_retained(self):
  with sqlite3.connect(self.db) as conn:
   for cube,raw in self.files.items():
    archived=conn.execute('SELECT raw_gzip FROM cube_raw_files WHERE cube=?',(cube,)).fetchone()[0]
    self.assertEqual(gzip.decompress(archived),raw)
   original,normalised=conn.execute("SELECT source_dimensions,building_type FROM cube_enrolled WHERE building_type='House, 2 residents' LIMIT 1").fetchone()
   self.assertIn('BldngTyp',json.loads(original));self.assertEqual(normalised,'House, 2 residents')
 def test_suppressed_contributions_never_become_partial_numeric_totals(self):
  result=self.query('characteristics',group_by=['age_band','disability_group'],filters={'age_band':'19 to 34','disability_group':'ABI','sda_status':'1. SDA in use'}).json
  self.assertEqual(len(result['data']),1);row=result['data'][0]
  self.assertIsNone(row['value']);self.assertEqual(row['value_status'],'suppressed');self.assertGreater(row['suppressed_cells'],0)
  self.assertNotIn('known_value',row)
 def test_snapshots_and_complete_cohort_values(self):
  rows=self.query('enrolled',group_by=['report_date','new_last_quarter']).json['data']
  latest=[r for r in rows if r['report_date']=='2026-09-30']
  self.assertEqual(sum(r['value'] for r in latest),14537)
  self.assertEqual(next(r['value'] for r in latest if r['new_last_quarter']=='1'),904)
  previous=sum(r['value'] for r in rows if r['report_date']=='2026-06-30');self.assertEqual(14537-previous,302)
  r=self.query('characteristics',group_by=['age_band','disability_group'],filters={'state':'NSW','age_band':'19 to 34','disability_group':'Autism','sda_status':'2. SDA eligible - not yet using'}).json
  self.assertEqual(r['data'][0]['value'],223)
 def test_demographic_profile_combines_only_complete_sil_contributions(self):
  result=self.query('demographics',group_by=['first_nations','sda_status'],filters={'state':'NSW','remoteness':'Major cities'}).json
  rows={(r['first_nations'],r['sda_status']):r['value'] for r in result['data']}
  self.assertEqual(rows['Indigenous','1. SDA in use'],146)
  self.assertEqual(rows['Indigenous','2. SDA eligible - not yet using'],118)
  self.assertEqual(rows['Non-Indigenous','1. SDA in use'],3626)
 def test_unverified_pipeline_dates_and_missing_snapshot(self):
  for quarter,count in [('2026-Q2',14),('2026-Q3',3)]:
   result=self.query('pipeline',quarter=quarter,group_by=['entry_cohort']).json
   self.assertEqual(next(r['value'] for r in result['data'] if r['entry_cohort']=='Unverified entry year'),count)
  absent=self.query('pipeline',quarter='2025-Q4',group_by=['state']).json
  self.assertFalse(absent['quarter_available']);self.assertEqual(absent['data'],[])
 def test_valid_dimensions_filters_and_request_validation(self):
  self.assertEqual(self.query('characteristics',group_by=['sa3']).status_code,400)
  self.assertEqual(self.query('characteristics',group_by=['state'],filters={'sa3':'Belconnen'}).status_code,400)
  for group in [['state','state'],[['state']],['value'],['state','age_band','sda_status']]:self.assertEqual(self.query('characteristics',group_by=group).status_code,400)
  self.assertEqual(self.client.post('/api/cubes/enrolled/query',json=['bad']).status_code,400)
  self.assertEqual(self.client.get('/api/cubes?release_id=missing').status_code,400)
 def test_stable_colour_domain_and_csv_status_provenance(self):
  a=self.query('enrolled',group_by=['state','design_category'],quarter='2026-Q2').json
  b=self.query('enrolled',group_by=['state','design_category'],quarter='2026-Q3').json
  self.assertEqual(a['scale_max'],b['scale_max'])
  response=self.client.post('/api/cubes/characteristics/export',json={'quarter':'2026-Q3','group_by':['age_band','disability_group'],'filters':{'state':'ACT','age_band':'19 to 34','disability_group':'ABI','sda_status':'1. SDA in use'}})
  self.assertEqual(response.status_code,200)
  rows=list(csv.DictReader(io.StringIO(response.get_data(as_text=True).lstrip('\ufeff'))))
  self.assertEqual(rows[0]['Value'],'');self.assertEqual(rows[0]['Value status'],'suppressed');self.assertEqual(rows[0]['Release'],self.manifest['release_id'])
  self.assertEqual(rows[0]['SHA256'],self.manifest['files']['characteristics']['sha256'])
 def test_idempotency_and_atomic_invalid_release(self):
  before=self.signature();self.assertFalse(cubes.install_release(self.db,self.manifest,self.files));self.assertEqual(before,self.signature())
  manifest=copy.deepcopy(self.manifest);manifest['release_id']='invalid-new-release';files=dict(self.files)
  files['pipeline']=files['pipeline'].replace(b'DwellingCnt',b'BrokenCount',1);manifest['files']['pipeline']['sha256']=hashlib.sha256(files['pipeline']).hexdigest()
  with self.assertRaises(ValueError):cubes.install_release(self.db,manifest,files)
  self.assertEqual(before,self.signature())
  with sqlite3.connect(self.db) as conn:self.assertEqual(conn.execute('SELECT COUNT(*) FROM cube_releases').fetchone()[0],1)
 def test_revised_release_retains_original(self):
  manifest=copy.deepcopy(self.manifest);manifest['release_id']='revised-release';manifest['label']='Revised release';files=dict(self.files)
  rows=list(csv.reader(io.StringIO(files['demand_usage'].decode('utf-8-sig'))));rows[1][-1]=str(int(rows[1][-1])+1)
  out=io.StringIO();csv.writer(out).writerows(rows);files['demand_usage']=out.getvalue().encode()
  manifest['files']['demand_usage']['sha256']=hashlib.sha256(files['demand_usage']).hexdigest()
  cubes.install_release(self.db,manifest,files)
  self.assertEqual(self.totals('demand_usage',release_id=self.manifest['release_id']),25584)
  self.assertEqual(self.totals('demand_usage',release_id='revised-release'),25585)
  changes=self.client.get('/api/cubes?release_id=revised-release').json['revisions']
  self.assertEqual(next(c['changed_existing_cells'] for c in changes if c['cube']=='demand_usage'),1)
 def test_authentication_and_complete_upload_required(self):
  anonymous=self.module.app.test_client()
  self.assertEqual(anonymous.get('/api/cubes').status_code,401)
  self.assertEqual(anonymous.post('/api/cubes/enrolled/query',json={}).status_code,401)
  self.assertEqual(anonymous.post('/admin/cubes/upload').status_code,401)
  before=self.signature()
  response=self.client.post('/admin/cubes/upload',data={'label':'Incomplete','files':[(io.BytesIO(raw),self.manifest['files'][cube]['filename']) for cube,raw in list(self.files.items())[:4]]})
  self.assertEqual(response.status_code,400);self.assertEqual(before,self.signature())
  with self.client.session_transaction() as session:session.pop('admin_authenticated',None)
  self.assertEqual(self.client.post('/admin/cubes/upload').status_code,401)
 def test_five_file_admin_upload_is_preserved_and_repeatable(self):
  def upload():
   return self.client.post('/admin/cubes/upload',data={'label':'New complete release','files':[(io.BytesIO(raw),self.manifest['files'][cube]['filename']) for cube,raw in self.files.items()]})
  first=upload();self.assertEqual(first.status_code,200,first.json);self.assertTrue(first.json['inserted'])
  before=self.signature();second=upload();self.assertEqual(second.status_code,200);self.assertFalse(second.json['inserted']);self.assertEqual(before,self.signature())

if __name__=='__main__':unittest.main()
