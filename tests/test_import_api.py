import gzip
import importlib
import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import openpyxl

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from load_supplement_p_v4 import create_fresh_database
from supplement_import import write_prepared


class ImportApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.master=Path(cls.temp.name)/'master.db'
        create_fresh_database(cls.master)
        data=json.loads(gzip.decompress((ROOT/'data/repairs/2026-Q3.json.gz').read_bytes()))
        with sqlite3.connect(cls.master) as conn:
            conn.execute('BEGIN IMMEDIATE');write_prepared(conn,data,'new')
        # An explicit nonempty test DB prevents the application's packaged-data seed.
        with patch.dict(os.environ,{'DB_PATH':str(cls.master)}):cls.module=importlib.import_module('app')
        cls.module.app.config.update(TESTING=True,SECRET_KEY='local-test-session')

    @classmethod
    def tearDownClass(cls):cls.temp.cleanup()

    def setUp(self):
        self.db=Path(self.temp.name)/'working.db';shutil.copy2(self.master,self.db)
        self.db_patch=patch.object(self.module,'DB_PATH',str(self.db));self.db_patch.start();self.addCleanup(self.db_patch.stop)
        self.client=self.module.app.test_client()
        with self.client.session_transaction() as session:
            session['authenticated']=True;session['admin_authenticated']=True

    def test_current_participant_pipeline_provider_and_sa3_apis(self):
        providers=self.client.get('/api/providers').get_json()
        self.assertEqual(providers['timeseries'][-1]['value'],614)
        self.assertEqual(providers['source_notes']['2026-Q3']['title_quarter'],'2026-Q2')
        participants=self.client.get('/api/participants').get_json()
        self.assertEqual(next(r['value'] for r in participants['timeseries'] if r['source_table']=='Table P.9'),25584)
        splits={r['measure_name']:r['value'] for r in participants['split']}
        self.assertEqual(splits['Participants with SDA in use'],16896)
        self.assertEqual(splits['Participants SDA eligible, not yet using SDA'],8688)
        dwellings=self.client.get('/api/dwellings/sa3').get_json()
        self.assertEqual(sum(r['geo_type']=='SA3' for r in dwellings['bt']),1715)
        self.assertEqual(sum(r['geo_type']=='SA3' for r in dwellings['dc']),2401)
        quality=self.client.get('/api/import-quality').get_json()
        self.assertEqual(len(quality['report']['tables']),18)
        self.assertEqual(quality['report']['rows_loaded'],32698)
        pipeline_id=next(d['dataset_id'] for d in self.client.get('/api/filters').get_json()['datasets'] if d['source_table']=='Table P.16')
        rows=self.client.post('/api/data/timeseries',json={'dataset_id':pipeline_id,'geo_type':'Australia'}).get_json()['data']
        self.assertEqual(next(r['value'] for r in rows if r['measure_name']=='Total'),5374)
        self.assertEqual(self.client.get('/api/sda-capacity').get_json()[-1]['total_places'],31446)

    def test_provider_aliases_do_not_duplicate_current_or_ever_counts(self):
        with sqlite3.connect(self.db) as conn:
            q,d,g=conn.execute("SELECT m.quarter_id,m.dataset_id,m.geo_id FROM measures m JOIN datasets d USING(dataset_id) JOIN geographies g USING(geo_id) WHERE d.source_table='Table P.3' AND g.geo_type='Australia'").fetchone()
            for name in ['Providers','Providers active in quarter','Providers ever active']:
                conn.execute('INSERT INTO measures(quarter_id,dataset_id,geo_id,measure_name,value) VALUES(?,?,?,?,999)',(q,d,g,name))
        rows=self.client.get('/api/providers').get_json()['timeseries']
        self.assertEqual(len(rows),1);self.assertEqual(rows[0]['value'],614)

    def test_admin_rejects_incomplete_release_with_existing_data_retained(self):
        wb=openpyxl.Workbook();ws=wb.active;ws.title='Table P.4'
        ws.append(['Number of Enrolled SDA Dwellings by SA4 Region and Building Type as at 30 September 2026'])
        ws.append(['SA4 Region','Existing','Total']);ws.append(['Total',1,1])
        file=io.BytesIO();wb.save(file);file.seek(0)
        with sqlite3.connect(self.db) as conn:before=conn.execute('SELECT COUNT(*),SUM(value) FROM measures').fetchone()
        response=self.client.post('/admin/upload',data={'file':(file,'incomplete.xlsx'),'fy':'2026-27','fyq':'Q1','mode':'replace'},content_type='multipart/form-data')
        self.assertEqual(response.status_code,500);self.assertFalse(response.get_json()['success'])
        self.assertIn('Incomplete release',response.get_json()['message'])
        with sqlite3.connect(self.db) as conn:self.assertEqual(conn.execute('SELECT COUNT(*),SUM(value) FROM measures').fetchone(),before)


if __name__=='__main__':unittest.main()
