import copy
import gzip
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import openpyxl

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import supplement_import as importer
from load_supplement_p_v4 import create_fresh_database, parse_quarter_from_filename


class ImportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.prepared=json.loads(gzip.decompress((ROOT/'data/repairs/2026-Q3.json.gz').read_bytes()))
        cls.headers=json.loads((ROOT/'tests/supplement_headers.json').read_text())

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.db=Path(self.temp.name)/'test.db';create_fresh_database(self.db)

    def seed(self,prepared=None):
        with sqlite3.connect(self.db) as conn:
            conn.execute('BEGIN IMMEDIATE')
            return importer.write_prepared(conn,prepared or self.prepared,'new')

    def signature(self):
        with sqlite3.connect(self.db) as conn:
            return conn.execute('SELECT quarter_id,dataset_id,geo_id,measure_name,value FROM measures ORDER BY measure_id').fetchall()

    def test_all_real_titles_and_leading_pipeline_title(self):
        wb=openpyxl.Workbook();wb.remove(wb.active)
        for item in self.headers:
            ws=wb.create_sheet(item['sheet'])
            if item['canonical']=='Table P.16':ws.append([self.headers[4]['title']])
            ws.append([item['title']]);ws.append(item['headers'])
        mapping,warnings=importer.detect_workbook(wb)
        self.assertEqual(set(mapping),{f'Table P.{n}' for n in range(1,19)})
        self.assertEqual(mapping['Table P.16']['sheet'],'Table P.16')
        self.assertEqual(len(warnings),1)
        self.assertEqual(importer.identify_title('Table11 Participants with identified SDA needs (SA4)')[0],'Table P.9')
        self.assertEqual(importer.identify_title('Unfinished (Unenrolled) New Build SDA Dwellings by Design Category and SA4 Region')[0],'Table P.8')

    def test_placeholder_columns_stop_at_actual_total(self):
        wb=openpyxl.Workbook();ws=wb.active
        ws.append(['SA4 Region','Existing','Total']+[f'Column{i}' for i in range(1,400)])
        ws.append(['ACT',5,5])
        headers,rows=importer.table_rows(ws)
        self.assertEqual(headers,['SA4 Region','Existing','Total'])
        self.assertEqual(rows,[('ACT',5,5)])

    def test_sa3_types_and_duplicate_act_service_district(self):
        records,_=importer.parse_table('Table P.13',['SA3 Region','Existing','Total'],[('ACT - Belconnen',1,1),('Total',1,1)])
        self.assertEqual(records[0][0],'SA3')
        rows=[('ACT',146,12834),('ACT',146,12833),('Total',146,12834)]
        records,_=importer.parse_table('Table P.1',['Service District','Participants with SDA in use','Number of active participants'],rows)
        self.assertEqual([r[0] for r in records[::2]],['State','Service District','Australia'])

    def test_suppression_is_preserved_not_estimated(self):
        records,notes=importer.parse_table('Table P.3',['State/Territory','Providers'],[('OT','<5'),('National',614)])
        self.assertEqual(records[0][-1],None)
        self.assertEqual(notes[0][-1],'<5')
        self.assertEqual(records[1][-2:],["Active providers",614.0])

    def test_full_payload_and_totals(self):
        importer.validate_tables(self.prepared['tables'])
        result=self.seed();self.assertEqual(result['rows_loaded'],32698)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT total_dwellings,total_places FROM sda_capacity WHERE quarter='2026-Q3'").fetchone(),(14537,31446))
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM import_suppressed').fetchone()[0],40)
        damaged=copy.deepcopy(self.prepared['tables'])
        next(t for t in damaged if t['canonical']=='Table P.16')['records'][0][-1]+=1
        with self.assertRaises(importer.ImportValidationError):importer.validate_tables(damaged)

    def test_replace_rolls_back_after_late_write_failure(self):
        self.seed();before=self.signature();original=importer.write_prepared
        def fail_late(*args,**kwargs):
            original(*args,**kwargs)
            raise RuntimeError('simulated late storage failure')
        with patch.object(importer,'prepare_workbook',return_value=self.prepared),patch.object(importer,'write_prepared',side_effect=fail_late):
            result=importer.run_loader('unused.xlsx','2026-Q3','replace',str(self.db))
        self.assertFalse(result['success']);self.assertEqual(result['rows_loaded'],0)
        self.assertEqual(before,self.signature())
        self.assertEqual(len(list(Path(str(self.db)+'.backups').glob('*.db'))),1)

    def test_new_mode_does_not_overwrite_existing_quarter(self):
        self.seed();before=self.signature()
        with patch.object(importer,'prepare_workbook',return_value=self.prepared):
            result=importer.run_loader('unused.xlsx','2026-Q3','new',str(self.db))
        self.assertFalse(result['success']);self.assertEqual(before,self.signature())

    def test_geography_repair_preserves_values_and_is_idempotent(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute("INSERT INTO quarters(year,quarter_num,quarter_str) VALUES(2025,2,'2025-Q2')")
            conn.execute("INSERT INTO datasets(source_table) VALUES('Table P.14')")
            conn.execute("INSERT INTO geographies(geo_type,name,state) VALUES('SA4','Belconnen','ACT')")
            geo=conn.execute('SELECT MAX(geo_id) FROM geographies').fetchone()[0]
            conn.execute("INSERT INTO measures(quarter_id,dataset_id,geo_id,measure_name,value) VALUES(1,1,?,'Basic',17)",(geo,))
        result=importer.apply_bundled_repairs(str(self.db))
        self.assertEqual(result[0]['measures_reclassified'],1)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute('SELECT g.geo_type,m.value FROM measures m JOIN geographies g USING(geo_id)').fetchone(),('SA3',17.0))
        self.assertEqual(importer.apply_bundled_repairs(str(self.db)),[])

    def test_release_repair_cannot_overwrite_unexpected_data(self):
        self.seed()
        with sqlite3.connect(self.db) as conn:conn.execute('DELETE FROM measures WHERE measure_id=(SELECT MAX(measure_id) FROM measures)')
        before=self.signature()
        with self.assertRaisesRegex(importer.ImportValidationError,'changed since'):
            importer.apply_bundled_repairs(str(self.db))
        self.assertEqual(before,self.signature())

    def test_financial_year_filename_conversion(self):
        for name,expected in [('Supplement P 2026-27 Q1.xlsx','2026-Q3'),('supp_202627_Q2.xlsx','2026-Q4'),
                              ('Supp P 2025-26 Q4.xlsx','2026-Q2'),('2026-Q3.xlsx','2026-Q3')]:
            self.assertEqual(parse_quarter_from_filename(name),expected)

    @unittest.skipUnless(os.environ.get('SDA_TEST_WORKBOOK'),'Set SDA_TEST_WORKBOOK for the full supplied-file check')
    def test_supplied_workbook_matches_reviewed_payload(self):
        actual=importer.prepare_workbook(os.environ['SDA_TEST_WORKBOOK'],'2026-Q3')
        self.assertEqual(actual['tables'],self.prepared['tables'])
        self.assertEqual(actual['workbook_sha256'],self.prepared['workbook_sha256'])
        self.assertEqual(len(actual['tables']),18)
        with self.assertRaisesRegex(importer.ImportValidationError,'Workbook dates'):
            importer.prepare_workbook(os.environ['SDA_TEST_WORKBOOK'],'2026-Q2')


if __name__=='__main__':unittest.main()
