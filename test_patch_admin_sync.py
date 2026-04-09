"""
Test: patch_country_layer admin parquet sync.

Verifies that after patch_country_layer updates the mercator parquet, it also
re-aggregates and saves the admin parquet with the patched column values.

Run with:
    python test_patch_admin_sync.py
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, box

# ---------------------------------------------------------------------------
# Minimal environment so impact_analysis can be imported without real creds
# ---------------------------------------------------------------------------
os.environ.setdefault('DATA_PIPELINE_DB', 'LOCAL')
os.environ.setdefault('GEOREPO_API_KEY', 'dummy')
os.environ.setdefault('GEOREPO_USER_EMAIL', 'dummy@example.com')
os.environ.setdefault('GIGA_SCHOOL_LOCATION_API_KEY', 'dummy')
os.environ.setdefault('HEALTHSITES_API_KEY', 'dummy')

import impact_analysis as ia


def _make_mercator_gdf():
    """Two tiles assigned to two different admin regions, num_shelters=0."""
    return gpd.GeoDataFrame(
        {
            'tile_id': ['quad_A', 'quad_B'],
            'geometry': [box(0, 0, 1, 1), box(1, 0, 2, 1)],
            'id': ['adm_1', 'adm_2'],   # admin assignment column
            'num_shelters': [0, 0],
            'population': [100.0, 200.0],
        },
        crs='EPSG:4326',
    )


def _make_admin_gdf():
    """Baseline admin parquet — stale num_shelters values."""
    return gpd.GeoDataFrame(
        {
            'tile_id': ['adm_1', 'adm_2'],
            'name': ['Region One', 'Region Two'],
            'geometry': [box(0, 0, 1, 1), box(1, 0, 2, 1)],
            'num_shelters': [0, 0],
            'population': [100.0, 200.0],
        },
        crs='EPSG:4326',
    )


class TestPatchAdminSync(unittest.TestCase):

    def test_admin_parquet_updated_after_patch(self):
        """
        After patching num_shelters, the admin parquet must reflect the new
        aggregated shelter counts.
        """
        mercator_gdf = _make_mercator_gdf()
        admin_gdf = _make_admin_gdf()

        # Track what write_dataset is called with
        written = {}

        def fake_read_dataset(ds, path):
            if 'mercator_views' in path:
                return mercator_gdf.copy()
            if 'admin_views' in path:
                return admin_gdf.copy()
            raise FileNotFoundError(path)

        def fake_write_dataset(gdf, ds, path):
            written[path] = gdf.copy()

        # Mock _MVG so pycountry is never invoked; map_points returns 3 shelters
        # in tile quad_A and 1 in quad_B
        mock_mvg_instance = MagicMock()
        mock_mvg_instance.map_points.return_value = {'quad_A': 3, 'quad_B': 1}
        MockMVG = MagicMock(return_value=mock_mvg_instance)

        # Fake shelter GeoDataFrame (content doesn't matter — map_points is mocked)
        fake_shelters = gpd.GeoDataFrame(
            {'geometry': [Point(0.5, 0.5)]}, crs='EPSG:4326'
        )

        mercator_path = os.path.join(
            ia.ROOT_DATA_DIR, ia.VIEWS_DIR, 'mercator_views', 'PNG_14.parquet'
        )
        admin_path = os.path.join(
            ia.ROOT_DATA_DIR, ia.VIEWS_DIR, 'admin_views', 'PNG_admin1.parquet'
        )

        def fake_file_exists(path):
            return path in (mercator_path, admin_path)

        with (
            patch.object(ia.data_store, 'file_exists', side_effect=fake_file_exists),
            patch('impact_analysis.read_dataset', side_effect=fake_read_dataset),
            patch('impact_analysis.write_dataset', side_effect=fake_write_dataset),
            patch('impact_analysis.fetch_shelters', return_value=fake_shelters),
            patch('gigaspatial.generators.MercatorViewGenerator', MockMVG),
        ):
            ia.patch_country_layer('PNG', 14, ['shelters'])

        # Mercator write must have happened
        self.assertIn(mercator_path, written, "Mercator parquet was not saved")

        # Admin write must have happened
        self.assertIn(admin_path, written, "Admin parquet was not saved after patch")

        saved_admin = written[admin_path]
        by_id = saved_admin.set_index('tile_id')

        self.assertEqual(
            by_id.loc['adm_1', 'num_shelters'], 3,
            "adm_1 should have 3 shelters after patch",
        )
        self.assertEqual(
            by_id.loc['adm_2', 'num_shelters'], 1,
            "adm_2 should have 1 shelter after patch",
        )

        # Names and geometries must be carried over from existing admin parquet
        self.assertEqual(by_id.loc['adm_1', 'name'], 'Region One')
        self.assertEqual(by_id.loc['adm_2', 'name'], 'Region Two')

    def test_admin_skipped_when_no_id_column(self):
        """If mercator parquet has no 'id' column, admin update is silently skipped."""
        mercator_gdf = _make_mercator_gdf().drop(columns=['id'])
        written = {}

        def fake_read_dataset(ds, path):
            return mercator_gdf.copy()

        def fake_write_dataset(gdf, ds, path):
            written[path] = gdf.copy()

        mock_mvg_instance = MagicMock()
        mock_mvg_instance.map_points.return_value = {'quad_A': 1, 'quad_B': 0}
        MockMVG = MagicMock(return_value=mock_mvg_instance)

        fake_shelters = gpd.GeoDataFrame(
            {'geometry': [Point(0.5, 0.5)]}, crs='EPSG:4326'
        )

        mercator_path = os.path.join(
            ia.ROOT_DATA_DIR, ia.VIEWS_DIR, 'mercator_views', 'PNG_14.parquet'
        )

        with (
            patch.object(ia.data_store, 'file_exists', return_value=True),
            patch('impact_analysis.read_dataset', side_effect=fake_read_dataset),
            patch('impact_analysis.write_dataset', side_effect=fake_write_dataset),
            patch('impact_analysis.fetch_shelters', return_value=fake_shelters),
            patch('gigaspatial.generators.MercatorViewGenerator', MockMVG),
        ):
            ia.patch_country_layer('PNG', 14, ['shelters'])

        admin_path = os.path.join(
            ia.ROOT_DATA_DIR, ia.VIEWS_DIR, 'admin_views', 'PNG_admin1.parquet'
        )
        self.assertNotIn(admin_path, written, "Admin parquet should NOT be saved without 'id' column")


if __name__ == '__main__':
    unittest.main(verbosity=2)
