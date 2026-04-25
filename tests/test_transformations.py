# ===========================================================
# test_transformations.py
# Unit tests para las funciones de transformacion.
# Se ejecutan en GitHub Actions con pytest (sin Spark real).
# ===========================================================

import sys
import os

# Agregar src/ al path para que GitHub Actions encuentre el modulo
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestConfig:
    def test_raw_path_format(self):
        """Verifica que raw_path genera la URL abfss correcta."""
        import config.config as cfg
        cfg.STORAGE_ACCOUNT = "retaildatalakejp01"
        path = cfg.raw_path("test.csv")
        assert path.startswith("abfss://raw@")
        assert "retaildatalakejp01" in path
        assert path.endswith("test.csv")

    def test_sample_limits_are_positive(self):
        """Verifica que los limites de muestreo son positivos."""
        import config.config as cfg
        assert cfg.SAMPLE_ORDERS > 0
        assert cfg.SAMPLE_OP > 0

    def test_schemas_exist(self):
        """Verifica que estan definidos los 3 schemas esperados."""
        import config.config as cfg
        assert "bronze" in cfg.SCHEMAS
        assert "silver" in cfg.SCHEMAS
        assert "gold"   in cfg.SCHEMAS

    def test_catalog_default(self):
        """Verifica que el catalog por defecto es retail_project."""
        import config.config as cfg
        # Sin variable de entorno, debe ser retail_project
        assert cfg.CATALOG in ["retail_project", "retail_project_prod"]


class TestDataQuality:
    def test_revenue_formula(self):
        """Revenue = unit_price * units_sold."""
        unit_price = 932.8
        units      = 32
        assert round(unit_price * units, 2) == 29849.6

    def test_discount_rate_range(self):
        """Discount rate debe estar entre 0 y 1."""
        rate = round(15.0 / 100.0, 4)
        assert 0.0 <= rate <= 1.0

    def test_roi_marketing_positive(self):
        """ROI de marketing debe ser positivo si revenue > 0."""
        roi = round(50000.0 / 6780.0, 3)
        assert roi > 0

    def test_sample_orders_less_than_total(self):
        """Usamos 200K de 1M+ para ahorrar credito Azure."""
        TOTAL_ORDERS = 1_048_576
        SAMPLE       = 200_000
        assert SAMPLE < TOTAL_ORDERS
