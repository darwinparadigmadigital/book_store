from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, lit, lower, trim
from medallion_dab.filler.base_filler import BaseFiller  # ajusta la ruta del import según tu estructura

class OceanFiller(BaseFiller):
    """
    Clase que asigna el océano o mar principal según el país.
    """

    def __init__(self, spark):
        super().__init__(spark)

        # 🔹 Diccionario país → océano/mar principal
        self.country_oceanic_map = {
            "India": "Indico",
            "Brasil": "Atlantico",
            "España": "Mediterraneo",
            "Ecuador": "Pacifico",
            "Colombia": "Pacifico",
            "Turquia": "Indico",
            "Italia": "Mediterraneo",
            "Suiza": "Mediterraneo",
            "Holanda": "Atlantico"
        }

    def fill_data(self, df: DataFrame) -> DataFrame:
        """
        Rellena la columna 'Oceans' con el océano/mar principal según el país.
        Solo actualiza filas donde el valor está vacío o nulo.
        """
        df_filled = df
        for country, ocean in self.country_oceanic_map.items():
            df_filled = df_filled.withColumn(
                "Oceans",
                when(
                    (lower(trim(col("Country"))) == lit(country.lower())) &
                    ((col("Oceans").isNull()) | (trim(col("Oceans")) == "")),
                    ocean
                ).otherwise(col("Oceans"))
            )
        return df_filled
