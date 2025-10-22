from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, lit, lower, trim
from medallion_dab.filler.base_filler import BaseFiller  # ajusta la ruta si es distinta

class TownFiller(BaseFiller):
    """
    Clase que rellena el campo 'cod_town' con el código del aeropuerto principal
    correspondiente a cada ciudad.
    """

    def __init__(self, spark):
        super().__init__(spark)

        # 🔹 Diccionario ciudad → código de aeropuerto principal
        self.town_airport_map = {
            "Asuncion": "ASU",
            "Madrid": "MAD",
            "Quito": "UIO",
            "Ankara": "ESB",
            "Roma": "FCO",
            "Amsterdam": "AMS",
            "Bombay": "BOM",
            "Rio de Janeiro": "GIG"
        }

    def fill_data(self, df: DataFrame) -> DataFrame:
        """
        Rellena la columna 'cod_town' en base al nombre de la ciudad (town).
        Solo actualiza filas donde el valor está vacío o nulo.
        """
        df_filled = df
        for town, airport_code in self.town_airport_map.items():
            df_filled = df_filled.withColumn(
                "cod_town",
                when(
                    (lower(trim(col("town"))) == lit(town.lower())) &
                    ((col("cod_town").isNull()) | (trim(col("cod_town")) == "")),
                    airport_code
                ).otherwise(col("cod_town"))
            )
        return df_filled
