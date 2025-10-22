from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, lit
from medallion_dab.filler.base_filler import BaseFiller 

class CountryCodeFiller(BaseFiller):
    """
    Clase que asigna el código de país (country_code) según el nombre del país.
    """

    def __init__(self, spark):
        super().__init__(spark)
        self.country_oceanic_map = {
            "Suiza": "CH",
            "Italia": "IT",
            "España": "ES",
            "Ecuador": "EC",
            "India": "IN",
            "Brasil": "BR",
            "Holanda": "NL",
            "Colombia": "CO",
            "Turquia": "TR"
        }

    def fill_data(self, df: DataFrame) -> DataFrame:
        """
        Añade o actualiza la columna 'country_code' según el nombre del país.
        Si el país no existe en el mapeo, conserva el valor actual.
        """
        df_filled = df.withColumn(
            "country_code",
            when(col("Country").isin(list(self.country_oceanic_map.keys())),
                 # usa el mapeo definido
                 when(col("Country") == lit("Suiza"), lit("CH"))
                 .when(col("Country") == lit("Italia"), lit("IT"))
                 .when(col("Country") == lit("España"), lit("ES"))
                 .when(col("Country") == lit("Ecuador"), lit("EC"))
                 .when(col("Country") == lit("India"), lit("IN"))
                 .when(col("Country") == lit("Brasil"), lit("BR"))
                 .when(col("Country") == lit("Holanda"), lit("NL"))
                 .when(col("Country") == lit("Colombia"), lit("CO"))
                 .when(col("Country") == lit("Turquia"), lit("TR"))
            ).otherwise(col("country_code"))
        )
        return df_filled
