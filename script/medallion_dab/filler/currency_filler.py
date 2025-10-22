"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, rand, round
from medallion_dab.filler.base_filler import BaseFiller  # ajusta la ruta según tu estructura

class CurrencyFiller(BaseFiller):

    def __init__(self, spark, min_value=5, max_value=100):
        super().__init__(spark)
        self.min_value = min_value
        self.max_value = max_value

    def fill_data(self, df: DataFrame) -> DataFrame:
        df_filled = df.withColumn(
            "value",
            when(
                (col("value").isNull()) | (col("value") == ""),
                round(self.min_value + (self.max_value - self.min_value) * rand(), 2)
            ).otherwise(col("value"))
        )
        return df_filled
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, rand, round
from pyspark.sql.types import DoubleType, IntegerType
from medallion_dab.filler.base_filler import BaseFiller  # ajusta la ruta según tu estructura

class CurrencyFiller(BaseFiller):
    """
    Clase que rellena la columna 'value' con valores numéricos aleatorios
    (por ejemplo, tasas o importes base) cuando está vacía o nula.
    """

    def __init__(self, spark, min_value=5, max_value=100):
        super().__init__(spark)
        self.min_value = min_value
        self.max_value = max_value

    def fill_data(self, df: DataFrame) -> DataFrame:
        """
        Rellena el campo 'value' con un número aleatorio entre min_value y max_value.
        Solo actualiza las filas donde 'value' sea nulo o esté vacío.
        Convierte la columna a DoubleType para evitar conflictos con Delta.
        """
        df_filled = df.withColumn(
            "value",
            when(
                (col("value").isNull()) | (col("value") == ""),
                round(self.min_value + (self.max_value - self.min_value) * rand(), 2)
            ).otherwise(col("value"))
        )

        # 🔹 Forzar el tipo Double para compatibilidad con Delta
        df_filled = df_filled.withColumn("value", col("value").cast(IntegerType()))

        return df_filled

