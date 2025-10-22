from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from abc import ABC, abstractmethod

class BaseFiller(ABC):
    """
    Clase base abstracta para rellenar y transformar datos en tablas Spark.
    Las clases hijas deben implementar el método 'fill_data'.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_table(self, table_name: str) -> DataFrame:
        """
        Lee una tabla desde el catálogo de Spark.
        """
        print(f"🔹 Leyendo tabla: {table_name}")
        return self.spark.table(table_name)

    def write_table(self, df: DataFrame, target_table: str, mode: str = "overwrite") -> None:
        """
        Escribe un DataFrame en una tabla de destino.
        """
        print(f"💾 Escribiendo resultados en: {target_table} (modo={mode})")
        df.write.mode(mode).saveAsTable(target_table)

    @abstractmethod
    def fill_data(self, df: DataFrame) -> DataFrame:
        """
        Método abstracto que deben implementar las subclases.
        Debe recibir un DataFrame y devolverlo transformado.
        """
        pass

    def process_table(self, source_table: str, target_table: str = None) -> None:
        """
        Proceso completo de lectura, transformación y escritura.
        Si no se especifica tabla destino, se sobrescribe la fuente.
        """
        df = self.read_table(source_table)
        df_filled = self.fill_data(df)
        target = target_table or source_table
        self.write_table(df_filled, target)
        print(f"✅ Proceso completado para {target}")
