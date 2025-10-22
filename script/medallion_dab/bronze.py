import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class BronzeIngestion:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.table_definitions = None
    
    def load_table_definitions(self, definitions_df: DataFrame):
        """
        Carga manualmente las definiciones de ingesta desde un DataFrame.
        """
        self.table_definitions = definitions_df

    def read_stream_data(self, landing_path: str, table_name: str, checkpoint_path: str) -> DataFrame:
        try:
            logging.info(f"Cargando datos con Autoloader: {table_name} desde {landing_path}")
            return (
                self.spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("cloudFiles.listBeforeLoad", "true")
                .option("cloudFiles.validateOptions", "false")
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("mergeSchema", "true")
                .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schemas/{table_name}")
                .load(f"{landing_path}/tables/{table_name}")
            )
        except Exception as e:
            logging.error(f"Error al aplicar Autoloader para {table_name}: {e}")
            raise

    def add_metadata_columns(self, df: DataFrame) -> DataFrame:
        # Añadir columna ExtractedAt con la fecha y hora actual
        return df.withColumn("ExtractedAt", current_timestamp())

    def build_schema_and_table(self, schema_name: str, system_source: str, table_type: str, table_name: str) -> str:
        """
        Construye el esquema y el nombre de la tabla en Unity Catalog.
        Usa directamente el catálogo proporcionado (dev_catalog) sin modificarlo.
        """
        dynamic_schema = f"{schema_name}"
        table_with_prefix = f"{table_name}"
        return f"{dynamic_schema}.{table_with_prefix}"
    
    def append_to_delta(self, df: DataFrame, catalog_table: str, partition_column: str, checkpoint_path: str, table_name: str):
        """
        Append en Delta Lake.
        """
        try:
            # Escribir los datos en la tabla Delta con particionamiento
            (
                df.writeStream
                .format("delta")
                .option("mergeSchema", "true")
                #.option("path", f"{checkpoint_path}/tables/{table_name}")
                .outputMode("append")
                .option("checkpointLocation", f"{checkpoint_path}/checkpoint/{table_name}")
                .partitionBy(partition_column)
                .trigger(availableNow=True)
                .toTable(catalog_table)
                #.start()
            )

            logging.info(f"Append completado para {catalog_table}")

        except Exception as e:
            logging.error(f"Error en Append para {catalog_table}: {e}")
            raise    
        
    def process_table(self, table_name: str, landing_path: str, checkpoint_path: str, system_source: str, schema_name: str, table_type: str, partition_column: str):
        """
        Procesa una tabla específica.
        """
        try:
            catalog_table = self.build_schema_and_table(schema_name, system_source, table_type, table_name)

            df = self.read_stream_data(landing_path, table_name, checkpoint_path)
            df = self.add_metadata_columns(df)
            self.append_to_delta(df, catalog_table, partition_column, checkpoint_path, table_name)

        except Exception as e:
            logging.error(f"Error al procesar la tabla {table_name}: {e}")
            raise

    def process_all_tables(self):
        """
        Procesa todas las tablas recuperadas.
        """
        if self.table_definitions is None:
            raise ValueError("No se han cargado las definiciones de ingesta. Usa el método 'load_table_definitions'.")

        try:
            for row in self.table_definitions.collect():
                self.process_table(
                    row["table_name"], 
                    row["landing_path"], 
                    row["checkpoint_path"],
                    row["system_source"], 
                    row["schema_name"], 
                    row["table_type"],
                    #row["catalog"], 
                    row["partition_column"]
                )

        except Exception as e:
            logging.error(f"Error general al procesar todas las tablas: {e}")
            raise
