from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, from_json, current_timestamp, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType


class SilverIngestion:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.table_definitions = None

    def load_table_definitions(self, df):
        """Carga las reglas de ingestión desde un DataFrame con la definición (tu CSV)."""
        self.table_definitions = df

    def read_from_catalog(self, catalog, schema, table):
        """Lee una tabla de un catálogo y esquema dado."""
        return self.spark.table(f"{catalog}.{schema}.{table}")

    def parse_type(self, dtype: str):
        """Convierte string en tipo de Spark SQL."""
        if not dtype:
            return StringType()
        dtype = dtype.lower()
        if dtype == "string":
            return StringType()
        elif dtype == "int":
            return IntegerType()
        elif dtype == "double":
            return DoubleType()
        elif dtype == "timestamp":
            return TimestampType()
        else:
            return StringType()

    def build_struct_type(self, struct_fields: str):
        """Construye un StructType a partir de la definición en CSV, adaptándose a tipos complejos."""
        fields = []
        for f in struct_fields.split(","):
            parts = f.split(":")
            name = parts[0].strip()
            dtype = ":".join(parts[1:]).strip()  # Adaptable a cualquier cantidad de ":"
            fields.append(StructField(name, self.parse_type(dtype)))
        return ArrayType(StructType(fields))

    def apply_transformations(self, df, target_table):
        """Castea columnas según CSV, maneja JSON y expande arrays si aplica. 
        Además, aplica trim a strings para limpiar espacios."""
        rules = self.table_definitions.filter(
            col("target_table") == target_table
        ).collect()

        for rule in rules:
            column = rule["source_column_name"]
            source_type = rule["source_field_type"]
            cast_type = rule["cast_type"]
            struct_fields = rule["struct_fields"]

            # Manejo de JSON embebido en string
            if source_type and source_type.lower() == "json":
                if struct_fields:
                    json_schema = StructType([
                        StructField(parts[0].strip(), self.parse_type(":".join(parts[1:]).strip()))
                        for f in struct_fields.split(",")
                        for parts in [f.split(":")]
                    ])
                    df = df.withColumn(column, from_json(col(column), json_schema))
                else:
                    print(f"Columna {column} marcada como JSON pero sin schema. Se deja como string.")

            # Manejo de array_struct
            elif cast_type and cast_type.lower() == "array_struct" and struct_fields:
                fields_expr = ", ".join([
                    f"cast(x.{parts[0].strip()} as {':'.join(parts[1:]).strip()}) as {parts[0].strip()}"
                    for f in struct_fields.split(",")
                    for parts in [f.split(":")]
                ])
                df = (
                    df.withColumn(column, explode_outer(col(column)))
                      .selectExpr("*", f"named_struct({fields_expr}) as {column}")
                      .drop(column)
                )

            # Array<struct> simple
            elif struct_fields:
                df = df.withColumn(column, col(column).cast(self.build_struct_type(struct_fields)))

            # Casteo normal
            else:
                df = df.withColumn(column, col(column).cast(self.parse_type(cast_type)))

            # 🔹 Siempre aplicar trim si es string
            if cast_type and cast_type.lower() == "string":
                df = df.withColumn(column, trim(col(column)))

        return df

    def process_table(self, target_table):
        """Procesa una tabla desde bronze a silver con transformaciones y añade columna ExtractedAt."""
        table_rules = (
            self.table_definitions.filter(col("target_table") == target_table).first()
        )

        source_catalog = table_rules["source_catalog"]
        source_schema = table_rules["source_schema"]
        source_table = table_rules["source_table"]

        target_catalog = table_rules["target_catalog"]
        target_schema = table_rules["target_schema"]
        partition_col = table_rules["partition_column"]

        # Leer datos desde el catálogo
        df = self.read_from_catalog(source_catalog, source_schema, source_table).alias(target_table)

        # Aplicar transformaciones
        df = self.apply_transformations(df, target_table)

        # Crear columna ExtractedAt si no existe
        if partition_col and partition_col not in df.columns:
            df = df.withColumn(partition_col, current_timestamp())

        # Guardar en silver particionando por ExtractedAt
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy(partition_col) \
            .saveAsTable(f"{target_catalog}.{target_schema}.{target_table}")

        print(f"Procesada tabla: {target_catalog}.{target_schema}.{target_table}")

    def process_all_tables(self):
        """Procesa todas las tablas target definidas en el CSV."""
        target_tables = [row["target_table"] for row in self.table_definitions.select("target_table").distinct().collect()]
        for table in target_tables:
            self.process_table(table)
