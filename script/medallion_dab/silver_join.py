from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, current_timestamp

class SilverEnrichmentAdvanced:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.join_definitions = None

    def load_join_definitions(self, df):
        """Carga las reglas de joins desde un DataFrame tipo CSV."""
        self.join_definitions = df

    def _rename_conflicting_columns(self, df, join_df, join_alias):
        """
        Renombra columnas de join_df SOLO si existen en df con el mismo nombre
        que causaría conflicto.
        Devuelve join_df y un dict para actualizar las condiciones.
        """
        df_cols = set(df.columns)
        rename_map = {}

        for c in join_df.columns:
            if c in df_cols:
                # Renombrar solo si hay conflicto
                new_name = f"{c}_{join_alias}"
                join_df = join_df.withColumnRenamed(c, new_name)
                rename_map[f"{join_alias}.{c}"] = f"{new_name}"
            else:
                rename_map[f"{join_alias}.{c}"] = c  # No renombrar

        return join_df, rename_map

    def enrich_table(self, target_table, add_extracted_at=True):
        """Lee la tabla target, aplica todos los joins y devuelve la enriquecida."""
        # Filtrar reglas del CSV
        rules = self.join_definitions.filter(col("target_table") == target_table).collect()
        if not rules:
            raise ValueError(f"No se encontraron reglas para target_table={target_table}")

        # Leer tabla target con alias "tgt"
        df_alias = self.spark.table(target_table).alias("tgt")

        # Aplicar joins según reglas
        for rule in rules:
            join_table = rule["join_table"]
            join_alias = rule["join_alias"]
            join_type = rule["join_type"]
            join_condition = rule["join_condition"]
            join_source_alias = rule["join_source_alias"]

            # Cargar tabla de join con alias
            join_df = self.spark.table(join_table).alias(join_alias)

            # Renombrar conflictos y obtener mapping
            join_df, rename_map = self._rename_conflicting_columns(df_alias, join_df, join_alias)

            # Reescribir condición: reemplazar alias de join y alias de target
            join_condition_fixed = join_condition
            for old, new in rename_map.items():
                # Reemplazamos solo la parte del alias de join
                if old.startswith(f"{join_alias}."):
                    join_condition_fixed = join_condition_fixed.replace(old, new)
            # Reemplazar alias de origen (target) por "tgt"
            join_condition_fixed = join_condition_fixed.replace(f"{join_source_alias}.", "tgt.")

            # Ejecutar join
            df_alias = df_alias.join(join_df, on=expr(join_condition_fixed), how=join_type.lower())

        # Columna ExtractedAt → siempre se sobreescribe
        if add_extracted_at:
            df_alias = df_alias.withColumn("ExtractedAt", current_timestamp())

        # Eliminar columnas duplicadas manteniendo la primera ---
        seen = set()
        cols_final = []
        for c in df_alias.columns:
            if c not in seen:
                cols_final.append(c)
                seen.add(c)

        df_alias = df_alias.select(*cols_final)

        # Nombre final enriquecido
        enriched_table_name = (
            rules[0]["enriched_table_name"]
            if "enriched_table_name" in self.join_definitions.columns
            else target_table
        )

        return df_alias, enriched_table_name

    def process_all_enrichments(self, add_extracted_at=True):
        """Procesa todas las tablas definidas en el CSV y guarda como tablas delta."""
        target_tables = [
            row["target_table"] for row in self.join_definitions.select("target_table").distinct().collect()
        ]

        for table in target_tables:
            enriched_df, final_table_name = self.enrich_table(table, add_extracted_at=add_extracted_at)

            # Guardar como tabla Delta
            enriched_df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(final_table_name)

            print(f"Procesada tabla enriquecida: {final_table_name}")
