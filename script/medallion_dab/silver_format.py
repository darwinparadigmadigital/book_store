from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, lower, lit

class CountryCodeFiller:
    def __init__(self, spark: SparkSession):
        self.spark = spark

        # 🔹 Diccionario país → código ISO Alpha-2
        self.country_code_map = {
            "India": "IN",
            "Brasil": "BR",
            "España": "ES",
            "Ecuador": "EC",
            "Colombia": "CO",
            "Turquia": "TR",
            "Italia": "IT",
            "Suiza": "CH",
            "Holanda": "NL"
        }

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

    # -----------------------------------------------------
    # 🔸 Función para rellenar los códigos de país
    # -----------------------------------------------------
    def fill_country_codes(self, df, country_col="Country", code_col="Oceans"):
        """
        Corrige el campo 'Oceans', que en tu dataset contiene los códigos de país.
        Rellena códigos de país ISO Alpha-2.
        """
        df_filled = df
        for country, code in self.country_code_map.items():
            df_filled = df_filled.withColumn(
                code_col,
                when(
                    (lower(trim(col(country_col))) == lit(country.lower())) &
                    (col(code_col).isNull() | (trim(col(code_col)) == "")),
                    code
                ).otherwise(col(code_col))
            )

        filled_count = df_filled.filter(col(code_col).isNotNull()).count()
        print(f"🏳️ Rellenadas {filled_count} filas en columna '{code_col}'.")
        return df_filled

    # -----------------------------------------------------
    # 🔸 Función para rellenar océanos/mar principal
    # -----------------------------------------------------
    def fill_oceanic_region(self, df, country_col="Country", oceanic_col="country_code"):
        """
        Corrige el campo 'country_code', que en tu dataset contiene los océanos.
        Rellena con el océano o mar correcto.
        """
        df_filled = df
        for country, ocean in self.country_oceanic_map.items():
            df_filled = df_filled.withColumn(
                oceanic_col,
                when(
                    (lower(trim(col(country_col))) == lit(country.lower())) &
                    (col(oceanic_col).isNull() | (trim(col(oceanic_col)) == "")),
                    ocean
                ).otherwise(col(oceanic_col))
            )

        filled_count = df_filled.filter(col(oceanic_col).isNotNull()).count()
        print(f"🌊 Rellenadas {filled_count} filas en columna '{oceanic_col}'.")
        return df_filled

    # -----------------------------------------------------
    # 🔸 Aplica todas las transformaciones
    # -----------------------------------------------------
    def process_all_fields(self, df, country_col="Country", code_col="Oceans", oceanic_col="country_code"):
        """Aplica todas las transformaciones en orden correcto."""
        df_filled = self.fill_country_codes(df, country_col, code_col)
        df_filled = self.fill_oceanic_region(df_filled, country_col, oceanic_col)
        return df_filled

    # -----------------------------------------------------
    # 🔸 Procesa una tabla del metastore
    # -----------------------------------------------------
    def process_table(self, table_name: str, save_as_table: bool = True):
        df = self.spark.table(table_name)
        df_enriched = self.process_all_fields(df)

        if save_as_table:
            df_enriched.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(table_name)

            print(f"✅ Tabla '{table_name}' procesada y actualizada con country_code y Oceans.")
        else:
            return df_enriched
