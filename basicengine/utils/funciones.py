from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import os
import re
import random
from datetime import datetime, timedelta
import string
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql import types as T
from functools import reduce
from pathlib import Path


class ClaseEngine():
    def __init__(self, OUTPUT_PATH):
        self.dataproc = (
            SparkSession.builder
            .appName("basicengine")
            .getOrCreate()
        )
        
        self.OUTPUT_PATH = OUTPUT_PATH
        
        base_path = Path(__file__).resolve().parent.parent.parent
        base_path = base_path / "data" / "input"

        self.ruta1 = os.path.join(base_path, "planuno")
        self.ruta2 = os.path.join(base_path, "customers_ba")
        self.ruta3 = os.path.join(base_path, "bajas2021")

        self.csv_path_pyspark = os.path.join(OUTPUT_PATH, "cuadrante_planuno")

    def read_data(self):
        self.df_plauno = self.dataproc.read.parquet(self.ruta1)
        self.df_customers = self.dataproc.read.parquet(self.ruta2)
        self.df_bajas = self.dataproc.read.parquet(self.ruta3)

        self.planuno_2020 = self.df_plauno.filter(F.col("date_alta") < "2021-01-01")
        self.planuno_2021 = self.df_plauno.filter(F.col("date_alta") >= "2021-01-01")
    
    def get_base(self):
        df_plauno = self.df_plauno

        self.base = (df_plauno
            .filter((F.col("business_area_id") == "BA03") & (F.col("planuno_quadrant_name") == "Básico"))
            .select("customer_id").distinct()
        )

    def get_antiguedad(self):
        base = self.base
        df_customers = self.df_customers

        self.df_antiguedad = (base
            .join(df_customers.select("customer_id",
                                      F.col("cust_entry_date").alias("fecha_alta_cliente")),
                  "customer_id", "left")
            .withColumn("hoy", F.lit("2022-06-25"))
            .withColumn("diferencia_dias",
                        F.datediff(F.col("hoy").cast("date"),
                                 F.col("fecha_alta_cliente").cast("date")))
            .withColumn(
                "antiguedad",
                F.when(F.col("diferencia_dias") < 183, "< 6 meses")
                .when(F.col("diferencia_dias") < 365, "6-12 meses")
                .when(F.col("diferencia_dias") < 730, "1-2 años")
                .when(F.col("diferencia_dias") < 1825, "2-5 años")
                .otherwise("> 5 años")
            )
        )

    def get_refundidos(self):
        planuno_2020 = self.planuno_2020
        w = Window.partitionBy("customer_id").orderBy(F.col("date_alta").desc())

        self.refunidos_df = (planuno_2020
            .withColumn("row_number", F.row_number().over(w))
            .filter(F.col("row_number") == 1)
            .drop("row_number")
            .select("customer_id", "date_alta"))

    def get_clasificacion(self):
        planuno_2020 = self.planuno_2020

        self.planuno_2021_enriq = (planuno_2020
            .withColumn(
                "Exclusivos_3d3",
                F.when(
                    (
                      (F.col("customer_type").isin(
                          "B.PRIVADA", "B.PERSONAL", "PARTICULARES", "PAES")) &
                      (F.col("mobile_digital_crit_type") == 1) &
                      (F.col("income_freelance_type") == 0) &
                      (F.col("transac_business_type") == 0)
                    ) |
                    (
                      (F.col("customer_type") == "PYMES") &
                      (F.col("mobile_digital_crit_type") == 1) &
                      (F.expr("soc_insur_asgn_fulflt_type  + payroll_asgn_crit_type + tax_pymt_crit_type") == 1) &
                      (F.col("planuno_segment_id").isin("PT","AT","CT","BT","VT"))
                    ),
                    F.lit("Exclusivos_3d3")
                ).otherwise("Otros")
            )
            .select("customer_id", "planuno_quadrant_name", "Exclusivos_3d3"))

    def get_union(self):
        df_antiguedad = self.df_antiguedad
        planuno_2021_enriq = self.planuno_2021_enriq
        df_bajas = self.df_bajas

        self.df = (df_antiguedad.join(planuno_2021_enriq, "customer_id", "left")
            .join(df_bajas.select("customer_id", F.col("movement_type")), "customer_id", "left")
            .withColumn("bajas", F.when(F.col("movement_type") == "baja", 1).otherwise(0))
        )

    def get_cuadrante(self):
        df = self.df
        
        self.df_cuadrante_planuno = (df
            .withColumn(
                "cuadrante_planuno",
                F.when(F.col("bajas") == 1, "BAJAS")
                .when(
                    (F.col("planuno_quadrant_name") == "Transaccional") &
                    (F.col("Exclusivos_3d3") == "Exclusivos_3d3"),
                    "3d3"
                )
                .otherwise(F.col("planuno_quadrant_name"))
            )
        )

    def get_riesgo_cliente(self):
        df = self.df

        self.df_riesgo = (
            df.withColumn(
                "riesgo_cliente",
                F.when(F.col("bajas") == 1, "ALTO")
                 .when(
                     (F.col("diferencia_dias") > 365) &
                     (F.col("planuno_quadrant_name") == "Transaccional"),
                     "MEDIO"
                 )
                 .when(F.col("Exclusivos_3d3") == "Exclusivos_3d3", "BAJO")
                 .otherwise("BAJO")
            )
            .withColumn(
                "score_riesgo",
                F.when(F.col("riesgo_cliente") == "ALTO", 3)
                 .when(F.col("riesgo_cliente") == "MEDIO", 2)
                 .otherwise(1)
            )
        )

    def to_csv(self):
        import os

        ruta_absoluta = os.path.abspath(self.csv_path_pyspark)

        # print("Ruta relativa:", self.csv_path_pyspark)
        # print("Ruta absoluta:", ruta_absoluta)

        self.df_riesgo.coalesce(1) \
            .write \
            .mode("overwrite") \
            .parquet(ruta_absoluta)

        print("WRITE OK")

    def run(self):
        print("1. read_data")
        self.read_data()

        print("2. get_base")
        self.get_base()

        print("3. get_antiguedad")
        self.get_antiguedad()

        print("4. get_refundidos")
        self.get_refundidos()

        print("5. get_clasificacion")
        self.get_clasificacion()

        print("6. get_union")
        self.get_union()

        print("7. get_cuadrante")
        self.get_cuadrante()

        print("8. get_riesgo_cliente")
        self.get_riesgo_cliente()

        print("9. to_csv")
        self.to_csv()

        print("10. terminado")