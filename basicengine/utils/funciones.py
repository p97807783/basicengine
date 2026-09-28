import os

import pandas as pd


class ClaseEngine:
    def __init__(self, OUTPUT_PATH):
        self.OUTPUT_PATH = OUTPUT_PATH

        base_path = os.path.join(os.getcwd(), "data", "input")

        self.ruta1 = os.path.join(base_path, "planuno")
        self.ruta2 = os.path.join(base_path, "customers_ba")
        self.ruta3 = os.path.join(base_path, "bajas2021")

        # Conservamos el nombre del atributo por compatibilidad.
        self.csv_path_pyspark = os.path.join(
            OUTPUT_PATH, "cuadrante_planuno"
        )

    @staticmethod
    def _left_join(izquierda, derecha):
        # Spark no empareja claves nulas; pandas sí.
        # Excluimos las claves nulas de la derecha para mantener
        # el comportamiento del left join original.
        derecha = derecha.loc[derecha["customer_id"].notna()]

        return izquierda.merge(
            derecha,
            on="customer_id",
            how="left",
            sort=False,
        )

    def read_data(self):
        self.df_plauno = pd.read_parquet(self.ruta1)
        self.df_customers = pd.read_parquet(self.ruta2)
        self.df_bajas = pd.read_parquet(self.ruta3)

        fechas = pd.to_datetime(
            self.df_plauno["date_alta"],
            errors="coerce",
        )
        fecha_corte = pd.Timestamp("2021-01-01")

        self.planuno_2020 = self.df_plauno.loc[
            fechas < fecha_corte
        ].copy()

        self.planuno_2021 = self.df_plauno.loc[
            fechas >= fecha_corte
        ].copy()

    def get_base(self):
        condicion = (
            self.df_plauno["business_area_id"].eq("BA03")
            & self.df_plauno["planuno_quadrant_name"].eq("Básico")
        )

        self.base = (
            self.df_plauno.loc[
                condicion.fillna(False), ["customer_id"]
            ]
            .drop_duplicates()
            .reset_index(drop=True)
        )

    def get_antiguedad(self):
        clientes = self.df_customers[
            ["customer_id", "cust_entry_date"]
        ].rename(
            columns={"cust_entry_date": "fecha_alta_cliente"}
        )

        self.df_antiguedad = self._left_join(
            self.base, clientes
        )

        df = self.df_antiguedad
        df["hoy"] = "2022-06-25"

        fechas_alta = pd.to_datetime(
            df["fecha_alta_cliente"],
            errors="coerce",
        ).dt.normalize()

        df["diferencia_dias"] = (
            pd.Timestamp("2022-06-25") - fechas_alta
        ).dt.days.astype("Int64")

        dias = df["diferencia_dias"]

        # Aplicamos los umbrales de mayor a menor para que
        # los tramos más pequeños tengan prioridad.
        df["antiguedad"] = "> 5 años"

        for limite, etiqueta in [
            (1825, "2-5 años"),
            (730, "1-2 años"),
            (365, "6-12 meses"),
            (183, "< 6 meses"),
        ]:
            df.loc[
                dias.lt(limite).fillna(False),
                "antiguedad",
            ] = etiqueta

    def get_refundidos(self):
        df = self.planuno_2020[
            ["customer_id", "date_alta"]
        ].copy()

        df["_fecha_orden"] = pd.to_datetime(
            df["date_alta"],
            errors="coerce",
        )

        self.refunidos_df = (
            df.sort_values(
                "_fecha_orden",
                ascending=False,
                na_position="last",
                kind="stable",
            )
            .drop_duplicates(subset="customer_id", keep="first")
            .drop(columns="_fecha_orden")
            .reset_index(drop=True)
        )

    def get_clasificacion(self):
        df = self.planuno_2020

        particulares = (
            df["customer_type"].isin(
                ["B.PRIVADA", "B.PERSONAL", "PARTICULARES", "PAES"]
            )
            & df["mobile_digital_crit_type"].eq(1)
            & df["income_freelance_type"].eq(0)
            & df["transac_business_type"].eq(0)
        )

        suma_criterios = (
            df["soc_insur_asgn_fulflt_type"]
            + df["payroll_asgn_crit_type"]
            + df["tax_pymt_crit_type"]
        )

        pymes = (
            df["customer_type"].eq("PYMES")
            & df["mobile_digital_crit_type"].eq(1)
            & suma_criterios.eq(1)
            & df["planuno_segment_id"].isin(
                ["PT", "AT", "CT", "BT", "VT"]
            )
        )

        self.planuno_2021_enriq = df[
            ["customer_id", "planuno_quadrant_name"]
        ].copy()

        self.planuno_2021_enriq["Exclusivos_3d3"] = "Otros"

        self.planuno_2021_enriq.loc[
            (particulares | pymes).fillna(False),
            "Exclusivos_3d3",
        ] = "Exclusivos_3d3"

    def get_union(self):
        self.df = self._left_join(
            self.df_antiguedad,
            self.planuno_2021_enriq,
        )

        self.df = self._left_join(
            self.df,
            self.df_bajas[["customer_id", "movement_type"]],
        )

        self.df["bajas"] = (
            self.df["movement_type"]
            .eq("baja")
            .fillna(False)
            .astype(int)
        )

    def get_cuadrante(self):
        self.df_cuadrante_planuno = self.df.copy()
        df = self.df_cuadrante_planuno

        df["cuadrante_planuno"] = df[
            "planuno_quadrant_name"
        ].copy()

        condicion_3d3 = (
            df["planuno_quadrant_name"].eq("Transaccional")
            & df["Exclusivos_3d3"].eq("Exclusivos_3d3")
        )

        df.loc[
            condicion_3d3.fillna(False),
            "cuadrante_planuno",
        ] = "3d3"

        # Las bajas tienen prioridad sobre cualquier cuadrante.
        df.loc[
            df["bajas"].eq(1).fillna(False),
            "cuadrante_planuno",
        ] = "BAJAS"

    def get_riesgo_cliente(self):
        self.df_riesgo = self.df.copy()
        df = self.df_riesgo

        df["riesgo_cliente"] = "BAJO"

        condicion_medio = (
            df["diferencia_dias"].gt(365)
            & df["planuno_quadrant_name"].eq("Transaccional")
        )

        df.loc[
            condicion_medio.fillna(False),
            "riesgo_cliente",
        ] = "MEDIO"

        # ALTO tiene prioridad sobre MEDIO y BAJO.
        df.loc[
            df["bajas"].eq(1).fillna(False),
            "riesgo_cliente",
        ] = "ALTO"

        df["score_riesgo"] = df["riesgo_cliente"].map(
            {"ALTO": 3, "MEDIO": 2, "BAJO": 1}
        )

    def to_csv(self):
        # El método original se llama to_csv, pero guarda Parquet.
        # Mantenemos ese formato.
        os.makedirs(self.csv_path_pyspark, exist_ok=True)

        ruta_salida = os.path.join(
            self.csv_path_pyspark,
            "resultado.parquet",
        )

        self.df_riesgo.to_parquet(
            ruta_salida,
            engine="pyarrow",
            index=False,
        )

    def run(self):
        self.read_data()
        self.get_base()
        self.get_antiguedad()
        self.get_refundidos()
        self.get_clasificacion()
        self.get_union()
        self.get_cuadrante()
        self.get_riesgo_cliente()
        self.to_csv()