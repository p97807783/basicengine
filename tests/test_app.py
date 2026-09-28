import pandas as pd
from basicengine.utils.funciones import ClaseEngine

def test_get_base(tmp_path):
    engine = ClaseEngine(str(tmp_path))

    engine.df_plauno = pd.DataFrame([
        ("BA03", "Básico", 1),
        ("BA03", "Premium", 2),
        ("BA01", "Básico", 3)
    ], columns=["business_area_id", "planuno_quadrant_name", "customer_id"])

    engine.get_base()

    result = [row.customer_id for row in engine.base.itertuples(index=False)]

    assert result == [1]

def test_get_antiguedad(tmp_path):
    engine = ClaseEngine(str(tmp_path))

    # base
    engine.base = pd.DataFrame([
        (1,),
        (2,)
    ], columns=["customer_id"])

    # customers
    engine.df_customers = pd.DataFrame([
        (1, "2022-04-01"),  # cliente reciente
        (2, "2015-01-01")   # cliente antiguo
    ], columns=["customer_id", "cust_entry_date"])

    engine.get_antiguedad()

    result = engine.df_antiguedad[["customer_id", "antiguedad"]].itertuples(index=False)

    res_dict = {r.customer_id: r.antiguedad for r in result}

    assert res_dict[1] == "< 6 meses"
    assert res_dict[2] == "> 5 años"

def test_get_refundidos(tmp_path):
    engine = ClaseEngine(str(tmp_path))

    engine.planuno_2020 = pd.DataFrame([
        (1, "2020-01-01"),
        (1, "2020-06-01"),  # más reciente
        (2, "2020-03-01")
    ], columns=["customer_id", "date_alta"])

    engine.get_refundidos()

    result = engine.refunidos_df.itertuples(index=False)
    res = {r.customer_id: r.date_alta for r in result}

    assert res[1] == "2020-06-01"
    assert res[2] == "2020-03-01"

def test_get_clasificacion(tmp_path):
    engine = ClaseEngine(str(tmp_path))

    engine.planuno_2020 = pd.DataFrame([
        ("1", "B.PERSONAL", 1, 0, 0, 1, 0, 0, "X", "Transaccional"),
        ("2", "PYMES", 1, 0, 0, 0, 0, 0, "ZZ", "Básico"),
        ("3", "PYMES", 1, 1, 0, 0, 1, 0, "PT", "Transaccional")
    ], columns=[
        "customer_id",
        "customer_type",
        "mobile_digital_crit_type",
        "income_freelance_type",
        "transac_business_type",
        "soc_insur_asgn_fulflt_type",
        "payroll_asgn_crit_type",
        "tax_pymt_crit_type",
        "planuno_segment_id",
        "planuno_quadrant_name"
    ])

    engine.get_clasificacion()

    df = engine.planuno_2021_enriq.itertuples(index=False)
    res = {r.customer_id: r.Exclusivos_3d3 for r in df}

    assert res["1"] == "Exclusivos_3d3"
    assert res["2"] == "Otros"
    assert res["3"] == "Exclusivos_3d3"

def test_get_union(tmp_path):
    engine = ClaseEngine(str(tmp_path))

    engine.df_antiguedad = pd.DataFrame([
        (1, "6-12 meses"),
        (2, "1-2 años")
    ], columns=["customer_id", "antiguedad"])

    engine.planuno_2021_enriq = pd.DataFrame([
        (1, "Transaccional", "Exclusivos_3d3"),
        (2, "Básico", "Otros")
    ], columns=["customer_id", "planuno_quadrant_name", "Exclusivos_3d3"])

    engine.df_bajas = pd.DataFrame([
        (1, "baja"),
        (2, "activo")
    ], columns=["customer_id", "movement_type"])

    engine.get_union()

    df = engine.df.itertuples(index=False)
    res = {r.customer_id: (r.bajas, r.movement_type) for r in df}

    assert res[1][0] == 1
    assert res[2][0] == 0

def test_get_cuadrante(tmp_path):
    engine = ClaseEngine(str(tmp_path))

    engine.df = pd.DataFrame([
        # baja
        (1, "Transaccional", "Exclusivos_3d3", 1),

        # caso 3d3
        (2, "Transaccional", "Exclusivos_3d3", 0),

        # default
        (3, "Básico", "Otros", 0)
    ], columns=[
        "customer_id",
        "planuno_quadrant_name",
        "Exclusivos_3d3",
        "bajas"
    ])

    engine.get_cuadrante()

    df = engine.df_cuadrante_planuno.itertuples(index=False)
    res = {r.customer_id: r.cuadrante_planuno for r in df}

    assert res[1] == "BAJAS"
    assert res[2] == "3d3"
    assert res[3] == "Básico"

def test_get_riesgo_cliente(tmp_path):
    engine = ClaseEngine(str(tmp_path))

    engine.df = pd.DataFrame(
        [
            # ALTO (bajas = 1)
            (1, "Transaccional", "Exclusivos_3d3", 1, 10),
            # MEDIO (diferencia_dias > 365 y Transaccional)
            (2, "Transaccional", "Otros", 0, 400),
            # BAJO (Exclusivos_3d3)
            (3, "Básico", "Exclusivos_3d3", 0, 10),
            # BAJO default
            (4, "Básico", "Otros", 0, 10),
        ],
        columns=[
            "customer_id",
            "planuno_quadrant_name",
            "Exclusivos_3d3",
            "bajas",
            "diferencia_dias",
        ],
    )

    engine.get_riesgo_cliente()

    df = engine.df_riesgo.itertuples(index=False)
    res = {
        r.customer_id: (r.riesgo_cliente, r.score_riesgo)
        for r in df
    }

    assert res[1] == ("ALTO", 3)
    assert res[2] == ("MEDIO", 2)
    assert res[3] == ("BAJO", 1)
    assert res[4] == ("BAJO", 1)
