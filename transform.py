import pandas as pd
    
def tag_issue(row):
    if row["efectivo"] > 0 and row["tarjeta"] == 0 and row["pagado"] > row["total"]:
        return "overpaid cash"
    elif row["tarjeta"] > 0 and row["efectivo"] == 0 and row["pagado"] > row["total"]:
        return "overpaid card"
    elif row["pagado"] == 0:
        return "no payment recorded"
    elif row["egresos"] > row["efectivo"] + row["tarjeta"] + row["otros"]:
        return "refund_too_big"
    else:
        return "unknown mismatch"

def clean_and_standardize_legacy(df, store):
    # Edge Case 1. Sale exists in ventas but not in flujo → DELETE, but keep a log
    dropped_df = df[df["efectivo_in"] + df["tarjeta_in"] + df["otros_in"] == 0].copy()
    df = df[df["efectivo_in"] + df["tarjeta_in"] + df["otros_in"] > 0].copy()
    
    # Calculate "efectivo", "tarjeta" and "otros"
    df["efectivo"] = df[["efectivo_in", "total"]].min(axis=1)
    df["resto"] = df["total"] - df["efectivo"] # Remaining amount after efectivo
    df["tarjeta"] = df[["tarjeta_in", "resto"]].min(axis=1)
    df["otros"] = df["total"] - df["efectivo"] - df["tarjeta"] # Remaining after efectivo + tarjeta
    
    # Clip otros at 0 to avoid negatives due to rounding
    df["otros"] = df["otros"].clip(lower=0)
    df.drop(columns=["resto"], inplace=True)
    
    # QA columns
    df["pagado"] = df["efectivo"] + df["tarjeta"] + df["otros"]
    df["pago_completo"] = df["pagado"].round(2) == df["total"].round(2)
    df["pago_excedente"] = df["pagado"].round(2) > df["total"].round(2)
    df["pago_incompleto"] = df["pagado"].round(2) < df["total"].round(2)

    # Build table of anormalities and tag “Known Issue” Types
    merged_df = df[~df["pago_completo"] | df["pago_excedente"] | df["pago_incompleto"]].copy()
    
    if not merged_df.empty:
        merged_df["issue_type"] = merged_df.apply(tag_issue, axis=1)
    
    # Combine fecha and usuhora to create datetime column
    df["fecha_hora"] = pd.to_datetime(df["fecha"] + " " + df["usuhora"], errors="coerce")
    
    # Rename legacy columns to match new schema
    column_map = {
        "venta": "ven_id",
        "total": "total_venta"
    }
    df.rename(columns = column_map, inplace=True)
    
    # Drop data cleaning columns from results
    # df["tienda"] = store
    df["otros"] = df["otros_in"] + df["cobranza_aplicada"]

    cleaned_df = df[["ven_id", "tienda", "fecha_hora", "caja", "usuario", "efectivo", "tarjeta", "otros", "total_venta", "source_db", "source_system", "extracted_at"]]

    return {
        "clean": cleaned_df,
        "qa": merged_df,
        "dropped": dropped_df
    }