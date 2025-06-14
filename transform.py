import pandas as pd
    
def tag_issue(row):
    if row["no_flujo"]:
        return "no payment recorded"
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
    # Flag rows where no flujo is present
    df["no_flujo"] = (df["efectivo_in"] + df["tarjeta_in"] + df["otros_in"] == 0)
    
    # Base calculation of efectivo, tarjeta, otros
    df["efectivo"] = df[["efectivo_in", "total"]].min(axis=1)
    df["resto"] = df["total"] - df["efectivo"] # Remaining amount after efectivo
    df["tarjeta"] = df[["tarjeta_in", "resto"]].min(axis=1)
    df["otros"] = df["total"] - df["efectivo"] - df["tarjeta"] # Remaining after efectivo + tarjeta
    
    # Override for no flujo → assume all cash
    df.loc[df["no_flujo"], "efectivo"] = df.loc[df["no_flujo"], "total"]
    df.loc[df["no_flujo"], "tarjeta"] = 0
    df.loc[df["no_flujo"], "otros"] = 0
    
    # Clip otros at 0 to avoid negatives due to rounding
    df["otros"] = df["otros"].clip(lower=0)
    df.drop(columns=["resto"], inplace=True)
    
    # QA columns
    df["pagado"] = df["efectivo"] + df["tarjeta"] + df["otros"]
    df["pago_completo"] = df["pagado"].round(2) == df["total"].round(2)
    df["pago_excedente"] = df["pagado"].round(2) > df["total"].round(2)
    df["pago_incompleto"] = df["pagado"].round(2) < df["total"].round(2)

    # Build QA dataframe with mismatches + no_flujo
    qa_df = df[~df["pago_completo"] | df["pago_excedente"] | df["pago_incompleto"] | df["no_flujo"]].copy()
    
    if not qa_df.empty:
        qa_df["issue_type"] = qa_df.apply(tag_issue, axis=1)
    
    # Combine fecha and usuhora to create datetime column
    df["fecha_hora"] = pd.to_datetime(df["fecha"] + " " + df["usuhora"], errors="coerce")
    
    # Rename columns to match target schema
    df.rename(columns={"venta": "ven_id", "total": "total_venta"}, inplace=True)
    
     # Adjust otros to include cobranza_aplicada (unless no_flujo → force to 0)
    df["otros"] = df["otros_in"] + df["cobranza_aplicada"]
    df.loc[df["no_flujo"], "otros"] = 0

    cleaned_df = df[["ven_id", "tienda", "fecha_hora", "caja", "usuario", "efectivo", "tarjeta", "otros", "total_venta", "source_db", "source_system", "extracted_at"]]

    return {
        "clean": cleaned_df,
        "qa": qa_df
    }