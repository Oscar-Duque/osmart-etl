import pandas as pd

# def tag_issue(row):
#     # import pdb; pdb.set_trace()
#     if row["efectivo_in"] > 0 and row["tarjeta_in"] == 0 and row["pagado"] > row["total"]:
#         return "overpaid cash"
#     elif row["tarjeta_in"] > 0 and row["efectivo_in"] == 0 and row["pagado"] > row["total"]:
#         return "overpaid card"
#     elif row["cobranza_aplicada"] > 0 and row["efectivo_in"] == 0 and row["tarjeta_in"] == 0:
#     #     return "only cobranza"
#     elif row["pagado"] == 0:
#         return "no payment recorded"
#     elif row["egresos"] > row["efectivo_in"] + row["tarjeta_in"] + row["otros_in"]:
#         return "refund_too_big"
#     else:
#         return "unknown mismatch"
    
def tag_issue(row):
    # import pdb; pdb.set_trace()
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
    df.columns = [col.lower() for col in df.columns]
    df["egresos"] = df["egresos"].fillna(0)
    df["cobranza_aplicada"] = df["cobranza_aplicada"].fillna(0)
    
    # # Edge Case 1. Sale exists in ventas but not in flujo → DELETE, but keep a log
    # dropped_df = df[df["efectivo_in"] + df["tarjeta_in"] + df["otros_in"] == 0].copy()
    # df = df[df["efectivo_in"] + df["tarjeta_in"] + df["otros_in"] > 0].copy()
    
    # # Edge Case 2. Refund is too big, if egresos is more than total ingresos (cash + card + other), ignore the egreso
    # total_ingresos = (df["efectivo_in"] + df["tarjeta_in"] + df["otros_in"])
    # df.loc[df["egresos"] > total_ingresos, "egresos"] = 0
    
    # # Edge Case 3. The efectivo amount is correct, but sometimes a random cobranza_aplicada is associated to the sale and shouldn't be
    # # Fix: Ignore cobranza_aplicada if efectivo already covers the total
    # df.loc[df["efectivo_in"] >= df["total"].round(2), "cobranza_aplicada"] = 0
    
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
    # df["pagado"] = df["efectivo_in"] + df["tarjeta_in"] + df["otros_in"] + df["cobranza_aplicada"] - df["egresos"]
    # df["pagado"] = df["efectivo_in"] + df["tarjeta_in"] + df["otros_in"]
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
        # "tarjeta_in": "tarjeta",
        # "efectivo_in": "efectivo"
    }
    df.rename(columns = column_map, inplace=True)
    
    # Drop data cleaning columns from results
    df["tienda"] = store
    df["otros"] = df["otros_in"] + df["cobranza_aplicada"]

    cleaned_df = df.drop(columns=["pago_completo", "pago_excedente", "pago_incompleto", "otros_in", "cobranza_aplicada", "egresos", "pagado", "fecha", "usuhora", "tarjeta_in", "tarjeta_in"])

    return {
        "clean": cleaned_df,
        "qa": merged_df,
        "dropped": dropped_df
    }