import pandas as pd


def check_tables(tables):
    """Revisa si se encontraron tablas válidas."""
    if not isinstance(tables, list) or len(tables) == 0:
        print("No se encontraron tablas o el valor no es válido.")
        exit()
    print(f"Tablas encontradas: {tables}")


def check_lengths(dataframe, max_length, limit=10):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    for column in dataframe.columns:
        if dataframe[column].apply(lambda x: len(str(x)) > max_length).any():
            print(f"Columna: {column} contiene valores demasiado largos:")
            print(dataframe[dataframe[column].apply(
                lambda x: len(str(x)) > max_length)].head(limit))
            print("\n")


def max_column_sizes(df):
    column_sizes = df.applymap(lambda x: len(str(x))).max()
    sorted_sizes = column_sizes.sort_values(ascending=False)
    print(sorted_sizes)
