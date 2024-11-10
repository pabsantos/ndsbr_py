import pandas as pd
import geopandas as gpd

def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and transforms the given DataFrame for NDSBR data processing.

    This function selects specific columns from the DataFrame, renames them
    to lowercase, and converts certain columns to appropriate data types.
    Additionally, it combines the 'date' and 'time' columns to create a
    'datetime' column.

    Args:
        df (pd.DataFrame): The input DataFrame containing raw NDSBR data.

    Returns:
        pd.DataFrame: A DataFrame with cleaned and transformed NDSBR data.
    """
    COLS = [
        "DRIVER", "LONG", "LAT", "DAY", "TRIP", "ID", "PR", "TIME_ACUM",
        "SPD_KMH", "VALID_TIME", 'TIMESTAMP', 'ACEL_MS2'
    ]

    df = df[COLS]
    df = df.rename(
        columns={
            "DRIVER": "driver",
            "LONG": "long",
            "LAT": "lat",
            "DAY": "date",
            "TRIP": "trip",
            "ID": "id",
            "PR": "time",
            "TIME_ACUM": "time_acum",
            "SPD_KMH": "spd_kmh",
            "VALID_TIME": "valid_time",
            "ACEL_MS2": "acel_ms2"
        }
    )

    def to_float(df: pd.DataFrame, col: str) -> pd.Series:
        return df[col].str.replace(",", ".").astype(float)
    
    for col in ["long", "lat", "spd_kmh", 'acel_ms2']:
        df[col] = to_float(df, col)

    return df

def create_datetime(
        date_col: str, time_col: str, df: pd.DataFrame
    ) -> pd.DataFrame:

    """
    Combines date and time columns into a single datetime column in the DataFrame.

    This function takes two column names representing date and time in the 
    DataFrame, combines them into a single datetime string, and converts it 
    into a pandas datetime object. The resulting datetime column is added 
    to the DataFrame.

    Args:
        date_col (str): The name of the column containing date values.
        time_col (str): The name of the column containing time values.
        df (pd.DataFrame): The input DataFrame containing the date and time columns.

    Returns:
        pd.DataFrame: A DataFrame with an added 'datetime' column.
    """
    df["datetime"] = df[date_col] + " " + df[time_col]
    df["datetime"] = pd.to_datetime(df["datetime"], format='%d/%m/%Y %H:%M:%S')
    df[date_col] = pd.to_datetime(df[date_col], format='%d/%m/%Y')

    return df

def create_spatial_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts longitude and latitude columns in the DataFrame into a GeoDataFrame.

    This function takes a DataFrame with longitude and latitude columns and 
    creates a GeoDataFrame with a geometry column containing Point geometries 
    constructed from these coordinates. The GeoDataFrame uses the specified 
    coordinate reference system (CRS).

    Args:
        df (pd.DataFrame): The input DataFrame containing 'long' and 'lat' columns.

    Returns:
        pd.DataFrame: A GeoDataFrame with a 'geometry' column of Point objects.
    """
    nds_geom = gpd.points_from_xy(df["long"], df["lat"])
    nds_spatial = gpd.GeoDataFrame(df, geometry=nds_geom, crs=4674)
    return nds_spatial

def join_bairros_data(
        ndsbr_data: gpd.GeoDataFrame, bairros_data: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
    """
    Spatially joins NDSBR data with Curitiba's neighborhoods data.

    This function takes two GeoDataFrames, one containing NDSBR data and the
    other containing Curitiba's neighborhoods data, and performs a spatial join
    of the two datasets. The resulting GeoDataFrame contains a column
    indicating which neighborhood each point belongs to.

    If the two input GeoDataFrames do not have the same coordinate reference
    system (CRS), the function will convert the neighborhoods data to the CRS
    of the NDSBR data.

    Args:
        ndsbr_data (gpd.GeoDataFrame): A GeoDataFrame containing NDSBR data.
        bairros_data (gpd.GeoDataFrame): A GeoDataFrame containing Curitiba's
            neighborhoods data.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame resulting from the spatial join of the
            two input datasets.
    """
    if ndsbr_data.crs != bairros_data.crs:
        print("Fixing CRS...")
        bairros_data = bairros_data.to_crs(ndsbr_data.crs)
    gdf = gpd.sjoin(bairros_data, ndsbr_data, how="right", predicate="contains")
    # Remove index_left
    gdf = gdf.drop(columns=["index_left"], axis=1)
    return gdf

def join_vias_data(
        ndsbr_data: gpd.GeoDataFrame, vias_data: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
    """
    Spatially joins NDSBR data with Curitiba's streets data.

    This function takes two GeoDataFrames, one containing NDSBR data and the
    other containing Curitiba's streets data, and performs a spatial join of
    the two datasets. The resulting GeoDataFrame contains a column indicating
    which street each point belongs to. If the two input GeoDataFrames do not
    have coordinate reference system (CRS) Sirgas 2000 UTM 22S, the function
    will convert to the required CRS

    Args:
        ndsbr_data (gpd.GeoDataFrame): A GeoDataFrame containing NDSBR data.
        vias_data (gpd.GeoDataFrame): A GeoDataFrame containing Curitiba's
            streets data.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame resulting from the spatial join of the
            two input datasets.
    """
    if ndsbr_data.crs != 31982:
        print("Fixing CRS (ndsbr)...")
        ndsbr_data = ndsbr_data.to_crs(31982)
    if vias_data.crs != 31982:
        print("Fixing CRS (vias)...")
        vias_data = vias_data.to_crs(31982)
    gdf = gpd.sjoin_nearest(ndsbr_data, vias_data, how="left", max_distance=20)
    gdf = gdf.to_crs(4674).drop(columns=["index_right"], axis=1)
    return gdf

def fill_missing_speed(ndsbr_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Fills missing speed limit values in the GeoDataFrame based on the type of
    road (tipo_via_ctb). If the speed limit is missing, it is replaced with the
    following values:

    - 1: 70 km/h
    - 2: 60 km/h
    - 3: 40 km/h
    - 4: 30 km/h

    Args:
        ndsbr_data (gpd.GeoDataFrame): The input GeoDataFrame containing the
            speed limit column.

    Returns:
        gpd.GeoDataFrame: The input GeoDataFrame with the missing speed limit
            values filled.
    """
    ndsbr_data["spd_limit"] = ndsbr_data["spd_limit"].fillna(
        ndsbr_data["tipo_via_ctb"].replace({
            "1": '70',
            "2": '60',
            "3": '40',
            "4": '30'
        })
    )
    return ndsbr_data
        

def fix_col_order(ndsbr_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Reorders the columns in the GeoDataFrame to a fixed order.
    
    This function takes a GeoDataFrame and reorders its columns to the following
    order:
    
    - id
    - driver
    - trip
    - long
    - lat
    - date
    - time
    - datetime
    - time_acum
    - spd_kmh
    - valid_time
    - nome_bairro
    - nome_via
    - tipo_via_cwb
    - tipo_via_ctb
    - spd_limit
    - geometry
    
    Args:
        ndsbr_data (gpd.GeoDataFrame): The input GeoDataFrame containing the
            columns to be reordered.
    
    Returns:
        gpd.GeoDataFrame: The input GeoDataFrame with the columns reordered.
    """
    
    cols = [
        'id', 'driver', 'trip', 'long', 'lat', 'date', 'time',
        'time_acum', 'spd_kmh', 'acel_ms2', 'valid_time',
        'nome_bairro', 'nome_via', 'tipo_via_cwb', 'tipo_via_ctb', 'spd_limit',
        'geometry'
    ]
    return ndsbr_data[cols]