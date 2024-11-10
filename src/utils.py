import geopandas as gpd
import osmnx as ox
import os

def load_bairros(url: str) -> gpd.GeoDataFrame:
    """
    Loads bairros data from a given url and renames the columns.
    
    Parameters
    ----------
    url : str
        The url to load the data from.
    
    Returns
    -------
    gpd.GeoDataFrame
        A GeoDataFrame with the bairros data, with the columns "nome_bairro" 
        and "geometry".
    """
    cols = ["NOME", "geometry"]
    bairros = gpd.read_file(url)
    bairros = bairros[cols].rename(columns={"NOME": "bairro"})
    return bairros

def load_vias_cwb(url: str) -> gpd.GeoDataFrame:
    """
    Loads vias data from a given url and renames the columns.
    
    Parameters
    ----------
    url : str
        The url to load the data from.
    
    Returns
    -------
    gpd.GeoDataFrame
        A GeoDataFrame with the vias data, with the columns "nome_via", 
        "tipo_via_cwb", "tipo_via_ctb", and "geometry".
    """
    cols = ["NMVIA", "SVIARIO", "HIERARQUIA", "geometry"]
    vias = gpd.read_file(url, encoding="latin1")
    vias = vias[cols].rename(
        columns={
            "NMVIA": "nome_via",
            "SVIARIO": "tipo_via_cwb",
            "HIERARQUIA": "tipo_via_ctb"
        }
    )
    return vias

def import_osm(place: str) -> gpd.GeoDataFrame:
    """
    Downloads the OSM road network for a given place and saves it to a file, 
    if the file does not already exist. Then, reads the file into a GeoDataFrame.
    
    Parameters
    ----------
    place : str
        The place to download the OSM road network for.
    
    Returns
    -------
    gpd.GeoDataFrame
        The GeoDataFrame with the OSM road network data.
    """
    file = "data/osmaxis/edges.shp"
    if os.path.exists(file):
        print("osmdata already downloaded. Loading now...")
    else:
        LOCATION = place
        NETWORK = "drive"
        osmaxis = ox.graph_from_place(LOCATION, network_type=NETWORK)
        ox.save_graph_shapefile(osmaxis, "data/osmaxis")
    cols = ["maxspeed", "geometry"]
    axis = gpd.read_file(file)
    axis = axis[cols].rename(columns={"maxspeed": "spd_limit"})

    return axis
