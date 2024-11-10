from src import ndsbr
from src import utils
import pandas as pd

def main() -> None:
    
    nds_path = "data_raw/FullTable_AO_AP_AQ_AR_AS_AT_AU_AV.csv"
    bairros_url = "https://ippuc.org.br/geodownloads/SHAPES_SIRGAS/DIVISA_DE_BAIRROS_SIRGAS.zip"
    vias_url = "https://ippuc.org.br/geodownloads/SHAPES_SIRGAS/EIXO_RUA_SIRGAS.zip"

    print("Loading bairros and vias data...")
    bairros = utils.load_bairros(bairros_url)
    vias = utils.load_vias_cwb(vias_url)
    osm_vias = utils.import_osm("Curitiba, Brazil")

    print("Loading ndsbr data...")
    nds_data = pd.read_csv(nds_path, sep=";", low_memory=False)

    print("Cleaning ndsbr data...")
    nds_data_cleaned = ndsbr.clean_cols(nds_data)
    nds_data_cleaned = ndsbr.create_datetime("date", "time", nds_data_cleaned)

    print("Adding spatial data...")
    nds_spatial = ndsbr.create_spatial_data(nds_data_cleaned)
    nds_sample = ndsbr.join_bairros_data(nds_spatial, bairros)
    nds_sample = ndsbr.join_vias_data(nds_sample, vias)
    nds_sample = ndsbr.join_vias_data(nds_sample, osm_vias)
    nds_sample = ndsbr.fill_missing_speed(nds_sample)
    nds_sample = ndsbr.fix_col_order(nds_sample)

    print("Saving ndsbr data...")
    nds_sample.info(show_counts=True, verbose=True)
    nds_sample.to_csv("data/ndsbr.csv", index=False)
    nds_sample.to_parquet("data/ndsbr.parquet")
    #nds_sample.to_file("data/ndsbr.geojson", driver="GeoJSON")

if __name__ == "__main__":
    main()