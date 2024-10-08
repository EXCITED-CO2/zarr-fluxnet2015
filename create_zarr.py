import process_fluxnet
from pathlib import Path
import datetime
from numcodecs import Blosc

zip_folder = Path("/data/fluxnet/zips/")
metadata_file = Path("/data/fluxnet/zips/FLX_AA-Flx_BIF_HH_20200501.xlsx")

vars_attrs = {
    "TA_F": {"name": "ta", "long_name": "air temperature", "units": "degC"},
    "TA_F_QC": {"name": "ta_qc", "long_name": "air temperature quality flag"},
    "SW_IN_F": {
        "name": "sw_in",
        "long_name": "incoming shortwave radiation",
        "units": "W m-2",
    },
    "SW_IN_F_QC": {
        "name": "sw_in_qc",
        "long_name": "incoming shortwave radiation quality flag",
    },
    "VPD_F": {"name": "vpd", "long_name": "vapor pressure deficit", "units": "kPa"},
    "VPD_F_QC": {"name": "vpd_qc", "long_name": "vapor pressure deficit quality flag"},
    "GPP_NT_VUT_REF": {
        "name": "gpp_nt",
        "long_name": "gross primary production (nighttime partitioning method)",
        "units": "umolCO2 m-2 s-1",
    },
    "GPP_DT_VUT_REF": {
        "name": "gpp_dt",
        "long_name": "gross primary production (daytime partitioning method)",
        "units": "umolCO2 m-2 s-1",
    },
    "NEE_VUT_REF": {
        "name": "nee",
        "long_name": "net ecosystem exchange",
        "units": "umolCO2 m-2 s-1",
    },
    "NEE_VUT_REF_QC": {
        "name": "nee_qc",
        "long_name": "net ecosystem exchange quality flag",
    },
}

dataset_attrs = {
    "title": "FLUXNET2015 Dataset",
    "description": (
        "The FLUXNET2015 Dataset includes data collected at sites from multiple"
        " regional flux networks. The preparation of this FLUXNET Dataset has been"
        " possible thanks only to the efforts of many scientists and technicians "
        "around the world and the coordination among teams from regional networks."
    ),
    "Conventions": "None",
    "license": "CC-BY-4.0",
    "history": (
        "Converted from FULLSET FLUXNET2015 to Zarr on "
        f"{datetime.datetime.now(datetime.UTC)}."
    ),
    "citation": (
        "Pastorello, G., Trotta, C., Canfora, E. et al. The FLUXNET2015 dataset and "
        "the ONEFlux processing pipeline for eddy covariance data. Sci Data 7, "
        "225 (2020)."
    ),
    "doi": "https://doi.org/10.1038/s41597-020-0534-3",
    "attribution_guidelines": (
        "- Cite the data-collection paper, i.e., cite Pastorello et al. 20201.\n"
        "- List each site used by its FLUXNET ID and/or per-site DOIs in the paper."
    ),
}

compressor = Blosc(cname="zstd", clevel=3, shuffle=2)


def get_encoding(ds):
    enc = {}
    for var in ds.data_vars:
        vmin = ds[var].min().compute().item()
        vmax = ds[var].max().compute().item()
        range = vmax - vmin
        enc[var] = {
            "compressor": compressor,
            "dtype": "uint16",
            "scale_factor": range / 65532,
            "add_offset": vmin,
            "_FillValue": 65535,
        }
    return enc


if __name__ == "__main__":
    ds = process_fluxnet.preprocess_fluxnet_sites(
        zip_folder, metadata_file, vars_attrs.keys()
    )

    print("Setting attributes")
    for var in vars_attrs:
        new_name = vars_attrs[var].pop("name")
        ds = ds.rename_vars({var: new_name})
        ds[new_name].attrs = vars_attrs[var]

    ds["site"].attrs = {"long_name": "Fluxnet site code"}
    ds["longitude"].attrs = {
        "long_name": "Fluxnet site longitude",
        "units": "degrees_east",
    }
    ds["latitude"].attrs = {
        "long_name": "Fluxnet site latitude",
        "units": "degrees_north",
    }
    ds = ds.chunk({"time": -1, "site": 1})

    ds.attrs = dataset_attrs

    print("Writing to zarr")
    ds.to_zarr(
        store="fluxnet.zarr",
        encoding=get_encoding(ds),
        mode="w",
    )
