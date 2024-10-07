# FLUXNET2015 as Zarr store

Total Zarr store size: 125 MB --> more variables can still be included on GitHub.


## How to reproduce

Download FLUXNET fullset for all sites.

Get page source, paste into links.txt. Then:
```
cat links.txt | grep -oP 'https(\S*)bschilperoort' > stripped_links.txt
xargs -n 1 curl -O < stripped_links.txt
```

Create a python virtual environment:
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Then run the processor:
```
python3 create_zarr.py
```
