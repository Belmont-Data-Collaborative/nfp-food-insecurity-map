"""Geocode real NFP Community Meals Partners from the 2026 Partner Info Hub PDF.

Produces data/partners.geojson with real names, addresses, and coordinates.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from geopy.geocoders import Nominatim

PARTNERS = [
    ("Aventura Community School", "490 Metroplex Dr STE 100, Nashville, TN 37211"),
    ("Begin Anew - Crievewood", "480 Hogan Rd, Nashville, TN 37220"),
    ("Begin Anew - Woodbine", "29 Whitsett Rd, Nashville, TN 37210"),
    ("Boys and Girls Club - Andrew Jackson", "916 16th Ave N, Nashville, TN 37208"),
    ("Boys and Girls Club - Teen Center", "916 16th Ave N, Nashville, TN 37208"),
    ("Boys and Girls Club - Fairview", "1814 Fairview Blvd, Fairview, TN 37062"),
    ("Boys and Girls Club - Preston Taylor", "915 38th Ave N, Nashville, TN 37209"),
    ("Community Care Fellowship", "511 S 8th St, Nashville, TN 37206"),
    ("Colby's Army", "1394 George Boyd Rd, Ashland City, TN 37015"),
    ("CrossBridge", "335 Murfreesboro Pike, Nashville, TN 37210"),
    ("Crossroads", "707 Monroe St, Nashville, TN 37208"),
    ("Dismas House", "2424 Charlotte Ave, Nashville, TN 37203"),
    ("Ebenezer United Methodist Church", "6200 Robertson Ave, Nashville, TN 37209"),
    ("Edgehill Brighter Days", "1416 Edgehill Ave, Nashville, TN 37212"),
    ("ENP Freestore", "109 Lafayette St, Nashville, TN 37217"),
    ("Fifty Forward", "174 Rains Ave, Nashville, TN 37203"),
    ("Firefly", "719 Thompson Ln, Nashville, TN 37204"),
    ("Green Hills YMCA", "4041 Hillsboro Cir, Nashville, TN 37215"),
    ("Green Street", "146 Green St, Nashville, TN 37210"),
    ("Harvest Hands - Harding Place", "4732 W Longdale Dr, Nashville, TN 37211"),
    ("Harvest Hands - Napier", "155 Old Hermitage Ave, Nashville, TN 37210"),
    ("Harvest Hands - Teen Program", "155 Old Hermitage Ave, Nashville, TN 37210"),
    ("Harvest Hands - South End", "5042 Edmondson Pike, Nashville, TN 37211"),
    ("JCALA", "41 Tusculum Rd, Antioch, TN 37013"),
    ("Judge Dinkins Educational Center", "2013 25th Ave N, Nashville, TN 37208"),
    ("King's Academy", "394 Strasser Dr, Nashville, TN 37211"),
    ("Launchpad", "2846 Lebanon Pike, Nashville, TN 37214"),
    ("Madison Church of Christ - Benevolence Center", "106 Gallatin Pike N, Madison, TN 37115"),
    ("Napier Kitchen Table", "155 Lafayette St, Nashville, TN 37210"),
    ("Nations - Hillcrest", "5112 Raywood Lane, Nashville, TN 37211"),
    ("NeighborStand", "60 Lester Ave, Nashville, TN 37210"),
    ("Nueva Vida", "416 E Thompson Ln, Nashville, TN 37211"),
    ("Operation Stand Down", "1125 12th Ave South, Nashville, TN 37203"),
    ("Project Return", "109 Lafayette Street, Nashville, TN 37210"),
    ("Room in the Inn", "705 Drexel St, Nashville, TN 37203"),
    ("Samaritan Recovery", "319 S 4th St, Nashville, TN 37206"),
    ("The ARK", "710 US-70, Pegram, TN 37143"),
    ("Trinity Community Commons", "204 East Trinity Lane, Nashville, TN 37207"),
    ("UpRise Nashville", "235 White Bridge Pike, Nashville, TN 37209"),
    ("UrbanPromise Nashville", "255 Haywood Lane, Nashville, TN 37211"),
    ("Urban Housing Solutions", "2131 26th Ave N, Nashville, TN 37208"),
    ("Water Walkers", "1300 South Street, Nashville, TN 37212"),
    ("Worker's Dignity", "335 Whitsett Rd, Nashville, TN 37210"),
]

PARTNER_TYPE = "community_meals"


def main():
    geolocator = Nominatim(user_agent="nfp_food_insecurity_map_geocoder")
    features = []
    failed = []

    for i, (name, address) in enumerate(PARTNERS):
        query = f"{address}, Davidson County, TN, USA"
        print(f"[{i+1}/{len(PARTNERS)}] Geocoding: {name} — {address}")
        try:
            location = geolocator.geocode(query, timeout=10)
            if location is None:
                # Try without Davidson County constraint
                location = geolocator.geocode(address, timeout=10)
            if location:
                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [location.longitude, location.latitude],
                    },
                    "properties": {
                        "partner_name": name,
                        "address": address,
                        "partner_type": PARTNER_TYPE,
                        "geocode_status": "success",
                    },
                })
                print(f"  -> ({location.latitude:.5f}, {location.longitude:.5f})")
            else:
                failed.append(name)
                print(f"  -> FAILED (no result)")
        except Exception as e:
            failed.append(name)
            print(f"  -> FAILED ({e})")
        time.sleep(1.1)  # Nominatim rate limit

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    out_path = Path("data/partners.geojson")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, indent=2)

    print(f"\nDone: {len(features)} geocoded, {len(failed)} failed")
    if failed:
        print(f"Failed: {failed}")
    print(f"Written to {out_path}")


if __name__ == "__main__":
    main()
