# ET study, Phase 1: findings and open questions

Setup: `run_bushland.py` turns the AgMIP xlsx into MONICA inputs per treatment
(`out_bushland/runs/<TREAT_ID>/`), runs `monica-run` (C++ AMEI branch) and writes the AMEI
templates (`out_bushland/results/`). `evaluate_bushland.py` compares against the lysimeter
(`out_bushland/evaluation.png`, `evaluation.csv`). Model: MONICA FAO56-PM, `MOFAPM`, "Orig".

## Fixed

- **capnp soil profile, initial moisture** (`monica_amei` `src/run/capnp-helper.cpp`, commit
  `ba2b9e25` on `amei`): the schema's `soilMoisture` [vol%] became `SoilMoisturePercentFC =
  vol% / (FC * 100)`, i.e. ~1 % of field capacity instead of ~100 %. **Every flow-based run that
  sent an initial moisture (Maricopa via `read_agmip_file.py`, which derives it from SLAWC + SLLL)
  started with an almost dry profile.** Now `vol% / FC`, capped at 100.
  - `monica` (master) and `monica_odin` have a different wrong version: they pass the vol%
    straight on as % of FC (`capnp-helper.cpp:212` / `:231`). Not changed yet.
- **capnp climate: vapor pressure and CO2 were dropped** (same commit): `climateElementToACD`
  and `fromCapnpData` only knew tmin/tavg/tmax/precip/relhumid/wind/globrad. So in the flows
  - the measured vapor pressure never reached ET0; MONICA used relative humidity instead,
  - **the CO2 column `read_agmip_file.py` builds for the Maricopa FACE treatments was ignored**,
    unless the flow sets CO2 some other way (I didn't find one). Worth re-checking the Maricopa
    FACE results.
  - Vapor pressure is now converted from the schema's hPa to the kPa MONICA's ET0 uses.
  - Earlier I said the flow would feed vapor pressure ×10 into ET0. That was wrong: it never
    arrived at all. The ×10 effect (ET0 ≈ 0 without a crop) only shows if someone writes hPa into
    a climate CSV, where MONICA takes `vaporpress` as kPa without conversion.
  - `monica` (master) and `monica_odin` presumably have the same gap, not checked.
  - Only compiled and checked for no change on the CSV path; the capnp path itself has not been
    run since. `result/bin` (what the flows use) needs a `nix build` to pick it up.

## Model issues relevant to the ET study

1. **Potential ET is hard-capped at 6.5 mm/d** (`soilmoisture.cpp:897`,
   `crop-module.cpp:3755`). That's the flat ETa plateau late in the season, while the lysimeter
   shows 10-13 mm/d at Bushland. The reported `Pot_ET` (EOAD/ETP) is *not* capped (max 14 mm/d),
   so output and water balance disagree. Keep for "Orig", remove for "Imp"?
2. **`ET0` changes meaning at emergence.** Without a crop it's the soil moisture module's FAO56-PM
   grass reference (rs = 100/1.44 s/m, uses vapor pressure if given). With a crop
   (`soilmoisture.cpp:866`) it's the crop module's ET0, with a stomatal resistance from CO2,
   saturation deficit and photosynthesis, and only relative humidity. TXBL1990F: crop ET0 1149 mm
   vs FAO56 module 1211 mm over the season, up to ~0.9 mm/d on single days. If the study wants
   the FAO56 reference for ETO: run each treatment a second time without a crop and take ET0 from
   there (that's MONICA's own FAO56 module, same weather).
3. `Pot_ET` is ET0 · Kc and goes into both EOAD and ETP. ETOS/ETRS (ASCE short/tall) are NA.

## Bushland data and setup

- Treatment ids differ between sheets: TXBL1990F (Treatments, Plots, Planting, Fertilizer) vs
  TXBL1989F (Irrigation, Obs, ET_Daily). Matched by prefix/suffix and sowing or harvest year.
- Weather has `RHUMD` (Maricopa: `RHAVD`) and one `RAIN` column, though the comments describe
  `RAIN_PART`/`RAIN_FULL`.
- `SLPHW` missing below 160 cm: taken from the horizon above.
- Initial moisture above FC in 1992 (deep layers) and 1993 (5-70 cm): set to FC.
- Initial N: only `ICIN` = 25 kg N/ha, spread evenly over 2.1 m. Nitrogen response is off.
- `WaterDeficitResponseOn` is **true** here; `sim_full_run.json` has it false, which would make the
  partial irrigation treatments meaningless.
- SWWPD is the profile **mean** of `Mois` (template unit cm3/cm3); `sim_full_run.json` sums 42
  layers, which gives no meaningful unit.
- The daily template's name row has one column more than the code/unit rows
  (`soil_temp_surface_daily_avg`, no code); the code row is followed.
- `read_agmip_file.py` reads `IRVAL` with `int()`, cutting 10.21 mm to 10 mm. The script uses the
  exact values; the flow component still truncates.

## First results (default winter wheat cultivar, uncalibrated)

| | ETa obs / sim (mm) | bias mm/d | anthesis obs / sim | LAI max obs / sim |
|---|---|---|---|---|
| 1990F | 825 / 685 | -0.60 | 05-16 / 06-05 | 3.6 / 11.1 |
| 1990P | 444 / 436 | -0.04 | 05-15 / 06-02 | 2.4 / 10.2 |
| 1992F | 930 / 783 | -0.55 | 05-09 / 05-25 | 7.1 / 12.3 |
| 1992P | 719 / 730 | 0.04 | - / 05-24 | 5.9 / 12.3 |
| 1993F | 907 / 758 | -0.61 | 05-18 / 06-02 | 4.1 / 11.9 |
| 1993P | 799 / 738 | -0.24 | 05-18 / 06-02 | 4.0 / 11.9 |

Anthesis 2-3 weeks late, maturity mostly not reached before harvest, LAI ~3x too high, root depth
up to 3.9 m (profile is 2.1 m). ETa underestimated mainly late in the season (6.5 mm cap, and the
crop keeps transpiring while the real one senesces).

## Open questions

- Phenology/LAI: calibrate per cultivar (TAM200, TAM107, Mesa) with the calibration flow, or is
  "Orig" meant uncalibrated?
- 6.5 mm/d cap: part of "Orig", removed in "Imp"?
- ETO column: crop module ET0 as now, or the FAO56 module via a crop-free run?
- Fix the capnp soil moisture / climate gaps in `monica` (master) and `monica_odin` as well?
- `Monica_AMEI_potential_Evapotranspiration` isn't integrated yet; "Imp" runs would select it via
  `EvapotranspirationModel` in sim.json.
