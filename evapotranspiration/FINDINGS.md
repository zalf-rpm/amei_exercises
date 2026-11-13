# ET study, Phase 1: findings and open questions

## Setup

```
pixi run python evapotranspiration/run_et.py --site bushland|avignon [--treatments ...] [--monica-run ...]
pixi run python evapotranspiration/evaluate_et.py --site bushland|avignon
```

- `site_bushland.py`, `site_avignon.py`: read the site's xlsx into site-independent `Setup`s
  (weather, soil, initial conditions, management, observations). All site-specific assumptions
  live there.
- `et_monica.py`: `Setup` -> MONICA inputs, `monica-run` (C++ AMEI branch by default), output
  parsing, AMEI templates.
- `out_<site>/runs/<id>/`: everything MONICA got and returned, per treatment (not in git;
  `cd` there and `monica-run sim.json` repeats a run). `out_<site>/results/`: the template files
  for submission. `out_<site>/evaluation.{png,csv}`: comparison with the measured ET.
- Model: MONICA FAO56-PM, `MOFAPM`, "Orig"; default winter wheat cultivar, uncalibrated.
- Environment parameters: `general/environment.json` with the site's values on top. (The first
  Bushland version replaced the include with just three values, so the rest silently fell back to
  MONICA's built-in defaults. Fixed; emergence moved by ~5 days.)

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
   `crop-module.cpp:3755`). That's the flat ETa plateau late in the season at both sites, while
   the Bushland lysimeter shows 10-13 mm/d. The reported `Pot_ET` (EOAD/ETP) is *not* capped
   (max 14 mm/d), so output and water balance disagree. Keep for "Orig", remove for "Imp"?
2. **`ET0` changes meaning at emergence.** Without a crop it's the soil moisture module's FAO56-PM
   grass reference (rs = 100/1.44 s/m, uses vapor pressure if given). With a crop
   (`soilmoisture.cpp:866`) it's the crop module's ET0, with a stomatal resistance from CO2,
   saturation deficit and photosynthesis, and only relative humidity. TXBL1990F: crop ET0 1149 mm
   vs FAO56 module 1211 mm over the season, up to ~0.9 mm/d on single days. If the study wants
   the FAO56 reference for ETO: run each treatment a second time without a crop and take ET0 from
   there (that's MONICA's own FAO56 module, same weather).
3. `Pot_ET` is ET0 · Kc and goes into both EOAD and ETP. ETOS/ETRS (ASCE short/tall) are NA.

## Data issues worth reporting to the data providers

- **Avignon `VPRSD` is the vapor pressure *deficit*, not the vapor pressure** the Definitions
  sheet says (kPa, "Vapor pressure, average daily"): correlation 0.98 with es - ea(TDEW), ~0 on
  days with 90-100 % RH, mean 0.76 kPa vs ea(TDEW) 1.18 kPa. Bushland's `VPRSD` is fine
  (correlation 1.000 with ea(TDEW)). Any model taking Avignon's VPRSD as ea gets a wrong ET0.
- Avignon `RHUMD` is the daily *minimum* RH (Bushland: the mean).
- Bushland treatment ids differ between sheets: TXBL1990F (Treatments, Plots, Planting,
  Fertilizer) vs TXBL1989F (Irrigation, Obs, ET_Daily) for the same season.
- Avignon: the soil profile's `SOIL_ID` is "calcaric fluvisol", Metadata refers to "FRAV".
- The daily template's name row has one column more than the code/unit rows
  (`soil_temp_surface_daily_avg`, no code); the code row is followed.

## Bushland setup

- Ids of the Treatments sheet; the other sheets' ids matched by prefix/suffix and sowing or
  harvest year.
- Weather as given (`RHUMD` mean RH, `VPRSD` kPa, wind at 2 m); one `RAIN` column, though the
  comments describe `RAIN_PART`/`RAIN_FULL`.
- `SLPHW` missing below 160 cm: taken from the horizon above. 42 layers = 2.1 m of the 2.4 m.
- Initial moisture above FC in 1992 (deep layers) and 1993 (5-70 cm): set to FC.
- Initial N: only `ICIN` = 25 kg N/ha, spread evenly over 2.1 m. Nitrogen response is off.
- Anhydrous ammonia as 100 % NH4. Harvest on the given dates.
- `WaterDeficitResponseOn` is **true**; `sim_full_run.json` has it false, which would make the
  partial irrigation treatments meaningless.
- SWWPD is the profile **mean** of `Mois` (template unit cm3/cm3); `sim_full_run.json` sums the
  layers, which gives no meaningful unit.
- `read_agmip_file.py` reads `IRVAL` with `int()` (10.21 mm -> 10 mm); the scripts don't, the
  flow component still does.

## Avignon setup

- Treatment id = `EXP_TRT_NAME` (FRAV_2004 ...), EXPTRNO in the run log. Which one does the
  protocol want in file names and TREAT_ID?
- Simulation from the initial conditions date (= sowing) to July 15 of the harvest year. No
  harvest dates in the data: **automatic harvest at maturity, at the latest on July 15**.
- Weather: TAVD = (TMAX + TMIN) / 2; ea from TDEW and mean RH = ea / es (see VPRSD above); wind
  brought from the daily `WINDHT` (2-4 m) to 2 m (FAO56 eq. 47). No CO2 given: MONICA's own by
  year. No soil albedo: environment.json's.
- Soil: 4 horizons to 190 cm (the last one extended from 160 cm by the providers); 38 layers.
  No pH, stones or C/N: MONICA defaults. Initial N from `NOSLD_kgperha`/`NHSLD_kgperha` per
  horizon.
- Initial moisture slightly above FC in a few layers (2004, 2010, 2012): set to FC. 2012 has
  only 4 initial moisture layers, simulated by the providers (SIMPLACE Lintul-5).
- Fertilizer FE001 (ammonium nitrate) as 50 % NH4 / 50 % NO3; the P/K applications (FE009)
  are skipped. Previous crop residues (ICRAG) and tillage are not used.
- Cultivars are durum wheat (Artimon, Acalou, Dakter, Isildur); simulated with the generic
  winter wheat. The comments give vernalization/photosensitivity scores.
- Observed ET: eddy covariance; 24 h values (`ETDAILY_100`, else `_90`) are compared, the much
  more frequent 6-18 h values (`ETDAY_*`) are only plotted. 2012 has 6 days of 24 h ET.
- Phenology: where ADAT/MDAT are missing, estimated from BBCH as the comments suggest (anthesis
  4 days before BBCH 69 or 150 °C d after BBCH 55/58; maturity at BBCH 89, else 87).
- Yield: `HWAH_machine`, as recommended in the comments.

## First results (default winter wheat cultivar, uncalibrated)

Bushland (lysimeter, 24 h):

| | ETa obs / sim (mm) | bias mm/d | anthesis obs / sim | LAI max obs / sim |
|---|---|---|---|---|
| TXBL1990F | 825 / 698 | -0.54 | 05-16 / 06-05 | 3.6 / 11.6 |
| TXBL1990P | 444 / 433 | -0.05 | 05-15 / 06-01 | 2.4 / 10.5 |
| TXBL1992F | 930 / 801 | -0.48 | 05-09 / 05-25 | 7.1 / 12.7 |
| TXBL1992P | 719 / 749 | 0.12 | - / 05-24 | 5.9 / 12.7 |
| TXBL1993F | 907 / 766 | -0.57 | 05-18 / 06-02 | 4.1 / 12.9 |
| TXBL1993P | 799 / 747 | -0.21 | 05-18 / 06-02 | 4.0 / 12.9 |

Avignon (eddy covariance, 24 h days only):

| | days | ETa obs / sim (mm) | bias mm/d | anthesis obs / sim | LAI max obs / sim | yield obs / sim |
|---|---|---|---|---|---|---|
| FRAV_2004 | 49 | 122 / 109 | -0.25 | 04-28 (est.) / 05-28 | 2.7 / 8.7 | 6660 / 6190 |
| FRAV_2006 | 46 | 96 / 45 | -1.11 | 05-15 / 05-21 | 5.5 / 8.5 | 4500 / 5957 |
| FRAV_2010 | 35 | 111 / 97 | -0.40 | 05-22 (est.) / 05-29 | 6.4 / 7.5 | 7030 / 8408 |
| FRAV_2012 | 6 | 12 / 10 | -0.43 | 05-06 (est.) / 05-23 | 1.2 / 10.5 | 5080 / 7620 |

Bushland: anthesis 2-3 weeks late, maturity mostly not reached before harvest, LAI ~3x too high,
root depth up to 3.9 m (profile is 2.1 m). ETa underestimated mainly late in the season (6.5 mm
cap, and the crop keeps transpiring while the real one senesces). Avignon: anthesis 1-4 weeks
late, LAI too high as well; ETa mostly fine in winter/spring, the 6.5 mm cap shows from May.

## Open questions

- Phenology/LAI: calibrate per cultivar (Bushland: TAM200, TAM107, Mesa; Avignon: four durum
  cultivars) with the calibration flow, or is "Orig" meant uncalibrated?
- 6.5 mm/d cap: part of "Orig", removed in "Imp"?
- ETO column: crop module ET0 as now, or the FAO56 module via a crop-free run?
- Avignon: EXPTRNO or EXP_TRT_NAME as treatment id? Use residues/tillage?
- Fix the capnp soil moisture / climate gaps in `monica` (master) and `monica_odin` as well?
- `Monica_AMEI_potential_Evapotranspiration` isn't integrated yet; "Imp" runs would select it via
  `EvapotranspirationModel` in sim.json.
