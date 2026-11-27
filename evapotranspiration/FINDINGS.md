# ET study, Phase 1: findings and open questions

## Setup

```
pixi run run-et --site bushland|avignon [--treatments ...] [--monica-run ...]
pixi run evaluate-et --site bushland|avignon
evapotranspiration/calibration/calibrate.sh   # the Bushland and Avignon calibrations, step by step
```

The scripts are in `src/amei_exercises/` (since 2026-11-19; before in this folder): the ET study's
in `evapotranspiration/`, `et_monica.py`, `icasa.py` and `calibrate.py` shared with the soil
temperature study; `paths.py` points to the data folders. The pixi tasks run them as modules.

- `site_bushland.py`, `site_avignon.py`: read the site's xlsx into site-independent `Setup`s
  (weather, soil, initial conditions, management, observations). All site-specific assumptions
  live there.
- `et_monica.py`: `Setup` -> MONICA inputs, `monica-run`, output parsing, AMEI templates.
- MONICA: the Odin port by default (`~/GitHub/monica_odin/odin/build/monica-run`, branch
  `simplify_copilot`, built with `pixi run build monica-run` in `odin/`). `--monica-run` takes
  any other build; the template header records which one (`Model: MONICA (MOFAPM,
  monica_odin simplify_copilot@<commit>)`). See "MONICA builds" below.
- `checks.py`: reproduces the checks behind the data and ET0 statements below
  (`pixi run et-checks humidity`, `pixi run et-checks et0`).
- `out_<site>/runs/<id>/`: everything MONICA got and returned, per treatment (not in git;
  `cd` there and `monica-run sim.json` repeats a run). `out_<site>/results/`: the template files
  for submission. `out_<site>/evaluation.{png,csv}`: comparison with the measured ET.
- Model: MONICA FAO56-PM, `MOFAPM`, "Orig"; per site a calibrated winter wheat cultivar (since
  2026-11-18/19, see "Calibration" and "Avignon calibration"; before the default, uncalibrated).
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
    a climate CSV, where the AMEI branch takes `vaporpress` as kPa without conversion
    (`checks.py et0 --monica-run ~/GitHub/monica_amei/build/monica-run`: 39.2 -> 0.1 mm before
    emergence for TXBL1990F). Master and Odin don't use the vapor pressure at all (see below).
  - `monica` (master) and `monica_odin` presumably have the same gap, not checked.
  - Only compiled and checked for no change on the CSV path; the capnp path itself has not been
    run since. `result/bin` (what the flows use) needs a `nix build` to pick it up.

## Model issues relevant to the ET study

1. **Potential ET is hard-capped at 6.5 mm/d** (`// from HERMES`, no reason given). With a crop
   the cap is on crop ET0 × crop Kc (`crop_water_uptake.odin:143`), before interception,
   transpiration and soil evaporation are taken from it; without a crop on ET0 × Kc in soil
   moisture (`soil_moisture.odin:993`, applied again there on the crop's remaining ET). The
   water balance uses the capped value consistently. Only the reported `Pot_ET` (EOAD/ETP) is not
   capped: it is soil moisture's ET0 × Kc, on crop days the crop's ET0 and Kc
   (`soil_moisture.odin:192`), i.e. exactly the value before the cap (max 14 mm/d). So it's a
   reporting mismatch, not a water balance error.
   - More than 6.5 mm/d is real at Bushland: the lysimeter measures up to 12.0-13.5 mm/d with
     35-48 days > 6.5 mm in the fully irrigated seasons (partial irrigation: 0-35 days), and
     MONICA's own FAO-56 ET0 reaches 10-14 mm/d (semi-arid, windy, dry air, advection over the
     irrigated field). Avignon: max 3.1-7.9 mm/d, 0-6 days > 6.5. Presumably the cap comes from
     HERMES' humid temperate origin, where grass ET0 rarely exceeds 6-7 mm/d and a cap only
     catches outliers (a guess; ask the HERMES/MONICA authors).
   - It causes the flat ETa plateau late in the season, i.e. most of Bushland's -0.6 mm/d bias
     in the fully irrigated treatments. Keep for "Orig", remove for "Imp"?
2. **`ET0` changes its surface resistance at emergence.** Since Odin's "G2" commit it is one
   FAO-56 Penman-Monteith function (`et0.odin`, `fao56_et0`) for both; the two calls differ only
   in the stomatal resistance:
   - without a crop (soil moisture): the FAO default 100 s/m (surface resistance 100/1.44 =
     70 s/m), i.e. the FAO-56 grass reference;
   - with a crop (`crop_et0.odin`, `crop_stomata_resistance`): from CO2, the saturation deficit
     and the crop's reference gross photosynthesis, so it depends on the crop's state.

   Both take the humidity only as relative humidity (master and Odin; the AMEI branch's soil
   moisture ET0 also used a given vapor pressure), so the measured vapor pressure reaches MONICA
   through the RH the site readers derive from it, ea / es_mean. TXBL1990F: crop ET0 1105 mm vs
   1211 mm with the FAO default resistance over the season (-9 %), up to 2.4 mm on single days
   (`checks.py et0`). If the study wants the FAO-56 grass reference for ETO: run each treatment a
   second time without a crop and take ET0 from there.
3. **What goes into the template's ET columns.** It has two columns for potential ET, EOAD
   ("potential_evapotrans") and ETP ("potential evapotranspiration_daily"); both get `Pot_ET`.
   EPPAD (potential transpiration) and its season sum EPPCM come from Odin's path output
   `current_crop_module.potential_transpiration` (2026-11-17), the crop's own value from the same
   step that gives EPAD (`Act_Trans`): crop ET0 × Kc, capped (issue 1), minus the evaporation from
   interception, times the soil coverage, before water and oxygen stress; 0 before emergence and
   after maturity, rounded to 0.1 mm like EPAD. So EPAD <= EPPAD (checked on all runs), and
   EPAD/EPPAD is the crop's transpiration deficit. Not comparable with EOAD/ETP, which are
   uncapped (issue 1): on hot days EPPAD stays at 6.5 mm while EOAD goes up to 14. Season sums,
   EPPCM vs EPCM: Bushland F 587-631 vs the same (no water stress), P 513 vs 344 (1990), 605 vs 597
   (1992); Avignon 369-413 vs 362-413.
   Still NA: EPSAD (potential soil evaporation; MONICA has no single value for it, the potential
   is reduced per layer) and ETOS/ETRS (ASCE standardized short/tall reference; MONICA computes
   neither).

4. **Ponding water: crop and soil disagree about transpiration (a bug).** On days with water on
   the surface (`soil_moisture.odin:997-1019`, the same in the C++):
   - the water surface evaporates at capped potential × 1.1 / Kc, i.e. up to 1.1 × ET0,
     past the cap (TXBL1992F 1991-10-03, bare soil: 9.9 mm). Every simulated day with
     ETa > 6.5 mm is such a day;
   - when that uses up the potential, soil moisture sets transpiration to 0 and takes no water
     from the roots, but the crop module has already computed its own actual transpiration,
     uses it for its water stress and reports it (`Act_Trans`, EPAD). So the crop transpires
     water the soil never loses, and ESAD + EPAD != ETAD in the template. TXBL1992F 1992-04-10
     (23 mm irrigation): Pot_ET 6.1, ETa 7.7 = surface evaporation 7.66, soil moisture
     transpiration 0, crop transpiration 6.0;
   - crop transpiration the soil didn't supply, per season: Bushland F 140-171 mm (about a
     quarter of the crop's transpiration), P 15-66 mm, Avignon 5-16 mm.

   Checked with Odin's path outputs `soil_moisture.evaporated_from_surface` and
   `soil_moisture.actual_transpiration` against `Act_Trans`.
5. **Infiltration depends on the layer thickness.** The daily infiltration is limited by the
   free pore space of the top layer (`fm_Infiltration`), so with 5 cm layers about 15 mm/d.
   Bushland's irrigations (17-31 mm) pond on most irrigation days, which triggers 4. TXBL1990F
   with 5 vs 10 cm layers: 46 vs 22 ponding days, surface evaporation 200 vs 100 mm, crop
   transpiration not supplied by the soil 171 vs 101 mm, season ETa 699 vs 682 mm (surface
   evaporation replaces transpiration, so ETa hardly changes, but the split and the crop's water
   stress do). In the lysimeter 20-30 mm of irrigation presumably infiltrate within hours.

   **Switches in Odin (2026-11-17), soil moisture parameters, both off by default (= the C++,
   byte-identical):**
   - `PondEvaporationByCoverage` (issue 4): the standing water takes the soil's place, not the
     crop's. It evaporates with the share of the potential that reaches the ground, (1 - soil
     coverage) × potential × 1.1 / Kc, capped; the soil evaporates only from what the water
     leaves (a pond that dries up during the day), and soil moisture takes the crop's
     transpiration from the layers as the crop computed it. Why the C++ does otherwise: probably
     a HERMES leftover, the replacement is the soil evaporation formula with coverage instead of
     1 - coverage, unchanged since MONICA's first commit (2013); the only sound idea in it is
     that pond + crop must not exceed the potential, which the switch keeps. Only MONICA's own ET
     (not with `Modules.Evapotranspiration`).
   - `InfiltrationByDepth` + `InfiltrationDepth` (default 0.1 m, MONICA's original layer
     thickness; issue 5): the day's infiltration is limited by the free pore space down to that
     depth, the moisture deficit in the Ks term is the mean over it, and the water fills those
     layers from the top (their upper boundary fluxes include it, for the solute transport).
   - `run_et.py --soil-moisture PondEvaporationByCoverage=true InfiltrationByDepth=true` sets
     them; the daily output has `SurfEvap` (evaporated from standing water), `SMTrans` (soil
     moisture's transpiration) and `Infilt` as checks.

   Effect (default cultivar, cap 6.5), season sums sowing to maturity/harvest:

   | | ETa | standing water evap. | soil moisture transp. (crop: EPCM) | ET bias mm/d |
   |---|---|---|---|---|
   | TXBL1990F 10 cm, off | 682 | 100 | 488 (587) | -0.66 |
   | TXBL1990F 10 cm, both (0.1 m) | 676 | 4 | 579 (587) | -0.69 |
   | TXBL1990F 5 cm, off | 699 | 200 | 417 (587) | -0.60 |
   | TXBL1990F 5 cm, both (0.1 m) | 669 | 4 | 580 (587) | -0.72 |
   | TXBL1992F 5 / 10 cm, off | 781 / 739 | 207 / 59 | 453 / 546 (606) | -0.57 / -0.74 |
   | TXBL1992F 5 / 10 cm, both | 734 / 735 | 4 / 1 | 601 / 599 (606) | -0.75 / -0.75 |

   - With the pond switch, crop and soil agree (the remaining 6 mm in TXBL1992F is the harvest
     day: the crop module still reports its transpiration, soil moisture treats the field as
     bare already; an older quirk, unrelated). No more days with ETa > 6.5 mm.
   - With both, 5 and 10 cm layers give nearly the same season ETa (-7…+10 mm, before 16-44
     mm); the rest depends on the layer thickness elsewhere (evaporation depth, percolation).
   - At 10 cm layers `InfiltrationByDepth` with 0.1 m changes nothing (one layer, as before).
     0.2 m: a few mm less standing water (P seasons: none left), ETa -0…-5 mm, bias ±0.02.
   - Avignon (5 cm): standing water evaporation 12-32 → 2-12 mm, ETa -5…-10 mm, bias
     unchanged (±0.03).
   - The ET bias doesn't improve: the split changes, the sum hardly. The F seasons' -0.7 mm/d
     stay the cap's (issue 1).

## MONICA builds: Odin vs the AMEI branch

Checked 2026-11-16 with all 10 treatments, same inputs:

- **Odin = its C++ reference, byte for byte.** `nix build .#monica-cpp-src` in `monica_odin`
  (the converted C++ that CI keeps in sync with master and Odin) gives identical `monica_out.csv`
  for every treatment.
- **Layering (fixed in `et_monica.py`).** The AMEI branch takes `NumberOfLayers`/`LayerThickness`
  from the site parameters, master and Odin from the *simulation* parameters (default 20 × 0.1 m)
  and ignore the site's. So with only the site.json set, Odin simulated our 5 cm layers as 10 cm
  layers: double the water per layer, all of a 22.5 mm rain infiltrating where the AMEI branch
  stopped at the top layer's pore space (15.4 mm). Over a season: yield up to 701 kg/ha
  different (Avignon), ETa up to 46 mm (Bushland), maturity up to 3 days. `sim_json` now sets
  both in sim.json too. No warning anywhere, so
  worth remembering for other setups (the Maricopa flow?).
- **What remains between Odin and the AMEI branch: 1-2 %**, from differences in the code: the
  `amei` branch is 141 commits apart from Odin's reference (master `ede378e9`), with changes in
  `crop-module.cpp`, `soilmoisture.cpp`, `soilorganic.cpp` and others. Same phenology. Plant N
  splits first (a month after sowing, e.g. TXBL1990P CNAM 61.7 Odin vs 72.3 AMEI), then
  biomass/yield (+0.1-1.5 %) and ETa (season ±1 %, TXBL1992F +11 mm). Not traced to single
  commits.
- **Output rounding.** Odin uses master's: ET0, Act_ET, Act_Ev, Act_Trans to 0.1 mm, Mois to
  0.001 (the AMEI branch: 0.01 mm, 0.0001). Odin also takes a path into the model as an output
  name, unrounded (`soil_moisture.reference_evapotranspiration|ETO` gives 0.437923 instead of 0.4),
  but that only works in Odin and `Pot_ET` has no path (it's computed). Not used yet.
- **Net radiation (RND), fixed 2026-11-16.** MONICA kept only the bare-soil net radiation: the
  soil moisture module sets it in its own ET0, but with a crop the crop module's ET0 computed its
  own net radiation and threw it away, so the value stayed at its last pre-emergence one (FRAV_2004:
  1.568 MJ m-2 from emergence to harvest). Master never had it as an output (it came with
  `intercropping_alessandro`/`amei`); the gap is old (crop-module local since at least 2020). Now
  the crop module keeps it and soil moisture takes it on crop days, as it does with ET0:
  - Odin (`monica_odin` `5ab28005`): `Crop_Et0.net_radiation` -> `Crop_To_Soil` ->
    `soil_moisture.net_radiation`, legacy name `net_radiation` added to the output table; the same
    in the C++ copy in `src/` (output appended at the end of the id list). Odin tests pass,
    component check clean, `components.md` regenerated, Odin = `monica-cpp-src` byte for byte on
    all 10 treatments.
  - AMEI branch (`monica_amei` `480692c9`): `CropModule::vc_NetRadiation`/`get_NetRadiation()`,
    taken over in `SoilMoisture::fm_Evapotranspiration`; output unit now "MJ m-2". Only RND
    changes (checked against a build without the fix), and it equals Odin's on every day.
  - `et_monica.py` converts it to the template's W/m2 (× 1e6/86400, daily mean).
  - With the Crop2ML ET modules it was already filled daily (they write it themselves).

## Data issues worth reporting to the data providers

All from `checks.py humidity`, which tests each column against the dew point and, independently,
against the other humidity columns.

- **Avignon `VPRSD` is the vapor pressure *deficit*, not the vapor pressure** the Definitions
  sheet says (kPa, "Vapor pressure, average daily"):
  - on the 19 days saturated all day (RHMXD = 100, RHUMD >= 95) it averages 0.027 kPa, where a
    vapor pressure must be about es = 1.09 kPa;
  - ea from Avignon's own RH min/max (FAO56 eq. 17) is 1.11 kPa on average, close to ea(TDEW)
    1.18, not to VPRSD 0.76: two independent measurements agree with each other, not with VPRSD;
  - VPRSD vs es - ea(TDEW): correlation 0.984, MAE 0.10 kPa.

  Bushland's and Maricopa's `VPRSD` equal ea(TDEW) exactly. Similar averages across the files
  (0.58 / 0.76 / 0.75 kPa) are a coincidence of the climates. Any model taking Avignon's VPRSD as
  ea gets a wrong ET0.
- **`RHUMD` means different things.** Avignon defines it as the daily *minimum*, and the data
  agree (vs ea/es(Tmax): MAE 6.8 %, corr 0.91). Bushland's header calls it
  "relative_humidity_mean" (no entry in its Definitions tab), but it pairs with `RHMXD` (max),
  goes down to 10 % on 167 days, and fits the minimum slightly better than the mean (corr 0.71 vs
  0.66; ea from it as min/max vs as mean: corr 0.77 vs 0.63). Neither fits well: Bushland's RH
  columns don't match its dew point under either reading (MAE ~12 %), so RH and TDEW may come
  from different sensors. Maricopa uses `RHAVD` for the mean, which is exactly ea(TDEW)/es_mean.
  Ask the Bushland providers what RHUMD is.
- Bushland treatment ids differ between sheets: TXBL1990F (Treatments, Plots, Planting,
  Fertilizer) vs TXBL1989F (Irrigation, Obs, ET_Daily) for the same season.
- Avignon: the soil profile's `SOIL_ID` is "calcaric fluvisol", Metadata refers to "FRAV".
- The daily template's name row has one column more than the code/unit rows
  (`soil_temp_surface_daily_avg`, no code); the code row is followed.

## Bushland setup

- Ids of the Treatments sheet; the other sheets' ids matched by prefix/suffix and sowing or
  harvest year.
- Weather as given (`VPRSD` = ea in kPa, wind at 2 m), except the mean RH: derived as
  ea / es_mean, since `RHUMD` is ambiguous (see above). That lowered ET0 by ~4 % (TXBL1990F
  1149 -> 1105 mm), as expected if RHUMD is closer to the minimum. One `RAIN` column, though the
  comments describe `RAIN_PART`/`RAIN_FULL`.
- `SLPHW` missing below 160 cm: taken from the horizon above. Since 2026-11-18 **24 layers of
  10 cm** = the whole 2.4 m profile (2026-11-17: 21 x 10 cm = 2.1 m, before 42 x 5 cm, no reason
  recorded for leaving out the bottom 30 cm), see "10 cm layers" below. 2.1 -> 2.4 m changes the
  season ETa by at most 0.2 mm (default cultivar).
- Initial moisture above FC in 1992 (deep layers) and 1993 (5-70 cm): kept since 2026-11-23 (MONICA
  accepts up to saturation since monica_odin 790ce3ca; set to FC before). The excess drains; yields
  -0…-12 kg/ha, ET unchanged.
- Initial N: only `ICIN` = 25 kg N/ha, spread evenly over the profile. Nitrogen response is off.
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
- Initial moisture slightly above FC in a few layers (2004, 2010, 2012): kept since 2026-11-23 (set
  to FC before; results unchanged). 2012 has
  only 4 initial moisture layers, simulated by the providers (SIMPLACE Lintul-5).
- Fertilizer FE001 (ammonium nitrate) as 50 % NH4 / 50 % NO3; the P/K applications (FE009)
  are skipped. Previous crop residues (ICRAG) and tillage are not used.
- Cultivars are durum wheat (Artimon, Acalou, Dakter, Isildur), one per season; simulated with
  one winter wheat cultivar calibrated for all four ("Avignon calibration"). The comments give
  vernalization/photosensitivity scores.
- Observed ET: eddy covariance; 24 h values (`ETDAILY_100`, else `_90`) are compared, the much
  more frequent 6-18 h values (`ETDAY_*`) are only plotted. 2012 has 6 days of 24 h ET.
- Phenology: where ADAT/MDAT are missing, estimated from BBCH as the comments suggest (anthesis
  4 days before BBCH 69 or 150 °C d after BBCH 55/58; maturity at BBCH 89, else 87).
- Yield: `HWAH_machine`, as recommended in the comments.
- Daily LAI (`LAID`) and above ground biomass (`TDRW`, total dry weight) from `Obs_daily`, for
  the calibration.

## First results (default winter wheat cultivar, uncalibrated)

Bushland (lysimeter, 24 h), Odin:

| | ETa obs / sim (mm) | bias mm/d | anthesis obs / sim | LAI max obs / sim |
|---|---|---|---|---|
| TXBL1990F | 825 / 684 | -0.60 | 05-16 / 06-05 | 3.6 / 11.6 |
| TXBL1990P | 444 / 431 | -0.06 | 05-15 / 06-01 | 2.4 / 10.7 |
| TXBL1992F | 930 / 776 | -0.57 | 05-09 / 05-25 | 7.1 / 12.8 |
| TXBL1992P | 719 / 742 | 0.09 | - / 05-25 | 5.9 / 12.8 |
| TXBL1993F | 907 / 770 | -0.56 | 05-18 / 06-02 | 4.1 / 12.9 |
| TXBL1993P | 799 / 750 | -0.19 | 05-18 / 06-02 | 4.0 / 12.9 |

Avignon (eddy covariance, 24 h days only), Odin:

| | days | ETa obs / sim (mm) | bias mm/d | anthesis obs / sim | LAI max obs / sim | yield obs / sim |
|---|---|---|---|---|---|---|
| FRAV_2004 | 49 | 122 / 110 | -0.23 | 04-28 (est.) / 05-28 | 2.7 / 8.7 | 6660 / 6268 |
| FRAV_2006 | 46 | 96 / 45 | -1.11 | 05-15 / 05-21 | 5.5 / 8.5 | 4500 / 6048 |
| FRAV_2010 | 35 | 111 / 97 | -0.40 | 05-22 (est.) / 05-29 | 6.4 / 7.5 | 7030 / 8439 |
| FRAV_2012 | 6 | 12 / 10 | -0.44 | 05-06 (est.) / 05-23 | 1.2 / 10.5 | 5080 / 7652 |

Bushland: anthesis 2-3 weeks late, maturity mostly not reached before harvest, LAI ~3x too high,
root depth up to 3.9 m (profile is 2.1 m). ETa underestimated mainly late in the season (6.5 mm
cap, and the crop keeps transpiring while the real one senesces). Avignon: anthesis 1-4 weeks
late, LAI too high as well; ETa mostly fine in winter/spring, the 6.5 mm cap shows from May.

## 10 cm layers (Bushland, 2026-11-17)

With 5 cm layers the infiltration limit (it scales with the layer thickness, model issue 5) let
the irrigation water pond, which then evaporated at up to 1.1 x ET0, past the 6.5 mm cap (model
issue 4). Bushland now uses 21 x 10 cm (`Setup.layer_thickness_m`, default still 5 cm; Avignon
and the soil temperature study unchanged). Same Odin build (`d343fb82`), default cultivar; the
build alone changed nothing against the table above.

| | ETa sim 5 / 10 cm (mm) | days with ETa > 6.5 mm, 5 / 10 cm | soil evaporation 5 / 10 cm (mm) |
|---|---|---|---|
| TXBL1990F | 684 / 670 | 9 / 3 | 62 / 74 |
| TXBL1990P | 431 / 432 | 0 / 0 | 61 / 75 |
| TXBL1992F | 776 / 732 | 10 / 2 | 63 / 77 |
| TXBL1992P | 742 / 702 | 5 / 0 | 63 / 77 |
| TXBL1993F | 770 / 749 | 11 / 2 | 77 / 92 |
| TXBL1993P | 750 / 755 | 1 / 0 | 75 / 92 |

The ponding is mostly gone; the water now evaporates from the soil instead. The bias of the fully
irrigated treatments gets a little worse (-0.64 to -0.74 mm/d): what's left is the cap.

## Calibration (`calibrate.py`, 2026-11-17)

Step by step, each step starting from the previous one's cultivar: 1. phenology, 2. LAI and
biomass (F treatments), 3. yield, 4. water (Kc, rooting, drought response on the P treatments),
then leave-one-season-out. The parameters per step in a CSV like the Maricopa flows'
`calibratethese_*.csv` (`calibration/<site>_<n>_<step>.csv`, free variables and derived rows
supported); SCE-UA (spotpy); a MONICA run takes 0.05 s, a step a few minutes. The runs last
until maturity (at the latest until the weather has a gap after harvest), a date not reached
counts as 30 days after the last simulated day.

**Step 1, phenology, Bushland** (`calibration/bushland_1_phenology.csv`; one winter wheat for all
three cultivars: TAM200/TAM107/Mesa have one season each, so per cultivar there would be one F/P
pair with the same dates): emergence, anthesis, maturity of all 6 treatments (17 dates). 5
parameters: `StageTemperatureSum[0]` (emergence), a factor on `[1..3]` (anthesis), `[4]`
(maturity), `VernalisationRequirement[1]`, one `DaylengthRequirement` for stages 2-4. RMSE 15.2
-> 2.9 d, ~1000 evaluations, 1.5 min:

| | emergence | anthesis | maturity |
|---|---|---|---|
| default, sim - obs (d) | -3..1 | 14..20 | 7..36 |
| calibrated, sim - obs (d) | -3..1 | -2..5 | -5..5 |

Calibrated: TSUM 154 / 279 / 373 / 177 / 401 (default 148 / 284 / 380 / 180 / 420),
vernalisation 11 d (50), daylength requirement 18.3 h (20). The vernalisation went to the lower
bound (10); with the bound at 0 it ends at 13 d with the same RMSE and 181 °C d for emergence:
**the dates don't determine the parameters** (vernalisation, daylength and the temperature sums
trade off). The daily Zadoks stages (9-13 per treatment) could pin them down better; MONICA's
stages map to Zadoks only at the stage boundaries (the GSTZD output interpolates within the stages
since 2026-11-23, see soil_temperature/FINDINGS.md "Outputs"; an approximation, not a calibration target).

With that cultivar (`crops/cultivar_winter-wheat_bushland_1_phenology.json`, 10 cm): all
treatments reach maturity, LAI max 6.2-7.9 (obs 2.4-7.1, before 10.7-13.2), ET RMSE 1.35-1.96
mm/d, bias of the F treatments -0.61..-0.67 mm/d (the cap). Yield off: F +13..+46 %, P -57 % (1990)
to +88 % (1992). Steps 2-4.

### Second round (2026-11-18): with N response, the ponding fixes and more observations

Setup changes for Bushland, in "Orig" and "Imp" alike (they are not about ET0/ETp): 24 layers
(2.4 m), `NitrogenResponseOn` (the treatments differ in N: 1990 F 127 / P 64, 1993 F 104 / P 50 kg
N/ha), `PondEvaporationByCoverage` and `InfiltrationByDepth`, the initial 25 kg N/ha (`ICIN`) spread
by organic carbon instead of evenly, the anhydrous ammonia at its injection depth (17 cm, `FEDEP`;
Odin's MineralFertilization got `atLayer`/`depth` for it). `calibrate.py` can now calibrate species
parameters (`species.<Name>`), `run_et.py` has `--species`. Not calibrated against ET (it is what the
study evaluates).

**Step 1, phenology** (`calibration/bushland_1_phenology.csv`): 24 dates, ear emergence (`EEMD`,
compared with the start of MONICA's stage 4) added, anthesis of TXBL1992P interpolated from the
daily Zadoks stages (61). Jointing (`JDAT`) isn't used: MONICA's stage 3 starts in December already,
jointing falls inside it. With the daylength requirement free, it traded off with the vegetative
temperature sums (two seeds: ×1.07 / 16.7 h vs ×1.45 / 12.7 h, same RMSE), so it stays at 20 h.
RMSE 11.9 -> 3.6 d, two seeds agree: TSUM ×0.80 for stages 2-3, stage 4 103-122 °C d, maturity
459-467 °C d, vernalisation 34 d. Left: TXBL1992P 4-8 days late (it develops earlier than F in
reality; MONICA accelerates with stress only from stage 4 on).

**Step 2, growth** (`calibration/bushland_2_growth.csv`, the F treatments, daily LAI and above
ground biomass; the organ weights aren't used: LAI / leaf weight contradicts `SLAD`, 1993F stem >
total): with the step 1 cultivar LAI was 2-6× too high (winter 4-6 vs 0.5-1.3), biomass 2-5× too high
in winter. Calibrated (two seeds agree): MaxAssimilationRate 38 (30), species
MinimumTemperatureForAssimilation 4.1 °C (0), SLA ×0.74, leaf senescence 0.02 in stage 4 (0) and 0.15
from anthesis (0.05, at the bound). RMSE (1 LAI ~ 3 t/ha) 3.4 -> 0.8; LAI max 3.7-5.1 (obs 2.4-7.1).
Leaf partitioning traded off with SLA, it stays at the default.

**Open: nitrogen.** From March on the F treatments are strongly N-limited (N stress factor
0.4-0.55), and late biomass stays 4-6 t/ha below the observed (1992F, 1993F). Season N balance
(1992F, step 2 parameters): ~32 kg mineral N at the start + 112 fertilizer + 33 net mineralisation;
crop uptake 130 (113 above ground), leaching 5, NH3 2, **75 kg left in the soil at the end** (1990F
59, 1993F 62). The N isn't missing, it is too dilute: the injected fertilizer (92 kg in 10-20 cm in
October) is moved down to 40-120 cm by January, the crop has taken up 78 kg N by then (1.7 t/ha,
~4.5 % N), and from March on the remaining 60-65 kg lie at 1-5 kg per 10 cm over 2 m. With N
response off (same parameters) the late biomass gap closes for 1993F (June -0.7 instead of -3.6
t/ha) and halves for 1992F (-3.1 instead of -6.1); F yields 5.3 / 6.8 / 6.4 t/ha (obs 4.5 / 5.8 /
6.4). No N observations to check against (neither crop N nor soil nitrate). And water: the P treatments show no water stress in MONICA (1992P
transpiration reduced ≤ 5 %), while their observed LAI collapses in April. ET with the step 2
parameters: F bias -0.85…-0.92 mm/d (lower LAI, same cap). Yield: F 4.8-4.9 t/ha (obs 4.5-6.4), P
1.4 / 5.1 / 3.3 (obs 1.9 / 3.2 / 5.0).

**Decision (2026-11-18): steps 2-3 with N response off** (`calibrate.py --no-nitrogen-stress`), on the
F treatments, taken as not N-limited, as the experiment presumably intended; the runs keep N
response on (the fertilization differences between F and P matter). So the crop parameters don't
absorb MONICA's N uncertainty. Question for the data providers: crop N or soil nitrate measured?
How reliable is `ICIN` = 25 kg/ha?

- Step 2 again (N off; senescence bound widened to 0.4): two seeds agree, nearly the same as with N
  on: MaxAssimilationRate 38, MinimumTemperatureForAssimilation 4.1 °C, SLA ×0.73, leaf senescence
  0.019 (stage 4) and 0.18 (from anthesis). RMSE 3.8 -> 0.7. **Replaced** (see the soil moisture
  check below): MinimumTemperatureForAssimilation also lowers the crop ET0 in winter.
- Step 3 (yield, `calibration/bushland_3_yield.csv`, AssimilateReallocation): not needed, the F yields
  are within ±0.5 t/ha with the step 2 parameters (RMSE 0.50 -> 0.47 t/ha with 0.25 instead of 0.3);
  the default stays.

With those step 2 parameters (MinimumTemperatureForAssimilation 4.1 °C), N response on:

| | ET bias mm/d | LAI max obs / sim | yield obs / sim (N off) |
|---|---|---|---|
| TXBL1990F | -0.88 | 3.6 / 3.8 | 4475 / 4694 (5013) |
| TXBL1990P | 0.03 | 2.4 / 3.6 | 1898 / 1519 (1749) |
| TXBL1992F | -0.92 | 7.1 / 4.9 | 5847 / 4635 (6350) |
| TXBL1992P | -0.24 | 5.9 / 5.0 | 3197 / 4926 (6187) |
| TXBL1993F | -0.86 | 4.1 / 4.4 | 6422 / 4609 (5972) |
| TXBL1993P | -0.60 | 4.0 / 4.0 | 5031 / 3247 (5972) |

Open: the N-limited F yields of 1992/1993 (see above), the missing water stress of 1992P, the cap
(the F treatments' ET bias, "Imp").

**Soil moisture check (2026-11-18, lysimeter and field data, step 2 parameters, N on).** MONICA's
profile (0-180 cm) is 40-90 mm too wet in spring in the P treatments, mostly at 70-130 cm, which is
why they show no water stress (1992P: 4.9 t/ha vs 3.2 observed). Two causes:
- The soil data's field capacity at 80-140 cm (0.35) is never reached: max measured 0.31-0.34
  (lysimeter), 0.31-0.32 (field); MONICA drains only down to 0.35. Lowering it to 0.32 (and LL to
  0.18) removes ~20 mm, 1992P 4.9 -> 4.7 t/ha. A question for the data providers rather than a
  calibration.
- **Winter ET is far too low** (the bigger part): Nov-Feb lysimeter ET 123-198 mm, MONICA 57-89 mm,
  the water stays in the profile (1990P: measured profile flat over winter, simulated +80 mm). On
  cold days MONICA's crop ET0 is ~0: its stomatal resistance comes from the crop's photosynthesis
  (model issue 2), which stops below MinimumTemperatureForAssimilation. The step 2 value (4.1 °C,
  default 0) makes that worse: winter ET0 -57…-62 mm, ETa -23…-30 mm. With the default (0 °C) winter
  ETa is still 86-119 mm, so most of it is MONICA's (crop) ET0 itself: a Phase 1 (ET0/ETp) finding,
  not a calibration one.
- Step 2 without MinimumTemperatureForAssimilation (so the calibration doesn't change ET0):
  MaxAssimilationRate 33, SLA ×0.65, leaf senescence 0.009/0.21, RMSE 0.9 (instead of 0.7). Runs (N
  on): F ET bias -0.73…-0.80 (instead of -0.86…-0.92), yields F 4.3/4.4/4.4, P 1.0/4.3/3.1 t/ha (obs
  F 4.5/5.8/6.4, P 1.9/3.2/5.0); the spring profile still 50-80 mm too wet.

**Final (2026-11-18): step 2 without MinimumTemperatureForAssimilation**, so the calibration doesn't
change MONICA's ET0 (`calibration/bushland_2_growth.csv`, two seeds agree): MaxAssimilationRate 33,
SLA ×0.65, leaf senescence 0.009 (stage 4) and 0.21 (from anthesis), RMSE 3.8 -> 0.9; the default
species. `crops/cultivar_winter-wheat_bushland_2_growth.json` is the Bushland default (`CULTIVAR`),
out_bushland ("Orig") rerun with it. Step 3 checked again: RMSE 0.6 -> 0.5 t/ha with reallocation
0.41, only by trading 1993 against 1990/1992; the default stays.

| | ET bias mm/d | RMSE mm/d | LAI max obs / sim | yield obs / sim |
|---|---|---|---|---|
| TXBL1990F | -0.75 | 1.82 | 3.6 / 3.8 | 4475 / 4308 |
| TXBL1990P | 0.02 | 1.38 | 2.4 / 3.4 | 1898 / 977 |
| TXBL1992F | -0.80 | 1.64 | 7.1 / 4.5 | 5847 / 4371 |
| TXBL1992P | -0.15 | 1.61 | 5.9 / 4.5 | 3197 / 4302 |
| TXBL1993F | -0.73 | 1.68 | 4.1 / 4.1 | 6422 / 4365 |
| TXBL1993P | -0.50 | 1.36 | 4.0 / 3.8 | 5031 / 3086 |

Phenology: emergence -3…+2 d, anthesis -5…+4 d, maturity -2…+12 d (TXBL1992P). Open, none of them
for the calibration: N (F yields 1992/1993, data providers), FC at 80-140 cm (data providers),
winter ET and the cap (MONICA's ET0/ETp, "Imp").

## Avignon calibration (2026-11-19)

The same steps and parameter CSVs as Bushland's (`calibration/avignon_<n>_<step>.csv`, copies), and
since then the same setup: **N response on and Odin's standing water fixes** (they were off for
Avignon before; the user: they are fixes that belong to the current MONICA). 38 x 5 cm layers as
before. From the default winter wheat; one cultivar for the four durum cultivars, since there's one
season per cultivar. Not against ET. Each step in `calibration/calibrate.sh`.

**Step 1, phenology** (all 4 seasons: emergence, anthesis, maturity where given; no ear emergence,
the BBCH 55/58 dates already make two anthesis estimates; N response on). RMSE 12.3 -> 4.9 d (seed
1, adopted) / 5.2 d (seed 7, which trades stages 1-2 against stage 3 and vernalisation): TSUM 194 /
235 / 314 / 229 / 385 (default 148 / 284 / 380 / 180 / 420), VernalisationRequirement 8.7 d (50),
which fits the comments' low vernalization scores (1.5-4.5 of 5, three of the four <= 2.5).

| sim - obs (d) | 2004 | 2006 | 2010 | 2012 |
|---|---|---|---|---|
| emergence | -5 | +3 | 0 | 0 |
| anthesis | +12 | -5 | -5 | 0 |
| maturity | - | +1 | +4 | -4 |

FRAV_2004's anthesis (04-28) is the comments' estimate, 150 °C d after BBCH 58 (04-19). The same
season's `Obs_daily` has Zadoks 61 (anthesis begins) on 05-06 and 65 (mid-anthesis) on 05-18, so
anthesis was in the first half of May, as simulated (05-10). The early estimate likely pulls the
other seasons' anthesis 5 days early. **For the data providers:** which one holds?

**Step 2, growth** (daily LAI and biomass of 2004, 2006, 2010, N response off as for Bushland;
2012 left out: the comments report N stress from leaching and late fertilization plus a poor
stand). Seeds 1 and 7 agree (RMSE 1.2 -> 0.8): MaxAssimilationRate 34 (30), SLA x1.14, leaf
senescence 0.019 (stage 4) and 0.117 (from anthesis). Biomass RMSE per season 0.20-0.53 x 3 t/ha,
LAI 0.75-1.51. LAI can't fit all seasons: 2004 stays at LAI 2.7 with high biomass, and 2010's LAI
6.4 on 05-07 means an SLA of 45 m2/kg from that date's leaf weight, against 12-31 on every other
date: one of the two is probably wrong (data providers).

**Step 3, yield** (AssimilateReallocation, N off): **not adopted**. It goes to its lower bound 0
(RMSE 2.1 -> 1.6 t/ha) and FRAV_2006 stays +2.7 t/ha. 2006 is the season with the largest gap
between the two observed yields (machine 4.5, counted 6.1 t/ha) and the only one irrigated.

`crops/cultivar_winter-wheat_avignon_2_growth.json` is the Avignon `CULTIVAR`, out_avignon rerun.
The steps first ran in the old setup (N off, no fixes); the cultivars came out almost the same, so
the difference between the columns is the setup's:

| | default cultivar, old setup | calibrated, old setup | **calibrated, N + fixes** |
|---|---|---|---|
| ET bias / RMSE mm/d, all 136 days | -0.58 / 1.26 | -0.60 / 0.96 | **-0.71 / 1.06** |
| FRAV_2004 bias / RMSE | -0.23 / 1.46 | -0.40 / 0.65 | -0.48 / 0.72 |
| FRAV_2006 | -1.11 / 1.26 | -0.86 / 1.06 | -0.89 / 1.07 |
| FRAV_2010 | -0.40 / 0.95 | -0.66 / 1.16 | -0.88 / 1.41 |
| FRAV_2012 | -0.44 / 0.95 | -0.09 / 0.96 | -0.14 / 0.93 |
| LAI max (obs 2.7 / 5.5 / 6.4 / 1.2) | 8.7 / 8.5 / 7.5 / 10.5 | 4.4 / 5.1 / 3.8 / 6.5 | 3.6 / 3.6 / 2.3 / 5.1 |
| yield t/ha (obs 6.7 / 4.5 / 7.0 / 5.1) | 6.3 / 6.0 / 8.4 / 7.7 | 7.7 / 7.9 / 7.0 / 8.3 | 7.1 / 6.6 / 5.7 / 7.5 |

The fixes hardly matter here (with N off: bias -0.41/-0.86/-0.64/-0.11 vs -0.40/-0.86/-0.66/-0.09);
**the N response does**: LAI max -0.7 to -1.5, ET 2010 -0.88 instead of -0.64 mm/d. Not a lack
of N over the season (crop N at maturity the same with and without, 2010: 238 vs 240 kg N/ha) but
**winter N dilution**: in FRAV_2010 the crop's N stays at ~4 kg N/ha from December to the first
fertilization (03-17) while its biomass grows from 120 to 700 kg/ha, so NDef falls to 0.25 in
February, although the roots are 0.8-1.8 m deep and the soil holds ~20 kg NO3-N/ha in 0-60 cm
and ~40 below. Why MONICA's uptake stalls there isn't traced yet (transpiration-driven uptake in
winter?). The simulated winter biomass is also too high (2010-02-03: ~450 vs 181 kg/ha observed;
step 2's residuals: +0.1-0.2 x 3 t/ha on the winter dates), which makes the dilution worse.
Open: the winter N uptake (MONICA), the initial soil N (estimated by the providers, comments).

## Sensitivity analysis (protocol steps 6 and 7, `et-sensitivity`, 2026-11-27)

```
pixi run et-sensitivity --weather standardized   # step 6 -> out_sensitivity_standardized/
pixi run et-sensitivity --weather actual         # step 7 -> out_sensitivity_actual/
```

27 treatments (ALPHA × ALBEDO × lambda/PSYCON, `Treatments.xlsx`) × 7 sites (USGA, DEMU, COCA,
FRLU, USMA, FRMO, CAQC; 1991-2020, NASA POWER), result files in the template
`3_Templates/Sensitivity_analysis/` (`results/`, gitignored, see `checksums.txt`; `summary.csv`:
mean annual ET0 per site and treatment). ETOS, ETRS, ETP: NA (MONICA has only the FAO-56 grass
reference). Script: `src/amei_exercises/evapotranspiration/sensitivity.py`.

**MONICA build: monica_odin branch `amei-et-sensitivity`** (on top of `simplify_copilot`; for the
exercise, not the main line):
- In the main line since 2026-11-27 (`simplify_copilot` 036d28b4, EvapoTranspirationModels main
  a6de6be): lambda (`LatentHeatOfVaporization`, crop module parameter next to `ReferenceAlbedo`,
  and in all three MONICA Crop2ML packages); 0 (the default) = MONICA's constants (0.408 = 1/2.45,
  0.000665 × pressure), so MONICA's results are unchanged. FAO-56's 2.45 MJ/kg is lambda at ~20 °C
  (eq. 3-1: 2.501 − 0.002361 T; 2.49 ≈ 5 °C, 2.43 ≈ 30 °C).
- Only on the branch: `fao56_et0` split into the intermediates and `fao56_penman_monteith` (the
  equation itself); the psychrometric constant (`PsychrometricConstant`) as a crop module parameter;
  `Monica_Internal_Reference_Evapotranspiration`, MONICA's own equation as an evapotranspiration
  module on given intermediates, so `monica-module` runs it and the Crop2ML package on the same
  input file; the Crop2ML package `Monica_Reference_Evapotranspiration` with optional slope and
  psychrometric constant (negative: calculated as before; 0 is a value: CAQC's rounded SLOPE is
  0.00 on 21 days), from EvapoTranspirationModels' branch `amei-et-sensitivity`.

**Step 6, standardized inputs:** `monica-module`, one run per site and lambda: NETRAD, VPRSD,
VPRSAT, SLOPE and TAVD from the weather file, WIND km/d -> m/s at 2 m, PSYCON and lambda from the
treatment, the FAO stomata resistance 100 s/m. `ETstudyOSAstand...` = MONICA's own equation,
`ETstudyCSAstand...` = the Crop2ML package: **identical on every day** (max difference 0).

**Step 7, actual weather:** MONICA without a crop (any soil), TMIN/TAVD/TMAX/SRAD/RH2M/WIND,
latitude from `soil_temperature/sensitivity_analysis/input_data/WeatherMetadata_corr.csv`, the
elevation NASA POWER's grid cell (25 / 54 / 794 / 128 / 434 / 86 / 332 m; the metadata have none;
fitting the standardized NETRAD gives COCA 790, USMA 430 m, the others are poorly determined),
ALBEDO as MONICA's reference albedo, lambda and PSYCON from the treatment. `ETstudySAactual...` =
MONICA's own ET0; the Crop2ML package and the internal module as drivers give the same on every day.

Mean annual ET0 (mm/a):

| | CAQC | DEMU | FRLU | FRMO | USGA | COCA | USMA |
|---|---|---|---|---|---|---|---|
| standardized, lambda 2.45 | 545 | 752 | 827 | 1153 | 1204 | 1240 | 1998 |
| actual, albedo 0.23, lambda 2.45 | 535 | 696 | 762 | 1087 | 1166 | 1148 | 1958 |
| actual, albedo 0.12 / 0.19 | 626 / 568 | 774 / 724 | 860 / 797 | 1206 / 1130 | 1349 / 1233 | 1307 / 1206 | 2171 / 2035 |

- **ALPHA has no effect** (Priestley-Taylor only), **ALBEDO only with actual weather** (the
  standardized NETRAD is given; it was calculated with 0.23): 0.12 instead of 0.23 +11-16 %.
  **lambda** 2.43 / 2.49 instead of 2.45: +0.4-0.6 % / -0.8-1.2 % (both 1/lambda and PSYCON).
  PHTD and LAID aren't used (reference grass).
- **MONICA's own intermediates give 1-7 % less ET0** than the standardized ones at the same
  settings (CAQC -10, COCA -92 mm/a). Mostly the vapour pressure: MONICA takes RH × the mean
  saturation vapour pressure (RH2M), the standardized VPRSD comes from TDEW (FAO-56 eq. 14); COCA:
  2.48 vs 2.28 kPa, VPD 0.44 vs 0.64 kPa. Net radiation +0.16 MJ/m² (COCA), the slope -0.007 kPa/°C
  (the standardized SLOPE is not from TAVD alone).

## Open questions

By relevance for Bushland and Avignon (2026-11-16):

1. **Phenology/LAI**, the largest error source: anthesis 1-4 weeks late, LAI 1.2-9x too high,
   Bushland mostly not mature at harvest, so Kc and soil cover are wrong. Calibrate per cultivar
   (Bushland: TAM200, TAM107, Mesa; Avignon: four durum cultivars) with the calibration flow, or
   is "Orig" meant uncalibrated?
   *Since 2026-11-18/19 both sites calibrated (one cultivar per site), see above.*
2. **6.5 mm/d cap** (model issue 1): most of the late-season ETa underestimation at Bushland
   (-0.6 mm/d in the fully irrigated treatments). Part of "Orig", removed in "Imp"?
3. **ETO column** (model issue 2): the crop-dependent ET0 as now, or the FAO-56 grass reference
   from a crop-free run?
4. **Not looked into yet:** FRAV_2006's bias (-1.11 mm/d, 45 vs 96 mm on the measured days, far
   worse than the other Avignon years); Bushland rooting depth up to 3.9 m in a 2.4 m profile.
5. **For the data providers** (no effect on the runs, RH is derived from ea / es_mean): Bushland,
   is RHUMD the mean or the minimum? Avignon, VPRSD is the deficit, not the vapor pressure.
6. **Ponding** (model issues 4 and 5): switches in Odin, off by default. Use them for "Imp"
   (together with the cap)? `InfiltrationDepth` 0.1 or 0.2 m: hardly matters for ET here.
7. ~~Template columns: fill EPPAD from Odin's path output?~~ Done (model issue 3). EPSAD, ETOS,
   ETRS stay NA.

Less relevant for these two sites:

- Avignon: EXPTRNO or EXP_TRT_NAME as treatment id? Use residues/tillage?
- Odin's outputs round ET to 0.1 mm/d: good enough, or switch to unrounded path outputs (Odin
  only)?
- `Monica_AMEI_potential_Evapotranspiration` isn't integrated yet; "Imp" runs would select it via
  `EvapotranspirationModel` in sim.json.
- Fix the capnp soil moisture / climate gaps in `monica` (master) and `monica_odin` as well?
  (Flows and Maricopa only.)
