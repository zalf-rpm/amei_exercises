# ET study, Phase 1: findings and open questions

## Setup

```
pixi run python evapotranspiration/run_et.py --site bushland|avignon [--treatments ...] [--monica-run ...]
pixi run python evapotranspiration/evaluate_et.py --site bushland|avignon
```

- `site_bushland.py`, `site_avignon.py`: read the site's xlsx into site-independent `Setup`s
  (weather, soil, initial conditions, management, observations). All site-specific assumptions
  live there.
- `et_monica.py`: `Setup` -> MONICA inputs, `monica-run`, output parsing, AMEI templates.
- MONICA: the Odin port by default (`~/GitHub/monica_odin/odin/build/monica-run`, branch
  `simplify_copilot`, built with `pixi run build monica-run` in `odin/`). `--monica-run` takes
  any other build; the template header records which one (`Model: MONICA (MOFAPM,
  monica_odin simplify_copilot@<commit>)`). See "MONICA builds" below.
- `checks.py`: reproduces the checks behind the data and ET0 statements below
  (`checks.py humidity`, `checks.py et0`).
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
   The potential parts are NA: EPSAD (potential soil evaporation) and EPPAD (potential
   transpiration). MONICA does compute the latter (`potential_transpiration` in
   `crop_water_uptake.odin`, after the cap and interception), which Odin could output via its path.
   ETOS/ETRS (ASCE standardized short/tall reference) are NA: MONICA computes neither.

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
- `SLPHW` missing below 160 cm: taken from the horizon above. Since 2026-11-17 **21 layers of
  10 cm** = 2.1 m of the 2.4 m (before 42 x 5 cm), see "10 cm layers" below.
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
stages map to Zadoks only at the stage boundaries.

With that cultivar (`crops/cultivar_winter-wheat_bushland_1_phenology.json`, 10 cm): all
treatments reach maturity, LAI max 6.2-7.9 (obs 2.4-7.1, before 10.7-13.2), ET RMSE 1.35-1.96
mm/d, bias of the F treatments -0.61..-0.67 mm/d (the cap). Yield off: F +13..+46 %, P -57 % (1990)
to +88 % (1992). Steps 2-4.

## Open questions

By relevance for Bushland and Avignon (2026-11-16):

1. **Phenology/LAI**, the largest error source: anthesis 1-4 weeks late, LAI 1.2-9x too high,
   Bushland mostly not mature at harvest, so Kc and soil cover are wrong. Calibrate per cultivar
   (Bushland: TAM200, TAM107, Mesa; Avignon: four durum cultivars) with the calibration flow, or
   is "Orig" meant uncalibrated?
2. **6.5 mm/d cap** (model issue 1): most of the late-season ETa underestimation at Bushland
   (-0.6 mm/d in the fully irrigated treatments). Part of "Orig", removed in "Imp"?
3. **ETO column** (model issue 2): the crop-dependent ET0 as now, or the FAO-56 grass reference
   from a crop-free run?
4. **Not looked into yet:** FRAV_2006's bias (-1.11 mm/d, 45 vs 96 mm on the measured days, far
   worse than the other Avignon years); Bushland rooting depth up to 3.9 m in a 2.1 m profile.
5. **For the data providers** (no effect on the runs, RH is derived from ea / es_mean): Bushland,
   is RHUMD the mean or the minimum? Avignon, VPRSD is the deficit, not the vapor pressure.
6. **Ponding** (model issues 4 and 5), changes to MONICA: tell the crop module when surface
   evaporation takes over its transpiration (or let soil moisture take the crop's transpiration
   anyway); base surface evaporation on the uncapped potential with an explicit cap; make the
   infiltration limit independent of the layer thickness. Until then: 10 cm layers for Bushland?
7. Template columns: fill EPPAD (potential transpiration) from Odin's path output? EPSAD,
   ETOS, ETRS stay NA.

Less relevant for these two sites:

- Avignon: EXPTRNO or EXP_TRT_NAME as treatment id? Use residues/tillage?
- Odin's outputs round ET to 0.1 mm/d: good enough, or switch to unrounded path outputs (Odin
  only)?
- `Monica_AMEI_potential_Evapotranspiration` isn't integrated yet; "Imp" runs would select it via
  `EvapotranspirationModel` in sim.json.
- Fix the capnp soil moisture / climate gaps in `monica` (master) and `monica_odin` as well?
  (Flows and Maricopa only.)
