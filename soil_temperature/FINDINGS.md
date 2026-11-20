# AMEI soil temperature intercomparison: MONICA runs

Script-based, like the ET study (`evapotranspiration/`): plain `monica-run` per model and treatment,
no FBP flow. Shares the ET study's MONICA input code (`et_monica.py`) and its ICASA readers
(`icasa.py`, also used by Bushland's ET reader). The scripts are in
`src/amei_exercises/soil_temperature/` (since 2026-11-19; before in this folder).

    pixi run run-st --site maricopa [--models MO DS] [--treatments 901 902]
    pixi run run-st --site bushland
    soil_temperature/calibration/calibrate.sh   # the Maricopa calibration, step by step
    pixi run evaluate-maricopa --cultivar ... --out ...   # all 16 Maricopa treatments vs observed
    pixi run evaluate-st --site maricopa|bushland   # the models against each other, plots per depth

- `site_maricopa.py`, `site_bushland.py`: the site's workbook -> one `Setup` per treatment
- `st_monica.py`: the soil temperature models, the outputs, the three result files
- `out_maricopa/runs/<model>/<treatment>/`: everything MONICA got and returned (repeat by hand with
  `monica-run sim.json`); `out_maricopa/results/`: the protocol's files, named as it asks
  (`DSMOSummaryMaricopa.txt`, `DSMODailyMaricopa901.txt`, `DSMOLayersMaricopa901.txt`, ...)
- `out_<site>/evaluation/`: `soil_temperature_<depth>cm.png` (5, 20, 50, 100 cm; per treatment the
  range and median of the 9 AMEI models and MO) and `model_deviation.csv` (each model against the
  9 models' median). No measured soil temperature in the workbooks, so no comparison with data.

160 runs (10 models x 16 treatments) take about 35 s. Odin MONICA (`monica_odin`, branch
`simplify_copilot`), soil temperature model by `"Modules": {"SoilTemperature": {"impl": ...}}` in
sim.json (integrated: the module's temperatures are MONICA's).

## Models

| code | Modules.SoilTemperature.impl |
|---|---|
| AP | ApsimCampbell |
| PS | BiomaSurfacePartonSoilSWATC |
| SW | BiomaSurfaceSWATSoilSWATC |
| DE | DSSAT_EPICST_standalone |
| DS | DSSAT_ST_standalone |
| MO | Monica_SoilTemp (MONICA's soil temperature as Crop2ML module) |
| SA | Simplace_Soil_Temperature |
| SQ | SQ_Soil_Temperature |
| ST | Stics_soil_temperature |
| iMO | internal (MONICA's own; not for the upload) |

MO and iMO give identical results (all 16 treatments), as in the old flow's setup.

## Inputs (Maricopa)

- 42 layers of 5 cm (0-210 cm), the protocol's. Soil horizons from `Soil_profile_layers`.
- Initial water and N per measured layer (`initial_condition_layers`: ICH2O, ICNO3M, ICNH4M in
  kg N/ha) as concentrations, depth-weighted onto the 5 cm layers.
- Weather `Weather_daily` of the treatment's dataset (MARA/MARB), as for Bushland: RH = VPRSD / es.
- CO2 per day in climate.csv (`co2` column): the station's CO2Y (370 ppm), changed on EMDATE by
  `Env_modifications` (1993/94: 550 ppm from Jan 1 or emergence to mid May; 1996/97: +185 ppm).
- TAV 21 °C, TAMP 11.1 °C of `Weather_stations` in `climate.csv-options` (new in Odin, see below).
- MONICA's own soil temperature (MO, iMO): `BaseTemperature` (its lower boundary, default 9.5 °C
  for Germany) = TAV. The same site input the other models get as TAV; with 9.5 °C MO's 205 cm
  layer started at 9.5 °C instead of about 20 °C.
- Fertilizers (`Fertilizer_events`, FEAMN > 0) as mineral fertilization with the NO3/NH4/urea
  shares of FENO3/FENH4/FEUREA, including the N in the irrigation water.
- Irrigations (`Irrigation_events`) into the layer at IRADP (buried drip lines at 23 cm: layer 5,
  20-25 cm; the few at 0 cm at the surface). Sowing PDATE, harvest HADAT.
- Cultivar `crops/cultivar_spring-wheat_maricopa_6_yield.json` and species
  `crops/species_wheat_maricopa_5_growth.json` of `soil_temperature/params` (since 2026-11-20:
  calibration steps 1 and 4-6, see "Maricopa calibration"; 2026-11-18/19: steps 1-3,
  `..._3_yield.json` + `..._2_growth.json`; before: the flows' `cultivar_spring-wheat_with_1_calibrated_phenology.json`).
- Odin's standing water fixes (`SOIL_MOISTURE` in site_maricopa.py), since 2026-11-18.
- Water and N stress on (low and high N treatments).
- Previous crop's residue (sheet `Residue`, since 2026-11-19): above ground residue (ICRAG) and roots
  (ICRT) as MONICA's wheat crop residue (`crops/residue_wheat.json`) with the workbook's N content
  (ICRN, used for the roots as well), on ICRDAT (= SDAT) spread evenly over the 5 cm layers to ICRDP
  (30 cm in 1993/94, 10 cm in 1996/97; ICRIP 100 %): one `OrganicFertilization` workstep per layer
  (`incorporateIntoLayerNo`). No MONICA change needed.

## Outputs

- Daily and layers files: every day SDAT..ENDAT. Summary: events (PDATE, PLDAE, ADAT, MDAT, values
  at anthesis/maturity) and season values sowing..maturity (end of the run if MONICA doesn't reach
  maturity), as in the ET study.
- TSSAV: `soil_column.soil_surface_temperature`; TSAV: `soil_column.layers.soil_temperature`
  (both unrounded; the STemp/SurfTemp aliases round to 0.1 °C). SWLD: Mois. SNLD and NIAD: NO3 + NH4
  [kg N/ha] of the layer / the profile. RND: net radiation in W/m2 (daily mean).
- GSTZD from MONICA's stage as in the old flow's consumer: 1 -> 0, 2 -> 9, 4 -> 51, 5 -> 65,
  6 -> 89, stage 3 na.
- na: LNUM, LIPCD, GWGD, GNGD, NIMD, GHFD, LHFD, HHFD, TSSMX, TSSMN, TGAV/MX/MN, EPSAD, EPPAD;
  TSMX, TSMN; LnoSM, LIPCCM, HnoAM, GWGM, GNGM, WAVSSM, NIAVSSM, NIMCM, EPSCM, EPPCM.
- Files in the templates' encoding (Windows-1252, for °C and m²/m²), template header kept, Model and
  Modeler_name lines filled in, the title line set per site (Bushland's summary template says
  "Maricopa Wheat FACE"), and an empty 4th line added where a template has none (Maricopa's summary,
  Bushland's daily), so the data start on line 8 as the checklist wants.

## MONICA changes (monica_odin and monica_amei, uncommitted)

1. **TAV/TAMP could not be passed.** Odin always calculated them from the climate data, which
   averages over all 12 months with a month without data counting as 0 °C (and drops the last
   month). A December-May season: TAV 5.4 °C, TAMP 18 °C instead of 21 and 11.1 °C. With these,
   the deep layers of AP, PS, SW, DE, DS, SA stayed at 5-9 °C instead of about 20 °C.
   Now `climate.csv-options` takes optional `"tav"`/`"tamp"` (used if given, as the C++ AMEI branch's
   `getTAMPandTAV`), and the calculation averages only the months that have data, including the last
   one (`NOTE(deviation)`: only the modules use it). Runs without modules: byte-identical (Bushland and
   Avignon ET runs checked). `pixi run test` (100) and `tests/check_drivers.py` pass.
   The C++ AMEI branch has the same calculation bug (mas_cpp_misc `calcTAMPandTAV`), not changed.
2. **Subsurface irrigation.** The AMEI branch's Irrigation `"atLayer"` (51c4fb12) never took effect:
   `Irrigation::apply` passed the layer as `applyIrrigation`'s third argument, the unused sulfate
   concentration, and even `SoilColumn::applyIrrigation(..., atLayer)` moved only the nitrate there,
   the water always went to the surface water storage. (The flow component's `int(IRADP / 5)` also
   pointed one layer too high, 15-20 cm for 23 cm.) Now, in Odin and in the AMEI branch, an
   Irrigation workstep with `"atLayer"` (1-based) or `"depth"` [m] puts water and nitrate straight
   into that layer's soil moisture: no surface water storage, infiltration limit, runoff or surface
   evaporation. Water above saturation goes to the layers below, then above, the rest to the surface.
   Without them: as before (Odin: tests, drivers check, ET runs byte-identical). Odin and AMEI give the
   same soil water day by day (901). Maricopa: season ET about 90 mm less (no ponding evaporation).
   The AMEI branch's crop, though, stalls for 6 weeks after emergence in 901 with the topsoil dry
   (biomass 100 kg/ha, no transpiration) where Odin's grows: a difference of the two crop codes on
   the same soil water (with surface irrigation they agree within 1 %), not looked into.

## Things MONICA can't represent (questions for the data providers)

1. ~~**Residue** of the previous crop (1100-4300 kg/ha wheat residue + 500 kg/ha roots, incorporated
   to 10-30 cm): not simulated.~~ Simulated since 2026-11-19 as organic fertilization (see Inputs).
   Effect (with the calibrated cultivar; not recalibrated): soil temperature practically unchanged
   (mean difference < 0.02 °C for every model, at most 0.7 °C on single days, SW); the crop only
   through N immobilization, mostly in 1997 (4300 kg/ha straw with 0.53 % N): low N biomass 913/915
   -0.6/-0.7 t/ha (already too low: 5.8/7.0 vs 13.3/12.4 t/ha observed), the others < 0.2 t/ha.
   All 16 treatments vs observed: LAI RMSE 1.24 -> 1.23, biomass 1.76 -> 1.80 t/ha, yield 1.47 ->
   1.52 t/ha (controls 0.98 -> 0.97); LowN/HighN yield 1997 0.42/0.36 -> 0.38/0.34 (observed
   1.02/0.85). So it doesn't help 1997's low N problem (Maricopa calibration, N), it adds to it.
2. **1994 (905-908): no initial NO3/NH4 below 120 cm.** MONICA's default used for layers 25-42
   (0.005 kg NO3-N m-3 and 0.0005 kg NH4-N m-3: 2.75 kg N/ha per 5 cm, about 50 kg N/ha in all).
   The protocol says not to estimate missing inputs.
3. ~~The 1994 harvest (HADAT 1994-06-01) is after ENDAT (1994-05-31): no harvest in the run.~~
   The user set HADAT to 1994-05-31 in the workbook (2026-11-19). No effect on the results: the
   calibrated crop matures on 05-11/05-15, and the summary reports at maturity.
4. The protocol's checklist doesn't match its templates: 32/34/11 columns (templates: 35/40/11),
   "last row = day of maturity" (protocol text: report SDAT..ENDAT, done here), 144 summary rows
   (16 per model file).

## Model issues

State 2026-11-20 (the `calibrate.py` parameters of steps 1 and 4-6, previous crop residue;
`pixi run evaluate-maricopa --species crops/species_wheat_maricopa_5_growth.json`, all 16 treatments
against the observations).

1. **Phenology: fine since the recalibration** (2026-11-18, "Maricopa calibration"): anthesis RMSE
   4.6 d, maturity 4.4 d; left is a year effect (1993 anthesis about a week late, 1997 a few days
   early). Before, with the flow-calibrated cultivar, anthesis was 2-4 weeks late in this setup
   (1993 +28..30 d) for reasons not found (not the code: the C++ AMEI branch gave the same; not water
   stress, not the latitude); hence the recalibration in this setup.
2. **Water and N responses** (the controls fit: e.g. 902 biomass 16.6 vs 18.3 t/ha observed):
   - **The dry treatments got no water stress** with the parameters of steps 1-3: Dry/Wet biomass
     0.99-1.01 (observed 0.77-0.82), so their biomass was 20-30 % too high (905: 17.7 vs 13.9 t/ha),
     as in Bushland's P treatments. **Since steps 4-6** (2026-11-20, crop coefficients against the
     measured ET and soil water): 1993 0.95/0.95 (observed 0.82/0.78), 1994 0.73/0.65 (0.77/0.82;
     905: 13.2 vs 13.9 t/ha). 1993's dry treatments are still hardly stressed (the deep soil water,
     below). Why (2026-11-19, with the parameters of steps 1-3):
     - **MONICA's water demand is too low.** Its ET0 is right: the same as FAO-56 grass ET0 from the
       weather (902 season 555 vs 553 mm). But its crop uses about ET0 (StageKcFactor 1.1): season ET
       400-450 mm, at most ~5.8 mm/d. The measured ET (`Cum_evapotranspiration_means`) of the wet
       treatments is 7.5-11 mm/d in April/May, 1.6-1.9 x ET0 (FAO-56: ~1.15 for wheat), the dry ones
       6-8 mm/d. So MONICA's demand is ~60 % of the measured one, and the dry treatments' water
       (1993: 592 mm irrigation + 156 mm rain, 1994: 287 + 107 mm, plus the soil) covers it: dry 901
       even drains 218 mm. Measured dry ET falls behind wet from late March; MONICA's dry = wet.
     - Test, StageKcFactor stages 2-5 0.7/1.1/1.1/0.8 -> 1.0/1.7/1.7/1.3: 1994 Dry/Wet biomass 0.87
       (FACE 0.75), observed 0.77/0.82; 1993 still 1.00.
     - **1993: MONICA's profile holds too much water.** Against the neutron probe readings
       (`Soil_moisture_means`, 30-210 cm): from January 100-170 mm more water in 20-220 cm, below
       120 cm at the horizons' FC (0.25) where the probes show mostly 0.16-0.21 (at 110-130 cm down
       to 0.07-0.10, below the workbook's PWP of 0.14-0.16). The measured dry profile drew down ~140 mm
       from mid January to late April, MONICA's ~50 mm.
     - For the data providers: how was the measured ET obtained (a soil water balance would count
       drainage as ET)? `ETAP` is the mean per period in mm/d in 1993/94, but the sum over the
       (10-day) period in 1996/97 (values up to 88; the definition says "averaged", mm). And the
       deep layers' FC/PWP vs the probe readings.
     - Hence steps 4-6 of the Maricopa calibration (StageKcFactor against the measured ET and soil
       water, then growth and yield again), see there; adopted 2026-11-20.
   - **1997 low N far too N-limited**: biomass 913/915 6.4/7.7 vs 13.3/12.4 t/ha, crop N 54/53 vs
     136/124 kg N/ha; the residue (4300 kg/ha straw, N immobilization) made it a bit worse. 1996's
     low N effect is too weak instead (yield ratio 0.96/0.92 vs 0.76/0.86).
   - CO2 (FACE/Ambient) a bit too strong: biomass 1.00-1.20 vs 0.94-1.14, yield 1.10-1.26 vs
     0.97-1.20.
3. **SQ has one temperature for all layers**: SiriusQuality's model has no layers (its design), the
   binding writes the mean of its minimum and maximum everywhere. Its minimum/maximum could go into
   TSSMX/TSSMN.
4. **ST doesn't use TAV**: STICS starts the whole profile at the first day's mean air temperature
   (8 °C at 205 cm in 901, the other models ~17 °C), the deep layers warm to about 25 °C by May. The
   model's design, not a missing input: the Crop2ML component's only parameters are the layer
   thicknesses and `air_temp_day1` (Odin's binding as the C++ AMEI adapter).
5. **SW's surface stays cool**: TSSAV maximum 17-26 °C over the 16 treatments, the others 27-38 °C.
   SWAT's surface temperature is `bcv * T(top layer, previous day) + (1 - bcv) * T(bare soil)`, with
   the soil cover `bcv = CV / (CV + exp(7.563 - 1.297e-4 CV))` from the above ground biomass CV
   (kg/ha): 0.81 at 4.6 t/ha, 0.96 at 11 t/ha, 0.99 at 16 t/ha (902). So from mid April the surface
   only repeats the top layer and stops following the weather (902, 05-14: SW 17.5 °C, the others'
   median 28.0 °C). The model's design (SWAT's cover by canopy and residue); MONICA gives its
   simulated above ground biomass in kg DM/ha, as SWAT wants and as the C++ AMEI adapter does. The
   only MONICA side: the biomass itself, but even the observed one (14-18 t/ha at maturity in the wet
   treatments) gives a cover of 0.98-0.99.

## The models against each other (`evaluate-st`, 2026-11-19)

Deviation from the median of the 9 AMEI models, all treatments and days, mean (RMSD) in °C
(Maricopa since 2026-11-20: calibration steps 1 and 4-6, with the residue):

| | Maricopa 5 cm | 20 cm | 50 cm | 100 cm | Bushland 5 cm | 20 cm | 50 cm | 100 cm |
|---|---|---|---|---|---|---|---|---|
| AP | +1.4 (1.9) | +0.2 (0.6) | -1.1 (1.6) | -1.3 (2.2) | +1.0 (1.9) | +1.5 (2.2) | +2.3 (4.2) | +3.5 (5.2) |
| PS | -0.5 (0.9) | +0.4 (1.0) | +1.7 (2.3) | +2.2 (2.6) | -0.8 (1.4) | -0.5 (1.1) | -0.4 (1.2) | -0.3 (1.3) |
| SW | -1.7 (3.0) | -0.6 (2.5) | +1.1 (2.8) | +1.7 (2.8) | -2.5 (4.3) | -1.9 (3.5) | -1.5 (2.9) | -0.9 (2.1) |
| DE | +1.4 (2.1) | +1.7 (2.2) | +2.4 (2.9) | +2.9 (3.4) | +1.0 (2.7) | +1.0 (2.6) | +0.8 (2.3) | +0.5 (1.8) |
| DS | -0.3 (1.0) | -0.6 (1.1) | -0.5 (1.4) | 0.0 (1.5) | -0.5 (1.6) | -0.4 (1.4) | -0.5 (1.6) | -0.7 (2.0) |
| MO | +3.2 (3.8) | +2.1 (2.7) | +0.7 (1.3) | -0.3 (0.9) | +2.2 (3.2) | +1.8 (2.4) | +1.0 (2.5) | +0.1 (2.6) |
| SA | +0.1 (1.7) | -0.2 (2.0) | -0.2 (2.3) | +0.1 (2.4) | +0.3 (1.8) | +0.4 (1.9) | +0.3 (2.5) | -0.1 (3.4) |
| SQ | -1.2 (1.8) | -1.8 (2.4) | -2.4 (3.2) | -2.9 (4.0) | -0.7 (2.2) | -0.6 (2.4) | -0.7 (2.8) | -1.0 (4.0) |
| ST | -0.4 (1.2) | -1.5 (1.6) | -2.3 (2.5) | -2.8 (3.5) | -0.5 (2.0) | -0.4 (1.0) | -0.5 (1.9) | -0.8 (3.6) |

- **MO is the warmest model near the surface** (+2-3 °C at 5 cm, up to 9 °C (Maricopa) and 10 °C (Bushland) above the median in
  May/June), close to the median at 100 cm. iMO (MONICA's own implementation) is identical to MO
  everywhere (max difference 0.0 °C), so the Crop2ML port reproduces MONICA exactly.
- The band's smooth lower edge from March on is **SW** (model note 5): at Bushland its 5 cm
  temperature in June is ~13 °C, the others 26-34 °C, almost its own 50 cm value.
- **SQ** changes 1-2 °C from day to day at 100 cm (the others 0.03-0.7): its one temperature for
  all layers (model note 3). In Maricopa's winter SQ, ST and AP form the band's lower edge at 1 m
  (~7-10 °C against ~15 °C for MO/median), DE and SA the upper one.

## Bushland

Workbook `Bushland/BUSHLAND_data_2026-10-03 (...)(noSoilT).xlsx`, 6 treatments, 9 + iMO models,
60 runs. Same experiment and weather as the ET study's Bushland, but differences there:
SDAT 3-4 weeks earlier (the real fertilization dates), TXBL1992 sowing 09-27 instead of 09-26 and
ENDAT 07-06 instead of 06-29, initial N per layer (ICNO3M/ICNH4M, 53 kg/ha) instead of ICIN spread
evenly, residue sheet (all 0 kg/ha).

- 48 layers of 5 cm (0-240 cm, the whole profile): the protocol says "48 soil layers ... from 0 to
  2.40 m" but also "(0 to 210 cm)", the latter probably taken over from Maricopa.
- ET study's parameters and its calibrated cultivar (`crops/cultivar_winter-wheat_bushland_2_growth.json`,
  since 2026-11-18; before: the uncalibrated `cultivar_winter-wheat.json`), as in the ET study's runs:
  water and N stress on, Odin's standing water fixes (`SOIL_MOISTURE`), the anhydrous ammonia at FEDEP
  (17.25 cm). Not as in the ET study: 48 × 5 cm layers (24 × 10 cm there), this workbook's initial N
  per layer (spread by SOC there).
- TAV 11.4, TAMP 17 °C, CO2 355 ppm from `Weather_stations`; MONICA's BaseTemperature = TAV.
- Sprinkler irrigation at the surface; RH from VPRSD as in the ET study.
- MO == iMO in all 6 treatments.

Data questions (for Thiago Berton Ferreira):

1. **Fertilizer TXBL1993**: the original workbook had a row without TREAT_ID (50 kg N, 1992-09-15,
   skipped), and TXBL1993P's only row was dated 1993-09-29, after ENDAT (34 kg N, broadcast;
   dropped), so TXBL1993P got no N at all. Corrected in the workbook by Michael (2026-11-19, asked
   the data providers, no answer yet): the row without TREAT_ID is TXBL1993F's, dated 1993-03-29
   (the ET study's spring application), TXBL1993P's is dated 1992-09-29 (its sowing). Now TXBL1993F
   gets 50 + 50 kg N (injected), TXBL1993P 34 kg N (broadcast); the ET study's workbook has 54 + 50
   and 50 (all injected). Source cited in FE_NOTES: Howell et al. (1995), Trans. ASAE 38(3):745-759,
   doi:10.13031/2013.27888 (not freely available).
2. The initial conditions are dated 2-4 weeks after SDAT (ICDAT = sowing), used at SDAT.
3. Initial water and N end at 220 cm: layers 45-48 get MONICA's defaults (water: the default,
   N: 2.75 kg N/ha per layer).
4. Initial water above field capacity (TXBL1992 140-220 cm: 0.32-0.35 vs FC 0.30; TXBL1993
   25-70 cm) is set to FC, MONICA's maximum.
5. The comments say RAIN_FULL for F and RAIN_PART for P, the sheet has a single RAIN column.
6. ICLST (initial soil temperature per layer, 1990/1992): MONICA and the modules start their soil
   temperature themselves, not used.
7. Anhydrous ammonia injected at 17.25 cm: applied at that depth since 2026-11-18 (Odin's
   MineralFertilization `depth`), at the surface before.

Model state with the calibrated cultivar (2026-11-18): close to the ET study's runs. Anthesis
1990-05-16/19, 1992-05-06, 1993-05-14/15; all treatments mature; LAI max 3.3-4.6.
Yields (t/ha, MO, obs F 4.5/5.8/6.4, P 1.9/3.2/5.0): F 4.5/5.0/4.5, P 1.1/5.0/3.0 (ET study F
4.3/4.4/4.4, P 1.0/4.3/3.1). With the workbook's original 1993 fertilization (data question 1:
F 50 kg N, P none) 1993 was F 3.3, P 2.1 t/ha; the correction changes only 1993's soil temperatures,
by 0-0.07 °C on average (SW: 0.06/0.45 °C, 0-20 cm in P 1.0 °C, max 4.8 °C). Before (uncalibrated):
LAI max 11-13, maturity 2-4 weeks late, not reached in TXBL1990F and 1993.

## Maricopa calibration (`calibrate.py --site maricopa`, 2026-11-18; `calibration/calibrate.sh`)

Independent of the Maricopa flows (their parameters and cultivars not used), the same way as Bushland
(evapotranspiration/FINDINGS.md, "Calibration"): from `cultivar_spring-wheat_default.json`, not
against ET, no crop parameters that change MONICA's crop ET0, two seeds per step. Setup: this
study's (42 × 5 cm, N response on, FACE CO2, buried drip) plus the ponding/infiltration fixes
(`SOIL_MOISTURE` in site_maricopa.py, also for the soil temperature runs, rerun with the calibrated cultivar on 2026-11-18).
Results in `soil_temperature/calibration/maricopa_*`, parameters in
`soil_temperature/params/crops/{cultivar_spring-wheat,species_wheat}_maricopa_*.json`.

- **Step 1, phenology** (all 16 treatments, emergence, ear emergence = start of stage 4, anthesis,
  maturity; 64 dates): emergence 186 °C d (80), stages 2-3 ×0.82, stage 4 49 °C d (180; ear
  emergence to anthesis is only 1-10 days here), maturity 434 °C d. RMSE 15.7 -> 4.2 d, seeds agree.
  The daylength requirement free gives the same RMSE (15.3 h, stages 2-3 ×1.2), so it stays.
  Left: a year effect, 1993 anthesis +7…+9 d, 1997 -3…-5 d.
- **Step 2, growth** (the controls Wet-High N-Ambient: 902, 906, 910, 914; daily LAI, above ground
  biomass and green leaves `WLVG`, which here are consistent with LAI and `SLAD`, unlike Bushland's):
  MaxAssimilationRate 31 (52), SLA 0.0024 stages 1-2 and 0.0020 stages 3-4 (0.0020-0.0016; measured
  0.0017-0.0028), leaf share 0.36 in stage 2 (0.2) and 0.17 in stage 3 (0.2), leaf senescence 0.07
  from anthesis, species InitialOrganBiomass ×0.55 (53 -> 29 kg/ha). RMSE (1 LAI ~ 3 t/ha ~ 1 t/ha
  green leaves) 0.9 -> 0.7. Seeds agree on MaxAssimilationRate and the stage 2 leaf share; SLA of
  stages 3-4 and the stage 3 leaf share trade off. Why InitialOrganBiomass: the crop had 2-4× the
  measured biomass until February at the same N uptake (1997, measured crop N), so its N
  concentration was diluted (1.3-2 % vs 2.6-3.8 %) and it was N-stressed in Feb/March.
- **Step 3, yield** (the controls): the simulated harvest index was ~0.57 (measured ~0.45). The ear's
  share of the assimilates in stage 4 (ear emergence to anthesis) 0.68 (0.97, rest to the stem);
  RMSE 2.1 -> 1.0 t/ha. AssimilateReallocation instead went to its bound (0) with the same fit, the
  grain share after anthesis stayed at 1; both keep their defaults.
- **Step 4, water** (2026-11-19, `calibration/maricopa_4_water.csv`, `calibrate.py --step water`;
  **adopted 2026-11-20** with steps 5-6): StageKcFactor of stages 2-5 against the measured ET per period
  (`Cum_evapotranspiration_means`, 282 periods, mm/d) and the soil water in 20-200 cm (neutron probes,
  628 dates; 50 mm weigh as 1 mm/d), all 16 treatments, from the step 3 cultivar. Against ET: only
  for this study, where ET isn't what's evaluated (see model issue 2). Seeds 1 and 7 agree:
  StageKcFactor 1.93 / 1.38 / 1.79 / 1.55 (seed 7: 1.89 / 1.42 / 1.73 / 1.35; before 0.7 / 1.1 /
  1.1 / 0.8); stage 2 near its bound 2.0 (the measured ET in Feb/March is higher than MONICA's
  at its small LAI). ET bias -1.26 -> -0.42 mm/d, RMSE 1.96 -> 1.91 mm/d (the dry 1994
  treatments' ET now too high, 905/907 1.0 -> 1.9-2.0); soil water bias +82 -> +27 mm, RMSE 106 ->
  65 mm (1993 FACE 903/904 still ~110 mm: the deep layers, model issue 2).
  All 16 treatments: LAI RMSE 1.23 -> 1.11, biomass 1.80 -> 1.61 t/ha, yield 1.52 -> 1.35 t/ha
  (controls 0.97 -> 0.97), phenology unchanged. Dry/Wet biomass 1993 0.96/0.95, 1994 0.76/0.65
  (observed 0.82/0.78, 0.77/0.82; before ~1.0). The wet control 902 loses 1.4 t/ha (16.1 vs 18.3
  observed). Soil temperature (all 10 models, April/May): SW +0.6-0.7 °C (dry treatments +1.2 °C
  at 20 cm: less biomass, less cover), AP up to +0.3 °C, the others <= 0.1 °C; single days up to
  4.6 °C (DS).
- **Steps 5-6, growth and yield again on top of step 4** (2026-11-20, `calibration/maricopa_5_growth.csv`,
  `maricopa_6_yield.csv`, copies of steps 2-3; **adopted 2026-11-20**, the study's parameters since): from the step 1 cultivar with step 4's
  StageKcFactor (`cultivar_spring-wheat_maricopa_4_water.json`), as steps 2-3 from step 1's, the
  default species. Step 5 (controls, seeds 1 and 7 agree, seed 1 adopted): MaxAssimilationRate 30
  (step 2: 31), SLA 0.0023 / 0.0024 (0.0024 / 0.0020), leaf share 0.36 in stage 2 (0.36) and 0.10 in
  stage 3 (0.17), leaf senescence 0.0016 / 0.063 (0.0007 / 0.069), InitialOrganBiomass ×0.83
  (×0.55); RMSE 0.9 -> 0.7 (the same). Step 6: the ear's share in stage 4 0.67 (0.68), the controls'
  yield RMSE 2.1 -> 0.9 t/ha. Results in `calibration/maricopa_{growth,yield}_kc/`, parameters
  `cultivar_spring-wheat_maricopa_{5_growth,6_yield}.json`, `species_wheat_maricopa_5_growth.json`.

  | all 16 treatments | steps 1-3 (the study's) | + step 4 | + steps 5-6 |
  |---|---|---|---|
  | LAI RMSE | 1.23 | 1.11 | 1.16 |
  | biomass RMSE t/ha | 1.80 | 1.61 | 1.60 |
  | yield RMSE t/ha (controls) | 1.52 (0.97) | 1.35 (0.97) | 1.32 (0.93) |
  | Dry/Wet biomass 1993, 1994 (obs 0.82/0.78, 0.77/0.82) | 1.01/1.00, 1.01/0.99 | 0.96/0.95, 0.76/0.65 | 0.95/0.95, 0.73/0.65 |
  | ET bias / RMSE mm/d | -1.26 / 1.96 | -0.42 / 1.91 | -0.39 / 1.89 |
  | soil water bias / RMSE mm | +82 / 106 | +27 / 65 | +23 / 63 |

  Phenology unchanged (anthesis 4.6 d, maturity 4.5 d). The refit changes little: growth and yield
  hardly depended on the water demand in the wet controls. Soil temperature (all 10 models,
  April/May, against the study's runs): as with step 4 alone, SW +0.6-0.7 °C (dry treatments +1.2 °C
  at 20 cm), AP up to +0.4 °C at 100 cm, the others <= 0.1 °C; SQ and ST unchanged.

All 16 treatments, the parameters of steps 1-3 (`cultivar_spring-wheat_maricopa_3_yield.json` +
`species_wheat_maricopa_2_growth.json`, 2026-11-18, without the residue) and of steps 1, 4-6 (the
study's since 2026-11-20, with the residue; see the table of steps 4-6 above), and the flow's for comparison (phase 1 cultivar
`cultivar_spring-wheat_with_1_calibrated_phenology.json`, phase 2 = its best.csv applied to it), the
same setup for all (`pixi run evaluate-maricopa`, the flow's phase 2 cultivar as
`calibration/flow_phase_2_cultivar.json`):

| | anthesis RMSE d | maturity RMSE d | LAI RMSE | biomass RMSE t/ha | yield RMSE t/ha (controls) |
|---|---|---|---|---|---|
| flow phase 1 | 21.7 | 10.0 (2 not reached) | 2.46 | 3.67 | 2.11 (1.80) |
| flow phase 2 | 21.7 | 10.6 (2 not reached) | 2.20 | 2.47 | 3.29 (2.76) |
| calibrate.py step 2 | 4.8 | 4.6 | 1.23 | 1.77 | 2.85 (2.06) |
| calibrate.py step 3 | 4.6 | 4.3 | 1.24 | 1.76 | 1.47 (0.98) |
| calibrate.py steps 4-6 (with residue) | 4.6 | 4.5 | 1.16 | 1.60 | 1.32 (0.93) |

The flow's phenology is 2-4 weeks late in this setup (as noticed before, "Phenology of the
flow-calibrated cultivar"); it was calibrated in the flow's setup, so its phenology is no fair
comparison here. The flow's phase 2 also calibrates the StageKcFactors (potential ET): for the ET
study that would be calibrating against ET.

What the treatments that weren't calibrated show (ratios of final biomass / yield):
- **Water**: Dry/Wet measured 0.77-0.82 / 0.80-0.86, MONICA 0.99-1.01: no water stress in the dry
  treatments, as in Bushland's P treatments.
- **N**: Low/High N measured 0.75-0.98, MONICA 1996 0.89-0.96 (too weak), 1997 0.36-0.43 (far too
  strong: crop N 53-56 vs 124-136 kg/ha measured; the 1997 low-N soil supplies too little N in
  MONICA - initial N? The residue, simulated since 2026-11-19, makes it a bit worse, see "Things
  MONICA can't represent" 1).
- **CO2**: FACE/Ambient measured 0.94-1.20, MONICA 1.13-1.27: a bit too strong.
- Crop N with high N (1997): 232-234 vs 191-223 kg/ha measured.
- Setup: the 1994 harvest (1994-06-01) was after the simulation's end (ENDAT 1994-05-31); since
  2026-11-19 05-31 in the workbook. No effect: the crop matures before.

## Open questions

1. Upload iMO, or only the 9 protocol models?
2. Missing 1994 N below 120 cm (MONICA's default used, "Things MONICA can't represent" 2): ask
   the data providers. Points 3-5 of the model issues: worth reporting, not MONICA's to fix.
3. Water stress in the dry treatments and 1997 low N (model issue 2).
4. Bushland: the data questions above (1 now matters: N stress on).
   Avignon once its data are clear.
5. The AMEI branch's crop stalling with a dry topsoil (MONICA change 2).
