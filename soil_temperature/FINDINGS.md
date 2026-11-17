# AMEI soil temperature intercomparison: MONICA runs

Script-based, like the ET study (`evapotranspiration/`): plain `monica-run` per model and treatment,
no FBP flow. Shares the ET study's MONICA input code (`evapotranspiration/et_monica.py`) and its
ICASA readers (`evapotranspiration/icasa.py`, also used by Bushland's ET reader).

    pixi run python soil_temperature/run_st.py --site maricopa [--models MO DS] [--treatments 901 902]
    pixi run python soil_temperature/run_st.py --site bushland

- `site_maricopa.py`, `site_bushland.py`: the site's workbook -> one `Setup` per treatment
- `st_monica.py`: the soil temperature models, the outputs, the three result files
- `out_maricopa/runs/<model>/<treatment>/`: everything MONICA got and returned (repeat by hand with
  `monica-run sim.json`); `out_maricopa/results/`: the protocol's files, named as it asks
  (`DSMOSummaryMaricopa.txt`, `DSMODailyMaricopa901.txt`, `DSMOLayersMaricopa901.txt`, ...)

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
- Cultivar `crops/cultivar_spring-wheat_with_1_calibrated_phenology.json` of
  `maricopa_wheat_face/params` (1st calibration step of the flows; LAI step not applied).
- Water and N stress on (low and high N treatments).

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

1. **Residue** of the previous crop (1100-4300 kg/ha wheat residue + 500 kg/ha roots, incorporated
   to 10-30 cm): not simulated.
2. **1994 (905-908): no initial NO3/NH4 below 120 cm.** MONICA's default used for layers 25-42
   (0.005 kg NO3-N m-3 and 0.0005 kg NH4-N m-3: 2.75 kg N/ha per 5 cm, about 50 kg N/ha in all).
   The protocol says not to estimate missing inputs.
3. The 1994 harvest (HADAT 1994-06-01) is after ENDAT (1994-05-31): no harvest in the run.
4. The protocol's checklist doesn't match its templates: 32/34/11 columns (templates: 35/40/11),
   "last row = day of maturity" (protocol text: report SDAT..ENDAT, done here), 144 summary rows
   (16 per model file).

## Model issues

1. **Phenology is far off with the calibrated cultivar** (MO, Odin):

   | | anthesis | maturity |
   |---|---|---|
   | 1993 (901-904) | +28..30 d | +9..16 d |
   | 1994 (905-908) | +15..18 d | +6..9 d |
   | 1996 (909-912) | +20..23 d | +10..12 d, 910/912 not before ENDAT |
   | 1997 (913-916) | +16..18 d | +2..8 d |

   Stage 4 (which the calibration matched with Zadoks 61) starts 1993-04-11 in 901, observed
   anthesis 03-25. Not the code: the C++ AMEI branch gives the same stages on the same inputs.
   Not water stress (the soil started at 1 % FC gives the same stages), not the latitude (52.5°
   instead of 33°: 1 day). Whatever differed in the calibration flow (objective 4.5) isn't found.
   Recalibrating with these inputs is the way out. 910/912 have no maturity values.
2. **Biomass at maturity about 50 % too high** (e.g. 901: 23.5 vs 14.9 t/ha), and the dry
   treatments are hardly stressed: 901/903 come out as 902/904, 905 as 906 (25.6 t/ha both; observed
   15-25 % less biomass in the dry ones). Calibration (LAI step) not applied.
3. **SQ has one temperature for all layers**: SiriusQuality's model has no layers, the binding
   writes the mean of its minimum and maximum everywhere. Its minimum/maximum could go into
   TSSMX/TSSMN.
4. **ST doesn't use TAV**: STICS starts the profile at the first day's air temperature (8 °C at
   205 cm in 901), the deep layers warm to about 25 °C by May.
5. **SW's surface stays cool**: TSSAV maximum 16-24 °C over the 16 treatments, the others 27-38 °C.

## Bushland

Workbook `Bushland/BUSHLAND_data_2026-10-03 (...)(noSoilT).xlsx`, 6 treatments, 9 + iMO models,
60 runs. Same experiment and weather as the ET study's Bushland, but differences there:
SDAT 3-4 weeks earlier (the real fertilization dates), TXBL1992 sowing 09-27 instead of 09-26 and
ENDAT 07-06 instead of 06-29, initial N per layer (ICNO3M/ICNH4M, 53 kg/ha) instead of ICIN spread
evenly, residue sheet (all 0 kg/ha).

- 48 layers of 5 cm (0-240 cm, the whole profile): the protocol says "48 soil layers ... from 0 to
  2.40 m" but also "(0 to 210 cm)", the latter probably taken over from Maricopa.
- ET study's parameters and cultivar (`crops/cultivar_winter-wheat.json`, not calibrated), water
  stress on, N stress off (as in the ET study; the treatments differ in water only).
- TAV 11.4, TAMP 17 °C, CO2 355 ppm from `Weather_stations`; MONICA's BaseTemperature = TAV.
- Sprinkler irrigation at the surface; RH from VPRSD as in the ET study.
- MO == iMO in all 6 treatments.

Data questions (for Thiago Berton Ferreira):

1. **Fertilizer TXBL1993**: a row without TREAT_ID (50 kg N, 1992-09-15, skipped), and TXBL1993P's
   only row is dated 1993-09-29, after ENDAT (34 kg N, broadcast; dropped). So TXBL1993P gets no N
   at all. 1992-09-29 (its sowing) probably meant; the empty row maybe TXBL1993P's too.
2. The initial conditions are dated 2-4 weeks after SDAT (ICDAT = sowing), used at SDAT.
3. Initial water and N end at 220 cm: layers 45-48 get MONICA's defaults (water: the default,
   N: 2.75 kg N/ha per layer).
4. Initial water above field capacity (TXBL1992 140-220 cm: 0.32-0.35 vs FC 0.30; TXBL1993
   25-70 cm) is set to FC, MONICA's maximum.
5. The comments say RAIN_FULL for F and RAIN_PART for P, the sheet has a single RAIN column.
6. ICLST (initial soil temperature per layer, 1990/1992): MONICA and the modules start their soil
   temperature themselves, not used.
7. Anhydrous ammonia injected at 17.25 cm, applied at the surface (MONICA's mineral fertilization).

Model state: the default cultivar is far off: LAI max 11-13 (observed 2.4-7.1), maturity 2-4 weeks
late, not reached by ENDAT in TXBL1990F and both 1993 treatments (no maturity values). Calibration
pending, as for the ET study.

## Open questions

1. Recalibrate the phenology (and LAI) with this setup?
2. Upload iMO, or only the 9 protocol models?
3. Residue, missing 1994 N below 120 cm: ask Pierre Martre?
4. Bushland: calibrate the cultivar (shared with the ET study?), and the data questions above.
   Avignon once its data are clear.
5. The AMEI branch's crop stalling with a dry topsoil (MONICA change 2).
